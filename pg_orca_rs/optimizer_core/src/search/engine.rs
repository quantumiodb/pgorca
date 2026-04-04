use crate::OptimizerError;
use crate::cost::stats::CatalogSnapshot;
use crate::cost::cardinality::estimate_selectivity_v2;
use crate::cost::model::cost_physical_op;
use crate::ir::logical::{LogicalExpr, LogicalOp};
use crate::ir::operator::Operator;
use crate::ir::physical::PhysicalOp;
use crate::ir::types::{ColumnId, Cost};
use crate::memo::{Memo, GroupId};
use crate::memo::group::Winner;
use crate::plan::extract::{PhysicalPlan, extract_plan};
use crate::properties::delivered::DeliveredProperties;
use crate::properties::logical::LogicalProperties;
use crate::properties::required::{RequiredProperties, RequiredPropsKey};
use crate::rules::RuleSet;
use crate::simplify::simplify_pass;

const MAX_GROUPS: usize = 10000;
const DEFAULT_TIMEOUT_MS: u64 = 5000;

/// Search context shared across the recursive optimization.
struct SearchCtx {
    deadline: std::time::Instant,
    rules: RuleSet,
    timed_out: bool,
}

impl SearchCtx {
    fn new(timeout_ms: u64) -> Self {
        Self {
            deadline: std::time::Instant::now()
                + std::time::Duration::from_millis(timeout_ms),
            rules: RuleSet::default(),
            timed_out: false,
        }
    }

    fn check_timeout(&mut self) -> bool {
        if !self.timed_out && std::time::Instant::now() >= self.deadline {
            self.timed_out = true;
        }
        self.timed_out
    }
}

/// Main optimization entry point.
pub fn optimize(
    input: LogicalExpr,
    catalog: &CatalogSnapshot,
) -> Result<PhysicalPlan, OptimizerError> {
    optimize_with_timeout(input, catalog, DEFAULT_TIMEOUT_MS)
}

/// Optimize with explicit timeout in milliseconds.
pub fn optimize_with_timeout(
    input: LogicalExpr,
    catalog: &CatalogSnapshot,
    timeout_ms: u64,
) -> Result<PhysicalPlan, OptimizerError> {
    let simplified = simplify_pass(input, catalog);
    let mut memo = Memo::new();
    let root = copy_in(&mut memo, &simplified);
    memo.set_root(root);

    let mut ctx = SearchCtx::new(timeout_ms);
    let required = RequiredProperties::none();
    optimize_group(&mut memo, root, &required, f64::INFINITY, &mut ctx, catalog)?;
    extract_plan(&memo, root, &required)
}

/// Recursively insert a LogicalExpr tree into the Memo, bottom-up.
fn copy_in(memo: &mut Memo, expr: &LogicalExpr) -> GroupId {
    let child_groups: Vec<GroupId> = expr.children.iter()
        .map(|child| copy_in(memo, child))
        .collect();
    let op = Operator::Logical(expr.op.clone());
    let (gid, _) = memo.insert_expr(op, child_groups, None);
    gid
}

/// Top-down Cascades search with branch-and-bound.
fn optimize_group(
    memo: &mut Memo,
    group_id: GroupId,
    required: &RequiredProperties,
    mut upper_bound: f64,
    ctx: &mut SearchCtx,
    catalog: &CatalogSnapshot,
) -> Result<Option<f64>, OptimizerError> {
    let key = RequiredPropsKey::from(required);

    // 1. Check winner cache
    if let Some(winner) = memo.get_group(group_id).winners.get(&key) {
        let cost = winner.cost.total;
        if cost <= upper_bound {
            return Ok(Some(cost));
        }
    }

    // 2. Derive logical properties (must come before costing)
    derive_logical_props(memo, group_id, catalog);

    // 3. Explore — apply transformation rules (skip if timed out)
    if !memo.get_group(group_id).explored && !ctx.check_timeout() {
        let expr_ids: Vec<_> = memo.get_group(group_id).exprs.clone();
        for eid in &expr_ids {
            for rule in &ctx.rules.xform_rules {
                let expr = memo.get_expr(*eid);
                if rule.matches(expr, memo) {
                    rule.apply(*eid, group_id, memo, catalog);
                }
            }
        }
        memo.get_group_mut(group_id).explored = true;
    }

    // 4. Implement — apply implementation rules
    if !memo.get_group(group_id).implemented {
        let expr_ids: Vec<_> = memo.get_group(group_id).exprs.clone();
        for eid in &expr_ids {
            if memo.get_expr(*eid).op.is_logical() {
                let matching: Vec<usize> = ctx.rules.impl_rules.iter().enumerate()
                    .filter(|(_, rule)| rule.matches(memo.get_expr(*eid), memo))
                    .map(|(i, _)| i)
                    .collect();
                for rule_idx in matching {
                    ctx.rules.impl_rules[rule_idx].apply(*eid, group_id, memo, catalog);
                }
            }
        }
        memo.get_group_mut(group_id).implemented = true;
    }

    // Graceful degradation: on group limit, stop exploring but still try to cost
    if memo.group_count() > MAX_GROUPS {
        ctx.timed_out = true; // stop further exploration
    }

    // 5. Cost all physical exprs, select winner
    let expr_ids: Vec<_> = memo.get_group(group_id).exprs.clone();
    for eid in &expr_ids {
        let expr = memo.get_expr(*eid);
        if !expr.op.is_physical() { continue; }

        let children = expr.children.clone();
        let phys_op = match &expr.op {
            Operator::Physical(p) => p.clone(),
            _ => continue,
        };

        // Recursively optimize children
        let mut child_costs: Vec<f64> = Vec::new();
        let mut child_rows: Vec<f64> = Vec::new();
        let mut feasible = true;
        for child_gid in &children {
            let child_budget = upper_bound - child_costs.iter().sum::<f64>();
            if child_budget <= 0.0 { feasible = false; break; }

            match optimize_group(memo, *child_gid, &RequiredProperties::none(), child_budget, ctx, catalog)? {
                Some(child_cost) => {
                    child_costs.push(child_cost);
                    let child_rows_val = memo.get_group(*child_gid).logical_props.get()
                        .map(|p| p.row_count)
                        .unwrap_or(1000.0);
                    child_rows.push(child_rows_val);
                }
                None => { feasible = false; break; }
            }
        }
        if !feasible { continue; }

        let logical_props = memo.get_group(group_id).logical_props.get()
            .cloned()
            .unwrap_or_default();

        let children_costs: Vec<Cost> = child_costs.iter()
            .map(|&c| Cost { startup: 0.0, total: c })
            .collect();

        let page_count = get_page_count(&phys_op, catalog);

        let local_cost = cost_physical_op(
            &phys_op,
            &logical_props,
            &catalog.cost_params,
            &children_costs,
            &child_rows,
            page_count,
        );

        let total_cost = local_cost.total;

        if total_cost < upper_bound {
            upper_bound = total_cost;
            let key = RequiredPropsKey::from(required);
            memo.get_group_mut(group_id).winners.insert(key, Winner {
                expr_id: *eid,
                cost: Cost { startup: local_cost.startup, total: total_cost },
                delivered_props: DeliveredProperties::none(),
            });
        }
    }

    let key = RequiredPropsKey::from(required);
    Ok(memo.get_group(group_id).winners.get(&key).map(|w| w.cost.total))
}

/// Derive logical properties for a group (using OnceLock — set once, then cached).
fn derive_logical_props(memo: &mut Memo, group_id: GroupId, catalog: &CatalogSnapshot) {
    if memo.get_group(group_id).logical_props.get().is_some() {
        return;
    }
    let expr_ids: Vec<_> = memo.get_group(group_id).exprs.clone();
    for eid in &expr_ids {
        // Derive children first (bottom-up)
        let children = memo.get_expr(*eid).children.clone();
        for child_gid in &children {
            derive_logical_props(memo, *child_gid, catalog);
        }

        let expr = memo.get_expr(*eid);
        if let Operator::Logical(ref logical_op) = expr.op {
            let children = expr.children.clone();
            let props = derive_props(logical_op, &children, memo, catalog);
            let _ = memo.get_group(group_id).logical_props.set(props);
            return;
        }
    }
}

fn derive_props(
    op: &LogicalOp,
    children: &[GroupId],
    memo: &Memo,
    catalog: &CatalogSnapshot,
) -> LogicalProperties {
    let child_props = |idx: usize| -> LogicalProperties {
        children.get(idx)
            .and_then(|gid| memo.get_group(*gid).logical_props.get())
            .cloned()
            .unwrap_or_default()
    };

    match op {
        LogicalOp::Get { table_id, columns, .. } => {
            let (row_count, avg_width) = catalog.tables.get(table_id)
                .map(|ts| {
                    let width: f64 = ts.columns.iter().map(|c| c.avg_width as f64).sum();
                    (ts.row_count, width.max(1.0))
                })
                .unwrap_or((1000.0, 32.0));
            LogicalProperties {
                output_columns: columns.clone(),
                row_count,
                table_ids: vec![*table_id],
                not_null_columns: vec![],
                unique_keys: vec![],
                avg_width,
            }
        }

        LogicalOp::Select { predicate } => {
            let base = child_props(0);
            let sel = estimate_selectivity_v2(predicate, catalog, &base.table_ids);
            let row_count = (base.row_count * sel).max(1.0);
            LogicalProperties { row_count, ..base }
        }

        LogicalOp::Project { projections } => {
            let base = child_props(0);
            let cols: Vec<ColumnId> = projections.iter().map(|(_, c)| *c).collect();
            LogicalProperties { output_columns: cols, ..base }
        }

        LogicalOp::Join { join_type, predicate } => {
            let left = child_props(0);
            let right = child_props(1);
            let join_sel = estimate_selectivity_v2(predicate, catalog, &{
                let mut t = left.table_ids.clone();
                t.extend_from_slice(&right.table_ids);
                t
            });
            let cross_product = left.row_count * right.row_count;
            let row_count = (cross_product * join_sel).max(1.0);
            let mut cols = left.output_columns.clone();
            cols.extend_from_slice(&right.output_columns);
            let mut table_ids = left.table_ids.clone();
            table_ids.extend_from_slice(&right.table_ids);
            LogicalProperties {
                output_columns: cols,
                row_count,
                table_ids,
                not_null_columns: vec![],
                unique_keys: vec![],
                avg_width: left.avg_width + right.avg_width,
            }
        }

        LogicalOp::Aggregate { group_by, aggregates } => {
            let base = child_props(0);
            // Rough estimate: 10% of input rows after aggregation
            let row_count = if group_by.is_empty() {
                1.0 // scalar aggregate → 1 row
            } else {
                (base.row_count * 0.1).max(1.0)
            };
            LogicalProperties {
                output_columns: group_by.clone(),
                row_count,
                table_ids: base.table_ids,
                not_null_columns: vec![],
                unique_keys: vec![],
                avg_width: base.avg_width,
            }
        }

        LogicalOp::Sort { .. } => child_props(0),

        LogicalOp::Limit { count, .. } => {
            let base = child_props(0);
            let row_count = if count.is_some() {
                (base.row_count * 0.01).max(1.0)
            } else {
                base.row_count
            };
            LogicalProperties { row_count, ..base }
        }

        LogicalOp::Distinct { .. } => {
            let base = child_props(0);
            let row_count = (base.row_count * 0.9).max(1.0);
            LogicalProperties { row_count, ..base }
        }
    }
}

fn get_page_count(op: &PhysicalOp, catalog: &CatalogSnapshot) -> u64 {
    match op {
        PhysicalOp::SeqScan { scanrelid } => {
            catalog.get_table_by_rte(*scanrelid)
                .map(|ts| ts.page_count)
                .unwrap_or_else(|| {
                    // fallback: first table
                    catalog.tables.values().next().map(|ts| ts.page_count).unwrap_or(0)
                })
        }
        PhysicalOp::IndexScan { scanrelid, index_oid, .. }
        | PhysicalOp::IndexOnlyScan { scanrelid, index_oid, .. }
        | PhysicalOp::BitmapHeapScan { scanrelid, index_oid, .. } => {
            catalog.get_table_by_rte(*scanrelid)
                .and_then(|ts| ts.indexes.iter().find(|idx| idx.oid == *index_oid))
                .map(|idx| idx.pages)
                .unwrap_or(1)
        }
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use crate::cost::stats::{CostParams, TableStats, ColumnStats};
    use crate::ir::types::{TableId, ColumnId};

    fn make_test_catalog() -> CatalogSnapshot {
        let mut tables = HashMap::new();
        let mut rte_to_table = HashMap::new();
        tables.insert(TableId(1), TableStats {
            oid: 16384,
            name: "t".into(),
            row_count: 1000.0,
            page_count: 10,
            columns: vec![
                ColumnStats { attnum: 1, name: "a".into(), avg_width: 4, ..Default::default() },
                ColumnStats { attnum: 2, name: "b".into(), avg_width: 32, ..Default::default() },
            ],
            indexes: vec![],
            col_id_to_attnum: HashMap::new(),
        });
        rte_to_table.insert(1u32, TableId(1));
        CatalogSnapshot { tables, rte_to_table, cost_params: CostParams::default() }
    }

    #[test]
    fn test_end_to_end_single_table() {
        let catalog = make_test_catalog();
        let expr = LogicalExpr {
            op: LogicalOp::Get {
                table_id: TableId(1),
                columns: vec![ColumnId(1), ColumnId(2)],
                rte_index: 1,
            },
            children: vec![],
        };
        let plan = optimize(expr, &catalog).unwrap();
        assert!(matches!(plan.op, PhysicalOp::SeqScan { scanrelid: 1 }));
        assert_eq!(plan.output_columns.len(), 2);
        assert!(plan.cost.total > 0.0);
        assert_eq!(plan.rows, 1000.0);
    }
}
