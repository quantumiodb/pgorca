use crate::OptimizerError;
use crate::cost::stats::CatalogSnapshot;
use crate::cost::model::cost_physical_op;
use crate::ir::logical::{LogicalExpr, LogicalOp};
use crate::ir::operator::Operator;
use crate::ir::types::Cost;
use crate::memo::{Memo, GroupId};
use crate::memo::group::Winner;
use crate::plan::extract::{PhysicalPlan, extract_plan};
use crate::properties::delivered::DeliveredProperties;
use crate::properties::logical::LogicalProperties;
use crate::properties::required::{RequiredProperties, RequiredPropsKey};
use crate::rules::RuleSet;
use crate::simplify::simplify_pass;

const MAX_GROUPS: usize = 10000;

/// Main optimization entry point.
pub fn optimize(
    input: LogicalExpr,
    catalog: &CatalogSnapshot,
) -> Result<PhysicalPlan, OptimizerError> {
    // 1. Pre-Cascades simplification
    let simplified = simplify_pass(input, catalog);

    // 2. Create Memo and copy-in the expression tree
    let mut memo = Memo::new();
    let root = copy_in(&mut memo, &simplified);
    memo.set_root(root);

    // 3. Run Cascades search
    let rules = RuleSet::default_m1();
    let required = RequiredProperties::none();
    optimize_group(&mut memo, root, &required, f64::INFINITY, &rules, catalog)?;

    // 4. Extract physical plan from winners
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
    rules: &RuleSet,
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

    // 2. Derive logical properties
    derive_logical_props(memo, group_id, catalog);

    // 3. Explore — apply transformation rules (none in M1)
    if !memo.get_group(group_id).explored {
        let expr_ids: Vec<_> = memo.get_group(group_id).exprs.clone();
        for eid in &expr_ids {
            for rule in &rules.xform_rules {
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
            let is_logical = memo.get_expr(*eid).op.is_logical();
            if is_logical {
                // Collect matching rules first, then apply
                let matching: Vec<usize> = rules.impl_rules.iter().enumerate()
                    .filter(|(_, rule)| rule.matches(memo.get_expr(*eid), memo))
                    .map(|(i, _)| i)
                    .collect();
                for rule_idx in matching {
                    rules.impl_rules[rule_idx].apply(*eid, group_id, memo, catalog);
                }
            }
        }
        memo.get_group_mut(group_id).implemented = true;
    }

    if memo.group_count() > MAX_GROUPS {
        return Err(OptimizerError::GroupLimitExceeded);
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
        let mut total_child_cost = 0.0;
        let mut feasible = true;
        for child_gid in &children {
            let child_budget = upper_bound - total_child_cost;
            if child_budget <= 0.0 { feasible = false; break; }

            match optimize_group(memo, *child_gid, &RequiredProperties::none(), child_budget, rules, catalog)? {
                Some(child_cost) => total_child_cost += child_cost,
                None => { feasible = false; break; }
            }
        }

        if !feasible { continue; }

        // Get logical properties for cost computation
        let logical_props = memo.get_group(group_id).logical_props.get()
            .cloned()
            .unwrap_or(LogicalProperties {
                output_columns: vec![],
                row_count: 0.0,
                table_ids: vec![],
                not_null_columns: vec![],
                unique_keys: vec![],
                avg_width: 0.0,
            });

        // Get page count for this table (for scan operators)
        let page_count = get_page_count(&phys_op, catalog);

        let local_cost = cost_physical_op(
            &phys_op,
            &logical_props,
            &catalog.cost_params,
            &[], // children costs
            &[], // children rows
            page_count,
        );

        let total_cost = local_cost.total + total_child_cost;

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

/// Derive logical properties for a group (using OnceLock).
fn derive_logical_props(memo: &mut Memo, group_id: GroupId, catalog: &CatalogSnapshot) {
    if memo.get_group(group_id).logical_props.get().is_some() {
        return;
    }

    // Find the first logical expression in the group
    let expr_ids: Vec<_> = memo.get_group(group_id).exprs.clone();
    for eid in &expr_ids {
        let expr = memo.get_expr(*eid);
        if let Operator::Logical(ref logical_op) = expr.op {
            let props = derive_props_for_op(logical_op, catalog);
            let _ = memo.get_group(group_id).logical_props.set(props);
            return;
        }
    }
}

fn derive_props_for_op(op: &LogicalOp, catalog: &CatalogSnapshot) -> LogicalProperties {
    match op {
        LogicalOp::Get { table_id, columns, .. } => {
            let (row_count, avg_width) = catalog.tables.get(table_id)
                .map(|ts| {
                    let width: f64 = ts.columns.iter()
                        .map(|c| c.avg_width as f64)
                        .sum();
                    (ts.row_count, width)
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
        _ => LogicalProperties {
            output_columns: vec![],
            row_count: 1000.0,
            table_ids: vec![],
            not_null_columns: vec![],
            unique_keys: vec![],
            avg_width: 32.0,
        },
    }
}

fn get_page_count(op: &crate::ir::physical::PhysicalOp, catalog: &CatalogSnapshot) -> u64 {
    use crate::ir::physical::PhysicalOp;
    match op {
        PhysicalOp::SeqScan { .. } => {
            // Use the first (and typically only) table's page count
            catalog.tables.values().next()
                .map(|ts| ts.page_count)
                .unwrap_or(0)
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
        });
        CatalogSnapshot {
            tables,
            cost_params: CostParams::default(),
        }
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
        match &plan.op {
            crate::ir::physical::PhysicalOp::SeqScan { scanrelid } => {
                assert_eq!(*scanrelid, 1);
            }
            other => panic!("expected SeqScan, got {:?}", other),
        }
        assert_eq!(plan.output_columns.len(), 2);
        assert!(plan.cost.total > 0.0);
        assert_eq!(plan.rows, 1000.0);
    }
}
