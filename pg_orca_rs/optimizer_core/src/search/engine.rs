use crate::OptimizerError;
use crate::cost::stats::CatalogSnapshot;
use crate::cost::cardinality::estimate_selectivity_v2;
use crate::ir::logical::{LogicalExpr, LogicalOp};
use crate::ir::operator::Operator;
use crate::ir::physical::PhysicalOp;
use crate::ir::types::ColumnId;
use crate::memo::{Memo, GroupId};
use crate::plan::extract::{PhysicalPlan, extract_plan};
use crate::properties::logical::LogicalProperties;
use crate::properties::required::RequiredProperties;
use crate::rules::RuleSet;
use crate::simplify::simplify_pass;
use super::scheduler::Scheduler;
use super::task::{Task, OptimizeGroupState};

pub(super) const MAX_GROUPS: usize = 10000;
const DEFAULT_TIMEOUT_MS: u64 = 5000;

/// Search context shared across the recursive optimization.
pub(super) struct SearchCtx {
    deadline: std::time::Instant,
    pub rules: RuleSet,
    pub timed_out: bool,
}

impl SearchCtx {
    pub(super) fn new(timeout_ms: u64) -> Self {
        Self {
            deadline: std::time::Instant::now()
                + std::time::Duration::from_millis(timeout_ms),
            rules: RuleSet::default(),
            timed_out: false,
        }
    }

    pub(super) fn check_timeout(&mut self) -> bool {
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
    // The top-level plan must always be serial — PG's executor cannot consume
    // a partial (parallel-worker) stream directly.
    let required = RequiredProperties::serial();
    
    let mut scheduler = Scheduler::new();
    scheduler.schedule(Task::OptimizeGroup {
        group_id: root,
        required: required.clone(),
        upper_bound: f64::INFINITY,
        state: OptimizeGroupState::Init,
    });
    
    scheduler.run(&mut memo, catalog, &mut ctx)?;
    
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

pub(super) fn derive_props(
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
                fd_keys: vec![],
                equivalence_classes: vec![],
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

        LogicalOp::Join { join_type: _, predicate } => {
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
                fd_keys: vec![],
                equivalence_classes: vec![],
                avg_width: left.avg_width + right.avg_width,
            }
        }

        LogicalOp::Aggregate { group_by, aggregates: _ } => {
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
                fd_keys: vec![],
                equivalence_classes: vec![],
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

        LogicalOp::Window { .. } => child_props(0), // same row count, adds window columns

        LogicalOp::Append => {
            // Sum row counts across all children; output columns from first child
            let mut row_count = 0.0;
            let mut output_columns = vec![];
            let mut table_ids = vec![];
            let mut avg_width = 0.0;
            for i in 0..children.len() {
                let cp = child_props(i);
                row_count += cp.row_count;
                table_ids.extend_from_slice(&cp.table_ids);
                if i == 0 {
                    output_columns = cp.output_columns.clone();
                    avg_width = cp.avg_width;
                }
            }
            LogicalProperties {
                output_columns,
                row_count: row_count.max(1.0),
                table_ids,
                not_null_columns: vec![],
                unique_keys: vec![],
                fd_keys: vec![],
                equivalence_classes: vec![],
                avg_width,
            }
        }
    }
}

pub(super) fn get_page_count(op: &PhysicalOp, catalog: &CatalogSnapshot) -> u64 {
    match op {
        PhysicalOp::SeqScan { scanrelid }
        | PhysicalOp::ParallelSeqScan { scanrelid, .. } => {
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
    use crate::cost::stats::{CostModel, TableStats, ColumnStats};
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
        CatalogSnapshot { 
            tables, 
            rte_to_table, 
            cost_model: CostModel::default(),
            parallel_safe: true,
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
        assert!(matches!(plan.op, PhysicalOp::SeqScan { scanrelid: 1 }));
        assert_eq!(plan.output_columns.len(), 2);
        assert!(plan.cost.total > 0.0);
        assert_eq!(plan.rows, 1000.0);
    }
}
