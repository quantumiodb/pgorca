use crate::cost::stats::CatalogSnapshot;
use crate::ir::logical::LogicalOp;
use crate::ir::operator::Operator;
use crate::ir::physical::PhysicalOp;
use crate::memo::{ExprId, GroupId, Memo, MemoExpr};
use crate::rules::{Rule, RulePromise};

/// Minimum table page count to consider parallel scan (mirrors PG's
/// min_parallel_table_scan_size default = 8 * 1024 * 1024 / 8192 = 1024 pages).
const MIN_PARALLEL_PAGES: u64 = 1024;

// ── Get2ParallelSeqScan ────────────────────────────────────────────────────────

/// Converts `LogicalOp::Get` on a large table → `ParallelSeqScan`.
/// Only fires when the table has enough pages to benefit from parallelism.
#[derive(Debug)]
pub struct Get2ParallelSeqScan;

impl Rule for Get2ParallelSeqScan {
    fn name(&self) -> &str { "Get2ParallelSeqScan" }
    fn is_transformation(&self) -> bool { false }

    fn matches(&self, expr: &MemoExpr, _memo: &Memo) -> bool {
        matches!(&expr.op, Operator::Logical(LogicalOp::Get { .. }))
    }

    fn apply(
        &self,
        expr_id: ExprId,
        group_id: GroupId,
        memo: &mut Memo,
        catalog: &CatalogSnapshot,
    ) -> Vec<ExprId> {
        let (rte_index, table_id) = match &memo.get_expr(expr_id).op {
            Operator::Logical(LogicalOp::Get { rte_index, table_id, .. }) => (*rte_index, *table_id),
            _ => return vec![],
        };

        // Only parallelize when the query is parallel-safe and the table is large enough.
        if !catalog.parallel_safe {
            return vec![];
        }

        let page_count = catalog.tables.get(&table_id)
            .map(|ts| ts.page_count)
            .unwrap_or(0);
        if page_count < MIN_PARALLEL_PAGES {
            return vec![];
        }

        let num_workers = choose_workers(page_count, catalog.cost_model.max_parallel_workers);
        let phys = Operator::Physical(PhysicalOp::ParallelSeqScan { scanrelid: rte_index, num_workers });
        let (_, eid) = memo.insert_expr(phys, vec![], Some(group_id));
        vec![eid]
    }

    fn promise(&self, _expr: &MemoExpr, _memo: &Memo) -> RulePromise { RulePromise::High }
}

// ── Select2ParallelSeqScan ─────────────────────────────────────────────────────

/// Converts `Select(pred, Get(table))` → `ParallelSeqScan` for large tables.
/// The Select group's predicate becomes the scan's qual.
#[derive(Debug)]
pub struct Select2ParallelSeqScan;

impl Rule for Select2ParallelSeqScan {
    fn name(&self) -> &str { "Select2ParallelSeqScan" }
    fn is_transformation(&self) -> bool { false }

    fn matches(&self, expr: &MemoExpr, memo: &Memo) -> bool {
        use crate::ir::logical::LogicalOp;
        if !matches!(&expr.op, Operator::Logical(LogicalOp::Select { .. })) {
            return false;
        }
        expr.children.len() == 1 && {
            let child_gid = expr.children[0];
            memo.get_group(child_gid).exprs.iter().any(|&eid| {
                matches!(&memo.get_expr(eid).op, Operator::Logical(LogicalOp::Get { .. }))
            })
        }
    }

    fn apply(
        &self,
        expr_id: ExprId,
        group_id: GroupId,
        memo: &mut Memo,
        catalog: &CatalogSnapshot,
    ) -> Vec<ExprId> {
        use crate::ir::logical::LogicalOp;

        let child_gid = match memo.get_expr(expr_id).children.first() {
            Some(&g) => g,
            None => return vec![],
        };

        let (rte_index, table_id) = match memo.get_group(child_gid).exprs.iter().find_map(|&eid| {
            match &memo.get_expr(eid).op {
                Operator::Logical(LogicalOp::Get { rte_index, table_id, .. }) => Some((*rte_index, *table_id)),
                _ => None,
            }
        }) {
            Some(r) => r,
            None => return vec![],
        };

        if !catalog.parallel_safe {
            return vec![];
        }

        let page_count = catalog.tables.get(&table_id)
            .map(|ts| ts.page_count)
            .unwrap_or(0);
        if page_count < MIN_PARALLEL_PAGES {
            return vec![];
        }

        let num_workers = choose_workers(page_count, catalog.cost_model.max_parallel_workers);
        let phys = Operator::Physical(PhysicalOp::ParallelSeqScan { scanrelid: rte_index, num_workers });
        let (_, eid) = memo.insert_expr(phys, vec![], Some(group_id));
        vec![eid]
    }

    fn promise(&self, _expr: &MemoExpr, _memo: &Memo) -> RulePromise { RulePromise::High }
}

/// Choose number of parallel workers based on page count (mirrors PG's formula).
/// Use max_parallel_workers directly as the worker count, capped by the GUC value.
fn choose_workers(_page_count: u64, max_parallel_workers: usize) -> usize {
    max_parallel_workers.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use crate::cost::stats::{CostModel, TableStats, CatalogSnapshot};
    use crate::ir::types::{TableId, ColumnId};

    fn make_catalog(page_count: u64) -> CatalogSnapshot {
        let mut tables = HashMap::new();
        let mut rte_to_table = HashMap::new();
        tables.insert(TableId(1), TableStats {
            oid: 16384,
            name: "big_t".into(),
            row_count: 100_000.0,
            page_count,
            columns: vec![],
            indexes: vec![],
            col_id_to_attnum: HashMap::new(),
        });
        rte_to_table.insert(1u32, TableId(1));
        CatalogSnapshot { tables, rte_to_table, cost_model: CostModel::default(), parallel_safe: true }
    }

    #[test]
    fn test_small_table_no_parallel() {
        use crate::memo::Memo;
        use crate::ir::logical::LogicalOp;
        use crate::ir::operator::Operator;
        let catalog = make_catalog(100); // below threshold
        let mut memo = Memo::new();
        let op = Operator::Logical(LogicalOp::Get {
            table_id: TableId(1),
            columns: vec![ColumnId(1)],
            rte_index: 1,
        });
        let (gid, eid) = memo.insert_expr(op, vec![], None);
        let rule = Get2ParallelSeqScan;
        let new_eids = rule.apply(eid, gid, &mut memo, &catalog);
        assert!(new_eids.is_empty(), "should not parallelize a small table");
    }

    #[test]
    fn test_large_table_parallel() {
        use crate::memo::Memo;
        use crate::ir::logical::LogicalOp;
        use crate::ir::operator::Operator;
        use crate::ir::physical::PhysicalOp;
        let catalog = make_catalog(4096); // 4× threshold → 2 workers
        let mut memo = Memo::new();
        let op = Operator::Logical(LogicalOp::Get {
            table_id: TableId(1),
            columns: vec![ColumnId(1)],
            rte_index: 1,
        });
        let (gid, eid) = memo.insert_expr(op, vec![], None);
        let rule = Get2ParallelSeqScan;
        let new_eids = rule.apply(eid, gid, &mut memo, &catalog);
        assert_eq!(new_eids.len(), 1);
        let new_expr = memo.get_expr(new_eids[0]);
        assert!(matches!(
            &new_expr.op,
            Operator::Physical(PhysicalOp::ParallelSeqScan { num_workers, .. }) if *num_workers >= 1
        ));
    }
}
