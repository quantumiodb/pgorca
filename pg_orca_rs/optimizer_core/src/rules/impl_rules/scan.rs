use crate::cost::stats::{CatalogSnapshot, predicate_matches_btree_index};
use crate::ir::logical::LogicalOp;
use crate::ir::operator::Operator;
use crate::ir::physical::PhysicalOp;
use crate::ir::types::{RteIndex, TableId};
use crate::ir::physical::ScanDirection;
use crate::memo::{ExprId, GroupId, Memo, MemoExpr};
use crate::rules::{Rule, RulePromise};

// ── helpers ──────────────────────────────────────────────────────────────────

fn find_get_in_group(memo: &Memo, gid: GroupId) -> Option<(RteIndex, TableId)> {
    memo.get_group(gid).exprs.iter().find_map(|&eid| {
        match &memo.get_expr(eid).op {
            Operator::Logical(LogicalOp::Get { rte_index, table_id, .. }) => {
                Some((*rte_index, *table_id))
            }
            _ => None,
        }
    })
}

fn has_get_in_group(memo: &Memo, gid: GroupId) -> bool {
    find_get_in_group(memo, gid).is_some()
}

// ── Get2SeqScan ───────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct Get2SeqScan;

impl Rule for Get2SeqScan {
    fn name(&self) -> &str { "Get2SeqScan" }
    fn is_transformation(&self) -> bool { false }

    fn matches(&self, expr: &MemoExpr, _memo: &Memo) -> bool {
        matches!(&expr.op, Operator::Logical(LogicalOp::Get { .. }))
    }

    fn apply(
        &self,
        expr_id: ExprId,
        group_id: GroupId,
        memo: &mut Memo,
        _catalog: &CatalogSnapshot,
    ) -> Vec<ExprId> {
        let rte_index = match &memo.get_expr(expr_id).op {
            Operator::Logical(LogicalOp::Get { rte_index, .. }) => *rte_index,
            _ => return vec![],
        };
        let phys = Operator::Physical(PhysicalOp::SeqScan { scanrelid: rte_index });
        let (_, eid) = memo.insert_expr(phys, vec![], Some(group_id));
        vec![eid]
    }

    fn promise(&self, _expr: &MemoExpr, _memo: &Memo) -> RulePromise { RulePromise::High }
}

// ── Select2SeqScan ────────────────────────────────────────────────────────────

/// Converts Select(pred, Get(table)) → SeqScan with qual in the Select group.
#[derive(Debug)]
pub struct Select2SeqScan;

impl Rule for Select2SeqScan {
    fn name(&self) -> &str { "Select2SeqScan" }
    fn is_transformation(&self) -> bool { false }

    fn matches(&self, expr: &MemoExpr, memo: &Memo) -> bool {
        if !matches!(&expr.op, Operator::Logical(LogicalOp::Select { .. })) {
            return false;
        }
        expr.children.len() == 1 && has_get_in_group(memo, expr.children[0])
    }

    fn apply(
        &self,
        expr_id: ExprId,
        group_id: GroupId,
        memo: &mut Memo,
        _catalog: &CatalogSnapshot,
    ) -> Vec<ExprId> {
        let child_gid = match memo.get_expr(expr_id).children.first() {
            Some(&g) => g,
            None => return vec![],
        };
        let (rte_index, _) = match find_get_in_group(memo, child_gid) {
            Some(r) => r,
            None => return vec![],
        };
        let phys = Operator::Physical(PhysicalOp::SeqScan { scanrelid: rte_index });
        // No children — this physical op handles the full scan+filter
        let (_, eid) = memo.insert_expr(phys, vec![], Some(group_id));
        vec![eid]
    }

    fn promise(&self, _expr: &MemoExpr, _memo: &Memo) -> RulePromise { RulePromise::Medium }
}

// ── Select2IndexScan ──────────────────────────────────────────────────────────

/// Converts Select(pred, Get(table)) → IndexScan when predicate matches B-tree.
#[derive(Debug)]
pub struct Select2IndexScan;

impl Rule for Select2IndexScan {
    fn name(&self) -> &str { "Select2IndexScan" }
    fn is_transformation(&self) -> bool { false }

    fn matches(&self, expr: &MemoExpr, memo: &Memo) -> bool {
        if !matches!(&expr.op, Operator::Logical(LogicalOp::Select { .. })) {
            return false;
        }
        expr.children.len() == 1 && has_get_in_group(memo, expr.children[0])
    }

    fn apply(
        &self,
        expr_id: ExprId,
        group_id: GroupId,
        memo: &mut Memo,
        catalog: &CatalogSnapshot,
    ) -> Vec<ExprId> {
        let predicate = match &memo.get_expr(expr_id).op {
            Operator::Logical(LogicalOp::Select { predicate }) => predicate.clone(),
            _ => return vec![],
        };
        let child_gid = match memo.get_expr(expr_id).children.first() {
            Some(&g) => g,
            None => return vec![],
        };
        let (rte_index, table_id) = match find_get_in_group(memo, child_gid) {
            Some(r) => r,
            None => return vec![],
        };
        let table_stats = match catalog.tables.get(&table_id) {
            Some(ts) => ts,
            None => return vec![],
        };

        let mut new_eids = Vec::new();
        for index in &table_stats.indexes {
            if !predicate_matches_btree_index(&predicate, index, table_stats) {
                continue;
            }
            let phys = Operator::Physical(PhysicalOp::IndexScan {
                scanrelid: rte_index,
                index_oid: index.oid,
                scan_direction: ScanDirection::Forward,
                index_quals: vec![predicate.clone()],
                index_order_keys: vec![],
            });
            let (_, eid) = memo.insert_expr(phys, vec![], Some(group_id));
            new_eids.push(eid);
        }
        new_eids
    }

    fn promise(&self, _expr: &MemoExpr, _memo: &Memo) -> RulePromise { RulePromise::High }
}

// ── Select2BitmapScan ─────────────────────────────────────────────────────────

/// Converts Select(pred, Get(table)) → BitmapHeapScan when predicate matches
/// an index. The BitmapIndexScan is embedded inside BitmapHeapScan at plan time.
#[derive(Debug)]
pub struct Select2BitmapScan;

impl Rule for Select2BitmapScan {
    fn name(&self) -> &str { "Select2BitmapScan" }
    fn is_transformation(&self) -> bool { false }

    fn matches(&self, expr: &MemoExpr, memo: &Memo) -> bool {
        if !matches!(&expr.op, Operator::Logical(LogicalOp::Select { .. })) {
            return false;
        }
        expr.children.len() == 1 && has_get_in_group(memo, expr.children[0])
    }

    fn apply(
        &self,
        expr_id: ExprId,
        group_id: GroupId,
        memo: &mut Memo,
        catalog: &CatalogSnapshot,
    ) -> Vec<ExprId> {
        let predicate = match &memo.get_expr(expr_id).op {
            Operator::Logical(LogicalOp::Select { predicate }) => predicate.clone(),
            _ => return vec![],
        };
        let child_gid = match memo.get_expr(expr_id).children.first() {
            Some(&g) => g,
            None => return vec![],
        };
        let (rte_index, table_id) = match find_get_in_group(memo, child_gid) {
            Some(r) => r,
            None => return vec![],
        };
        let table_stats = match catalog.tables.get(&table_id) {
            Some(ts) => ts,
            None => return vec![],
        };

        let mut new_eids = Vec::new();
        for index in &table_stats.indexes {
            if !predicate_matches_btree_index(&predicate, index, table_stats) {
                continue;
            }
            let phys = Operator::Physical(PhysicalOp::BitmapHeapScan {
                scanrelid: rte_index,
                index_oid: index.oid,
                index_quals: vec![predicate.clone()],
            });
            let (_, eid) = memo.insert_expr(phys, vec![], Some(group_id));
            new_eids.push(eid);
        }
        new_eids
    }

    fn promise(&self, _expr: &MemoExpr, _memo: &Memo) -> RulePromise { RulePromise::Medium }
}

// ── Select2IndexOnlyScan ──────────────────────────────────────────────────────

/// Converts Select(pred, Get(table)) → IndexOnlyScan when all output columns
/// are covered by the index (covering index).
#[derive(Debug)]
pub struct Select2IndexOnlyScan;

impl Rule for Select2IndexOnlyScan {
    fn name(&self) -> &str { "Select2IndexOnlyScan" }
    fn is_transformation(&self) -> bool { false }

    fn matches(&self, expr: &MemoExpr, memo: &Memo) -> bool {
        if !matches!(&expr.op, Operator::Logical(LogicalOp::Select { .. })) {
            return false;
        }
        expr.children.len() == 1 && has_get_in_group(memo, expr.children[0])
    }

    fn apply(
        &self,
        expr_id: ExprId,
        group_id: GroupId,
        memo: &mut Memo,
        catalog: &CatalogSnapshot,
    ) -> Vec<ExprId> {
        let predicate = match &memo.get_expr(expr_id).op {
            Operator::Logical(LogicalOp::Select { predicate }) => predicate.clone(),
            _ => return vec![],
        };
        let child_gid = match memo.get_expr(expr_id).children.first() {
            Some(&g) => g,
            None => return vec![],
        };
        let (rte_index, table_id) = match find_get_in_group(memo, child_gid) {
            Some(r) => r,
            None => return vec![],
        };
        let table_stats = match catalog.tables.get(&table_id) {
            Some(ts) => ts,
            None => return vec![],
        };
        // Get the required output columns from the Get group's logical props
        let output_cols: Vec<_> = memo.get_group(child_gid)
            .logical_props.get()
            .map(|p| p.output_columns.clone())
            .unwrap_or_default();

        let mut new_eids = Vec::new();
        for index in &table_stats.indexes {
            if !predicate_matches_btree_index(&predicate, index, table_stats) {
                continue;
            }
            // Check covering: all output columns must be in the index
            let index_attnums: std::collections::HashSet<i16> =
                index.columns.iter().copied().collect();
            let all_covered = output_cols.iter().all(|col_id| {
                table_stats.col_id_to_attnum.get(col_id)
                    .map_or(false, |att| index_attnums.contains(att))
            });
            if !all_covered {
                continue;
            }
            let phys = Operator::Physical(PhysicalOp::IndexOnlyScan {
                scanrelid: rte_index,
                index_oid: index.oid,
                index_quals: vec![predicate.clone()],
            });
            let (_, eid) = memo.insert_expr(phys, vec![], Some(group_id));
            new_eids.push(eid);
        }
        new_eids
    }

    fn promise(&self, _expr: &MemoExpr, _memo: &Memo) -> RulePromise { RulePromise::High }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use crate::cost::stats::{CostModel, CatalogSnapshot};
    use crate::ir::types::{TableId, ColumnId};

    #[test]
    fn test_get2seqscan() {
        let mut memo = Memo::new();
        let catalog = CatalogSnapshot {
            tables: HashMap::new(),
            rte_to_table: HashMap::new(),
            cost_model: CostModel::default(),
        };
        let op = Operator::Logical(LogicalOp::Get {
            table_id: TableId(1),
            columns: vec![ColumnId(1)],
            rte_index: 1,
        });
        let (gid, eid) = memo.insert_expr(op, vec![], None);
        let rule = Get2SeqScan;
        assert!(rule.matches(memo.get_expr(eid), &memo));
        let new_eids = rule.apply(eid, gid, &mut memo, &catalog);
        assert_eq!(new_eids.len(), 1);
        let new_expr = memo.get_expr(new_eids[0]);
        assert!(matches!(
            &new_expr.op,
            Operator::Physical(PhysicalOp::SeqScan { scanrelid: 1 })
        ));
        assert_eq!(memo.get_group(gid).exprs.len(), 2);
    }
}
