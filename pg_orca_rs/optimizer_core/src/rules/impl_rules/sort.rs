use crate::cost::stats::CatalogSnapshot;
use crate::ir::logical::LogicalOp;
use crate::ir::operator::Operator;
use crate::ir::physical::PhysicalOp;
use crate::memo::{ExprId, GroupId, Memo, MemoExpr};
use crate::rules::{Rule, RulePromise};

// ── Sort2Sort ─────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct Sort2Sort;

impl Rule for Sort2Sort {
    fn name(&self) -> &str { "Sort2Sort" }
    fn is_transformation(&self) -> bool { false }

    fn matches(&self, expr: &MemoExpr, _memo: &Memo) -> bool {
        matches!(&expr.op, Operator::Logical(LogicalOp::Sort { .. }))
    }

    fn apply(
        &self,
        expr_id: ExprId,
        group_id: GroupId,
        memo: &mut Memo,
        _catalog: &CatalogSnapshot,
    ) -> Vec<ExprId> {
        let (keys, children) = match &memo.get_expr(expr_id).op {
            Operator::Logical(LogicalOp::Sort { keys }) => {
                (keys.clone(), memo.get_expr(expr_id).children.clone())
            }
            _ => return vec![],
        };
        let phys = Operator::Physical(PhysicalOp::Sort { keys });
        let (_, eid) = memo.insert_expr(phys, children, Some(group_id));
        vec![eid]
    }

    fn promise(&self, _expr: &MemoExpr, _memo: &Memo) -> RulePromise { RulePromise::High }
}

// ── Limit2Limit ───────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct Limit2Limit;

impl Rule for Limit2Limit {
    fn name(&self) -> &str { "Limit2Limit" }
    fn is_transformation(&self) -> bool { false }

    fn matches(&self, expr: &MemoExpr, _memo: &Memo) -> bool {
        matches!(&expr.op, Operator::Logical(LogicalOp::Limit { .. }))
    }

    fn apply(
        &self,
        expr_id: ExprId,
        group_id: GroupId,
        memo: &mut Memo,
        _catalog: &CatalogSnapshot,
    ) -> Vec<ExprId> {
        let (offset, count, children) = match &memo.get_expr(expr_id).op {
            Operator::Logical(LogicalOp::Limit { offset, count }) => {
                (offset.clone(), count.clone(), memo.get_expr(expr_id).children.clone())
            }
            _ => return vec![],
        };
        let phys = Operator::Physical(PhysicalOp::Limit { offset, count });
        let (_, eid) = memo.insert_expr(phys, children, Some(group_id));
        vec![eid]
    }

    fn promise(&self, _expr: &MemoExpr, _memo: &Memo) -> RulePromise { RulePromise::High }
}

// ── Distinct2Unique ───────────────────────────────────────────────────────────

/// Implements DISTINCT via Sort + Unique.
/// Inserts a Sort node below the Unique node.
#[derive(Debug)]
pub struct Distinct2Unique;

impl Rule for Distinct2Unique {
    fn name(&self) -> &str { "Distinct2Unique" }
    fn is_transformation(&self) -> bool { false }

    fn matches(&self, expr: &MemoExpr, _memo: &Memo) -> bool {
        matches!(&expr.op, Operator::Logical(LogicalOp::Distinct { .. }))
    }

    fn apply(
        &self,
        expr_id: ExprId,
        group_id: GroupId,
        memo: &mut Memo,
        _catalog: &CatalogSnapshot,
    ) -> Vec<ExprId> {
        let (num_cols, children) = match &memo.get_expr(expr_id).op {
            Operator::Logical(LogicalOp::Distinct { columns }) => {
                (columns.len(), memo.get_expr(expr_id).children.clone())
            }
            _ => return vec![],
        };
        // We model this as a Unique node; the Sort is implicit in the cost
        // (actual Sort insertion happens in the enforcer pass, future work)
        let phys = Operator::Physical(PhysicalOp::Unique { num_cols });
        let (_, eid) = memo.insert_expr(phys, children, Some(group_id));
        vec![eid]
    }

    fn promise(&self, _expr: &MemoExpr, _memo: &Memo) -> RulePromise { RulePromise::High }
}
