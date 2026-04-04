use crate::ir::logical::LogicalOp;
use crate::ir::operator::Operator;
use crate::ir::physical::PhysicalOp;
use crate::memo::{ExprId, GroupId, Memo, MemoExpr};
use crate::cost::stats::CatalogSnapshot;
use crate::rules::{Rule, RulePromise};

/// Converts LogicalOp::Append → PhysicalOp::Append.
/// The physical Append inherits all child groups from the logical Append.
#[derive(Debug)]
pub struct Append2Append;

impl Rule for Append2Append {
    fn name(&self) -> &str { "Append2Append" }
    fn is_transformation(&self) -> bool { false }

    fn matches(&self, expr: &MemoExpr, _memo: &Memo) -> bool {
        matches!(&expr.op, Operator::Logical(LogicalOp::Append))
    }

    fn apply(
        &self,
        expr_id: ExprId,
        group_id: GroupId,
        memo: &mut Memo,
        _catalog: &CatalogSnapshot,
    ) -> Vec<ExprId> {
        let children = memo.get_expr(expr_id).children.clone();
        let phys = Operator::Physical(PhysicalOp::Append);
        let (_, eid) = memo.insert_expr(phys, children, Some(group_id));
        vec![eid]
    }

    fn promise(&self, _expr: &MemoExpr, _memo: &Memo) -> RulePromise {
        RulePromise::High
    }
}
