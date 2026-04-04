use crate::cost::stats::CatalogSnapshot;
use crate::ir::logical::LogicalOp;
use crate::ir::operator::Operator;
use crate::ir::physical::PhysicalOp;
use crate::memo::{ExprId, GroupId, Memo, MemoExpr};
use crate::rules::{Rule, RulePromise};

/// Window → WindowAgg (pass-through: one physical strategy).
#[derive(Debug)]
pub struct Window2WindowAgg;

impl Rule for Window2WindowAgg {
    fn name(&self) -> &str { "Window2WindowAgg" }
    fn is_transformation(&self) -> bool { false }

    fn matches(&self, expr: &MemoExpr, _memo: &Memo) -> bool {
        matches!(&expr.op, Operator::Logical(LogicalOp::Window { .. }))
    }

    fn apply(
        &self,
        expr_id: ExprId,
        group_id: GroupId,
        memo: &mut Memo,
        _catalog: &CatalogSnapshot,
    ) -> Vec<ExprId> {
        let clauses = match &memo.get_expr(expr_id).op {
            Operator::Logical(LogicalOp::Window { clauses }) => clauses.clone(),
            _ => return vec![],
        };
        let children = memo.get_expr(expr_id).children.clone();
        let phys = Operator::Physical(PhysicalOp::WindowAgg { clauses });
        let (_, eid) = memo.insert_expr(phys, children, Some(group_id));
        vec![eid]
    }

    fn promise(&self, _expr: &MemoExpr, _memo: &Memo) -> RulePromise { RulePromise::High }
}
