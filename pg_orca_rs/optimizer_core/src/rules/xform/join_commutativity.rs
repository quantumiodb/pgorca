use crate::cost::stats::CatalogSnapshot;
use crate::ir::logical::LogicalOp;
use crate::ir::operator::Operator;
use crate::memo::{ExprId, GroupId, Memo, MemoExpr};
use crate::rules::{Rule, RulePromise};

/// A ⋈ B → B ⋈ A  (swap left and right children)
#[derive(Debug)]
pub struct JoinCommutativity;

impl Rule for JoinCommutativity {
    fn name(&self) -> &str { "JoinCommutativity" }
    fn is_transformation(&self) -> bool { true }

    fn matches(&self, expr: &MemoExpr, _memo: &Memo) -> bool {
        match &expr.op {
            Operator::Logical(LogicalOp::Join { join_type, .. }) => {
                use crate::ir::types::JoinType;
                matches!(join_type, JoinType::Inner | JoinType::Full)
                    && expr.children.len() == 2
            }
            _ => false,
        }
    }

    fn apply(
        &self,
        expr_id: ExprId,
        group_id: GroupId,
        memo: &mut Memo,
        _catalog: &CatalogSnapshot,
    ) -> Vec<ExprId> {
        let (join_type, predicate, children) = match &memo.get_expr(expr_id).op {
            Operator::Logical(LogicalOp::Join { join_type, predicate }) => {
                (*join_type, predicate.clone(), memo.get_expr(expr_id).children.clone())
            }
            _ => return vec![],
        };
        if children.len() != 2 { return vec![]; }

        // Swap children
        let swapped = vec![children[1], children[0]];
        let new_op = Operator::Logical(LogicalOp::Join { join_type, predicate });
        let (_, eid) = memo.insert_expr(new_op, swapped, Some(group_id));
        vec![eid]
    }

    fn promise(&self, _expr: &MemoExpr, _memo: &Memo) -> RulePromise { RulePromise::Medium }
}
