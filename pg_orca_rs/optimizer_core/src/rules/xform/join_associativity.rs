use crate::cost::stats::CatalogSnapshot;
use crate::ir::logical::LogicalOp;
use crate::ir::operator::Operator;
use crate::ir::types::JoinType;
use crate::memo::{ExprId, GroupId, Memo, MemoExpr};
use crate::rules::{Rule, RulePromise};

/// (A ⋈ B) ⋈ C → A ⋈ (B ⋈ C)  (right-rotation)
///
/// Only applies to inner joins (safe without predicate movement).
#[derive(Debug)]
pub struct JoinAssociativity;

impl Rule for JoinAssociativity {
    fn name(&self) -> &str { "JoinAssociativity" }
    fn is_transformation(&self) -> bool { true }

    fn matches(&self, expr: &MemoExpr, memo: &Memo) -> bool {
        // Top join must be Inner
        if !matches!(&expr.op, Operator::Logical(LogicalOp::Join { join_type: JoinType::Inner, .. })) {
            return false;
        }
        if expr.children.len() != 2 { return false; }
        // Left child group must contain an inner Join
        let left_gid = expr.children[0];
        memo.get_group(left_gid).exprs.iter().any(|&eid| {
            matches!(
                &memo.get_expr(eid).op,
                Operator::Logical(LogicalOp::Join { join_type: JoinType::Inner, .. })
            )
        })
    }

    fn apply(
        &self,
        expr_id: ExprId,
        group_id: GroupId,
        memo: &mut Memo,
        _catalog: &CatalogSnapshot,
    ) -> Vec<ExprId> {
        // top: (A ⋈_pred1 B) ⋈_pred2 C
        let (top_pred, top_children) = match &memo.get_expr(expr_id).op {
            Operator::Logical(LogicalOp::Join { predicate, join_type: JoinType::Inner }) => {
                (predicate.clone(), memo.get_expr(expr_id).children.clone())
            }
            _ => return vec![],
        };
        if top_children.len() != 2 { return vec![]; }
        let left_gid = top_children[0];
        let c_gid = top_children[1];

        // Find left join expr
        let left_exprs: Vec<_> = memo.get_group(left_gid).exprs.clone();
        let found = left_exprs.iter().find_map(|&eid| {
            match &memo.get_expr(eid).op {
                Operator::Logical(LogicalOp::Join { predicate, join_type: JoinType::Inner }) => {
                    Some((predicate.clone(), memo.get_expr(eid).children.clone()))
                }
                _ => None,
            }
        });
        let (ab_pred, ab_children) = match found {
            Some(x) => x,
            None => return vec![],
        };
        if ab_children.len() != 2 { return vec![]; }
        let a_gid = ab_children[0];
        let b_gid = ab_children[1];

        // new inner: B ⋈_top_pred C
        let inner_op = Operator::Logical(LogicalOp::Join {
            join_type: JoinType::Inner,
            predicate: top_pred,
        });
        let (bc_gid, _) = memo.insert_expr(inner_op, vec![b_gid, c_gid], None);

        // new outer: A ⋈_ab_pred (B ⋈ C)
        let outer_op = Operator::Logical(LogicalOp::Join {
            join_type: JoinType::Inner,
            predicate: ab_pred,
        });
        let (_, eid) = memo.insert_expr(outer_op, vec![a_gid, bc_gid], Some(group_id));
        vec![eid]
    }

    fn promise(&self, _expr: &MemoExpr, _memo: &Memo) -> RulePromise { RulePromise::Medium }
}
