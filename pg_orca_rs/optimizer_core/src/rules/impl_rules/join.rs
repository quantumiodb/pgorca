use crate::cost::stats::CatalogSnapshot;
use crate::ir::logical::LogicalOp;
use crate::ir::operator::Operator;
use crate::ir::physical::PhysicalOp;
use crate::memo::{ExprId, GroupId, Memo, MemoExpr};
use crate::rules::{Rule, RulePromise};

fn get_join_info(expr: &MemoExpr) -> Option<(crate::ir::types::JoinType, crate::ir::scalar::ScalarExpr)> {
    match &expr.op {
        Operator::Logical(LogicalOp::Join { join_type, predicate }) => {
            Some((*join_type, predicate.clone()))
        }
        _ => None,
    }
}

// ── Join2HashJoin ─────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct Join2HashJoin;

impl Rule for Join2HashJoin {
    fn name(&self) -> &str { "Join2HashJoin" }
    fn is_transformation(&self) -> bool { false }

    fn matches(&self, expr: &MemoExpr, _memo: &Memo) -> bool {
        matches!(&expr.op, Operator::Logical(LogicalOp::Join { .. }))
            && expr.children.len() == 2
    }

    fn apply(
        &self,
        expr_id: ExprId,
        group_id: GroupId,
        memo: &mut Memo,
        _catalog: &CatalogSnapshot,
    ) -> Vec<ExprId> {
        let (join_type, predicate) = match get_join_info(memo.get_expr(expr_id)) {
            Some(j) => j,
            None => return vec![],
        };
        let children = memo.get_expr(expr_id).children.clone();

        // Extract equality conditions for hash clauses
        let hash_clauses = extract_equality_clauses(&predicate);

        let phys = Operator::Physical(PhysicalOp::HashJoin { join_type, hash_clauses });
        let (_, eid) = memo.insert_expr(phys, children, Some(group_id));
        vec![eid]
    }

    fn promise(&self, _expr: &MemoExpr, _memo: &Memo) -> RulePromise { RulePromise::High }
}

// ── Join2NestLoop ─────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct Join2NestLoop;

impl Rule for Join2NestLoop {
    fn name(&self) -> &str { "Join2NestLoop" }
    fn is_transformation(&self) -> bool { false }

    fn matches(&self, expr: &MemoExpr, _memo: &Memo) -> bool {
        matches!(&expr.op, Operator::Logical(LogicalOp::Join { .. }))
            && expr.children.len() == 2
    }

    fn apply(
        &self,
        expr_id: ExprId,
        group_id: GroupId,
        memo: &mut Memo,
        _catalog: &CatalogSnapshot,
    ) -> Vec<ExprId> {
        let join_type = match &memo.get_expr(expr_id).op {
            Operator::Logical(LogicalOp::Join { join_type, .. }) => *join_type,
            _ => return vec![],
        };
        let children = memo.get_expr(expr_id).children.clone();
        let phys = Operator::Physical(PhysicalOp::NestLoop { join_type });
        let (_, eid) = memo.insert_expr(phys, children, Some(group_id));
        vec![eid]
    }

    fn promise(&self, _expr: &MemoExpr, _memo: &Memo) -> RulePromise { RulePromise::Low }
}

// ── Join2MergeJoin ────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct Join2MergeJoin;

impl Rule for Join2MergeJoin {
    fn name(&self) -> &str { "Join2MergeJoin" }
    fn is_transformation(&self) -> bool { false }

    fn matches(&self, expr: &MemoExpr, _memo: &Memo) -> bool {
        matches!(&expr.op, Operator::Logical(LogicalOp::Join { .. }))
            && expr.children.len() == 2
    }

    fn apply(
        &self,
        expr_id: ExprId,
        group_id: GroupId,
        memo: &mut Memo,
        _catalog: &CatalogSnapshot,
    ) -> Vec<ExprId> {
        let (join_type, predicate) = match get_join_info(memo.get_expr(expr_id)) {
            Some(j) => j,
            None => return vec![],
        };
        let children = memo.get_expr(expr_id).children.clone();

        let merge_clauses = extract_merge_clauses(&predicate);
        if merge_clauses.is_empty() {
            return vec![]; // MergeJoin requires at least one merge clause
        }

        let phys = Operator::Physical(PhysicalOp::MergeJoin { join_type, merge_clauses });
        let (_, eid) = memo.insert_expr(phys, children, Some(group_id));
        vec![eid]
    }

    fn promise(&self, _expr: &MemoExpr, _memo: &Memo) -> RulePromise { RulePromise::Medium }
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn extract_equality_clauses(pred: &crate::ir::scalar::ScalarExpr)
    -> Vec<(crate::ir::scalar::ScalarExpr, crate::ir::scalar::ScalarExpr)>
{
    use crate::ir::scalar::{ScalarExpr, BoolExprType};
    match pred {
        ScalarExpr::OpExpr { op_oid, args, .. } => {
            // Assume any binary OpExpr between two ColumnRefs is a hash clause
            if args.len() == 2 {
                if matches!(&args[0], ScalarExpr::ColumnRef(_))
                    && matches!(&args[1], ScalarExpr::ColumnRef(_))
                {
                    return vec![(args[0].clone(), args[1].clone())];
                }
            }
            vec![]
        }
        ScalarExpr::BoolExpr { bool_type: BoolExprType::And, args } => {
            args.iter().flat_map(|a| extract_equality_clauses(a)).collect()
        }
        _ => vec![],
    }
}

fn extract_merge_clauses(pred: &crate::ir::scalar::ScalarExpr)
    -> Vec<crate::ir::physical::MergeClauseInfo>
{
    use crate::ir::scalar::{ScalarExpr, BoolExprType};
    use crate::ir::physical::MergeClauseInfo;
    match pred {
        ScalarExpr::OpExpr { op_oid, args, .. } => {
            if args.len() == 2
                && matches!(&args[0], ScalarExpr::ColumnRef(_))
                && matches!(&args[1], ScalarExpr::ColumnRef(_))
            {
                return vec![MergeClauseInfo {
                    left_key: args[0].clone(),
                    right_key: args[1].clone(),
                    merge_op: *op_oid,
                    collation: 0,
                    nulls_first: false,
                }];
            }
            vec![]
        }
        ScalarExpr::BoolExpr { bool_type: BoolExprType::And, args } => {
            args.iter().flat_map(|a| extract_merge_clauses(a)).collect()
        }
        _ => vec![],
    }
}
