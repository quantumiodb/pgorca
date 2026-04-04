use crate::cost::stats::CatalogSnapshot;
use crate::ir::logical::LogicalOp;
use crate::ir::operator::Operator;
use crate::ir::physical::PhysicalOp;
use crate::ir::physical::AggStrategy;
use crate::memo::{ExprId, GroupId, Memo, MemoExpr};
use crate::rules::{Rule, RulePromise};

// ── Agg2HashAgg ───────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct Agg2HashAgg;

impl Rule for Agg2HashAgg {
    fn name(&self) -> &str { "Agg2HashAgg" }
    fn is_transformation(&self) -> bool { false }

    fn matches(&self, expr: &MemoExpr, _memo: &Memo) -> bool {
        // Only for GROUP BY; plain agg (no group_by) uses Agg2PlainAgg
        match &expr.op {
            Operator::Logical(LogicalOp::Aggregate { group_by, .. }) => !group_by.is_empty(),
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
        let (group_by, aggregates, children) = match &memo.get_expr(expr_id).op {
            Operator::Logical(LogicalOp::Aggregate { group_by, aggregates }) => {
                (group_by.clone(), aggregates.clone(), memo.get_expr(expr_id).children.clone())
            }
            _ => return vec![],
        };
        let phys = Operator::Physical(PhysicalOp::Agg {
            strategy: AggStrategy::Hashed,
            group_by,
            aggregates,
        });
        let (_, eid) = memo.insert_expr(phys, children, Some(group_id));
        vec![eid]
    }

    fn promise(&self, _expr: &MemoExpr, _memo: &Memo) -> RulePromise { RulePromise::High }
}

// ── Agg2SortAgg ──────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct Agg2SortAgg;

impl Rule for Agg2SortAgg {
    fn name(&self) -> &str { "Agg2SortAgg" }
    fn is_transformation(&self) -> bool { false }

    fn matches(&self, expr: &MemoExpr, _memo: &Memo) -> bool {
        match &expr.op {
            Operator::Logical(LogicalOp::Aggregate { group_by, .. }) => !group_by.is_empty(),
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
        let (group_by, aggregates, children) = match &memo.get_expr(expr_id).op {
            Operator::Logical(LogicalOp::Aggregate { group_by, aggregates }) => {
                (group_by.clone(), aggregates.clone(), memo.get_expr(expr_id).children.clone())
            }
            _ => return vec![],
        };
        let phys = Operator::Physical(PhysicalOp::Agg {
            strategy: AggStrategy::Sorted,
            group_by,
            aggregates,
        });
        let (_, eid) = memo.insert_expr(phys, children, Some(group_id));
        vec![eid]
    }

    fn promise(&self, _expr: &MemoExpr, _memo: &Memo) -> RulePromise { RulePromise::Medium }
}

// ── Agg2PlainAgg ─────────────────────────────────────────────────────────────

/// For aggregate without GROUP BY (e.g. SELECT count(*) FROM t).
#[derive(Debug)]
pub struct Agg2PlainAgg;

impl Rule for Agg2PlainAgg {
    fn name(&self) -> &str { "Agg2PlainAgg" }
    fn is_transformation(&self) -> bool { false }

    fn matches(&self, expr: &MemoExpr, _memo: &Memo) -> bool {
        match &expr.op {
            Operator::Logical(LogicalOp::Aggregate { group_by, .. }) => group_by.is_empty(),
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
        let (group_by, aggregates, children) = match &memo.get_expr(expr_id).op {
            Operator::Logical(LogicalOp::Aggregate { group_by, aggregates }) => {
                (group_by.clone(), aggregates.clone(), memo.get_expr(expr_id).children.clone())
            }
            _ => return vec![],
        };
        let phys = Operator::Physical(PhysicalOp::Agg {
            strategy: AggStrategy::Plain,
            group_by,
            aggregates,
        });
        let (_, eid) = memo.insert_expr(phys, children, Some(group_id));
        vec![eid]
    }

    fn promise(&self, _expr: &MemoExpr, _memo: &Memo) -> RulePromise { RulePromise::High }
}
