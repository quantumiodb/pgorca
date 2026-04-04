use crate::cost::stats::CatalogSnapshot;
use crate::ir::logical::LogicalOp;
use crate::ir::operator::Operator;
use crate::ir::physical::PhysicalOp;
use crate::memo::{ExprId, GroupId, Memo, MemoExpr};
use crate::rules::{Rule, RulePromise};

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
        let expr = memo.get_expr(expr_id);
        let rte_index = match &expr.op {
            Operator::Logical(LogicalOp::Get { rte_index, .. }) => *rte_index,
            _ => return vec![],
        };

        let phys_op = Operator::Physical(PhysicalOp::SeqScan { scanrelid: rte_index });
        let (_, new_eid) = memo.insert_expr(phys_op, vec![], Some(group_id));
        vec![new_eid]
    }

    fn promise(&self, _expr: &MemoExpr, _memo: &Memo) -> RulePromise {
        RulePromise::High
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use crate::cost::stats::{CostParams, CatalogSnapshot};
    use crate::ir::types::{TableId, ColumnId};

    #[test]
    fn test_get2seqscan() {
        let mut memo = Memo::new();
        let catalog = CatalogSnapshot {
            tables: HashMap::new(),
            cost_params: CostParams::default(),
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
        assert!(matches!(&new_expr.op, Operator::Physical(PhysicalOp::SeqScan { scanrelid: 1 })));
        assert_eq!(memo.get_group(gid).exprs.len(), 2);
    }
}
