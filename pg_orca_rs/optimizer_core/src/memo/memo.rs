use std::collections::HashMap;

use crate::ir::operator::Operator;
use crate::utility::union_find::UnionFind;

use super::group::{GroupId, Group};
use super::expr::{ExprId, MemoExpr, Fingerprint, compute_fingerprint};

pub struct Memo {
    groups: Vec<Group>,
    expressions: Vec<MemoExpr>,
    fingerprints: HashMap<Fingerprint, (GroupId, ExprId)>,
    pub root_group: Option<GroupId>,
    union_find: UnionFind,
}

impl Memo {
    pub fn new() -> Self {
        Self {
            groups: Vec::new(),
            expressions: Vec::new(),
            fingerprints: HashMap::new(),
            root_group: None,
            union_find: UnionFind::new(),
        }
    }

    /// Insert an expression. Returns (GroupId, ExprId). Deduplicates via fingerprint.
    pub fn insert_expr(
        &mut self,
        op: Operator,
        children: Vec<GroupId>,
        target_group: Option<GroupId>,
    ) -> (GroupId, ExprId) {
        let norm_children: Vec<GroupId> = children.iter()
            .map(|c| GroupId(self.union_find.find(c.0)))
            .collect();

        let fp = compute_fingerprint(&op, &norm_children);
        if let Some(&(gid, eid)) = self.fingerprints.get(&fp) {
            return (GroupId(self.union_find.find(gid.0)), eid);
        }

        let expr_id = ExprId(self.expressions.len() as u32);
        let group_id = match target_group {
            Some(gid) => GroupId(self.union_find.find(gid.0)),
            None => {
                let gid = GroupId(self.groups.len() as u32);
                self.union_find.make_set();
                self.groups.push(Group::new(gid));
                gid
            }
        };

        self.expressions.push(MemoExpr {
            id: expr_id,
            op,
            children: norm_children,
            explored: false,
            implemented: false,
        });

        self.groups[group_id.0 as usize].exprs.push(expr_id);
        self.fingerprints.insert(fp, (group_id, expr_id));

        (group_id, expr_id)
    }

    pub fn find_group(&mut self, id: GroupId) -> GroupId {
        GroupId(self.union_find.find(id.0))
    }

    pub fn get_group(&self, id: GroupId) -> &Group {
        &self.groups[id.0 as usize]
    }

    pub fn get_group_mut(&mut self, id: GroupId) -> &mut Group {
        &mut self.groups[id.0 as usize]
    }

    pub fn get_expr(&self, id: ExprId) -> &MemoExpr {
        &self.expressions[id.0 as usize]
    }

    pub fn get_expr_mut(&mut self, id: ExprId) -> &mut MemoExpr {
        &mut self.expressions[id.0 as usize]
    }

    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    pub fn set_root(&mut self, gid: GroupId) {
        self.root_group = Some(gid);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::logical::LogicalOp;
    use crate::ir::types::{TableId, ColumnId};

    #[test]
    fn test_insert_and_dedup() {
        let mut memo = Memo::new();
        let op1 = Operator::Logical(LogicalOp::Get {
            table_id: TableId(1), columns: vec![ColumnId(1), ColumnId(2)], rte_index: 1,
        });
        let op2 = Operator::Logical(LogicalOp::Get {
            table_id: TableId(1), columns: vec![ColumnId(1), ColumnId(2)], rte_index: 1,
        });

        let (g1, e1) = memo.insert_expr(op1, vec![], None);
        let (g2, e2) = memo.insert_expr(op2, vec![], None);
        assert_eq!(g1, g2);
        assert_eq!(e1, e2);
        assert_eq!(memo.group_count(), 1);
    }

    #[test]
    fn test_separate_groups() {
        let mut memo = Memo::new();
        let op1 = Operator::Logical(LogicalOp::Get {
            table_id: TableId(1), columns: vec![], rte_index: 1,
        });
        let op2 = Operator::Logical(LogicalOp::Get {
            table_id: TableId(2), columns: vec![], rte_index: 2,
        });
        let (g1, _) = memo.insert_expr(op1, vec![], None);
        let (g2, _) = memo.insert_expr(op2, vec![], None);
        assert_ne!(g1, g2);
        assert_eq!(memo.group_count(), 2);
    }

    #[test]
    fn test_add_to_existing_group() {
        let mut memo = Memo::new();
        let op1 = Operator::Logical(LogicalOp::Get {
            table_id: TableId(1), columns: vec![], rte_index: 1,
        });
        let (g1, _) = memo.insert_expr(op1, vec![], None);

        let op2 = Operator::Physical(crate::ir::physical::PhysicalOp::SeqScan { scanrelid: 1 });
        let (g2, _) = memo.insert_expr(op2, vec![], Some(g1));
        assert_eq!(g1, g2);
        assert_eq!(memo.get_group(g1).exprs.len(), 2);
    }
}
