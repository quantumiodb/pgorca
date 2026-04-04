use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::ir::operator::Operator;
use crate::ir::logical::LogicalOp;
use crate::ir::physical::PhysicalOp;

use super::group::GroupId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExprId(pub u32);

pub struct MemoExpr {
    pub id: ExprId,
    pub op: Operator,
    pub children: Vec<GroupId>,
    pub explored: bool,
    pub implemented: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Fingerprint(pub u64);

/// Compute a fingerprint for deduplication.
/// Based on the operator discriminant + key fields + child group IDs.
pub fn compute_fingerprint(op: &Operator, children: &[GroupId]) -> Fingerprint {
    let mut hasher = DefaultHasher::new();

    match op {
        Operator::Logical(logical) => {
            "L".hash(&mut hasher);
            match logical {
                LogicalOp::Get { table_id, rte_index, .. } => {
                    "Get".hash(&mut hasher);
                    table_id.0.hash(&mut hasher);
                    rte_index.hash(&mut hasher);
                }
                LogicalOp::Select { .. } => "Select".hash(&mut hasher),
                LogicalOp::Project { .. } => "Project".hash(&mut hasher),
                LogicalOp::Join { join_type, .. } => {
                    "Join".hash(&mut hasher);
                    std::mem::discriminant(join_type).hash(&mut hasher);
                }
                LogicalOp::Aggregate { .. } => "Aggregate".hash(&mut hasher),
                LogicalOp::Sort { .. } => "Sort".hash(&mut hasher),
                LogicalOp::Limit { .. } => "Limit".hash(&mut hasher),
                LogicalOp::Distinct { .. } => "Distinct".hash(&mut hasher),
                LogicalOp::Append => "Append".hash(&mut hasher),
            }
        }
        Operator::Physical(physical) => {
            "P".hash(&mut hasher);
            match physical {
                PhysicalOp::SeqScan { scanrelid } => {
                    "SeqScan".hash(&mut hasher);
                    scanrelid.hash(&mut hasher);
                }
                PhysicalOp::IndexScan { scanrelid, index_oid, .. } => {
                    "IndexScan".hash(&mut hasher);
                    scanrelid.hash(&mut hasher);
                    index_oid.hash(&mut hasher);
                }
                PhysicalOp::HashJoin { join_type, .. } => {
                    "HashJoin".hash(&mut hasher);
                    std::mem::discriminant(join_type).hash(&mut hasher);
                }
                PhysicalOp::NestLoop { join_type } => {
                    "NestLoop".hash(&mut hasher);
                    std::mem::discriminant(join_type).hash(&mut hasher);
                }
                PhysicalOp::MergeJoin { join_type, .. } => {
                    "MergeJoin".hash(&mut hasher);
                    std::mem::discriminant(join_type).hash(&mut hasher);
                }
                PhysicalOp::Sort { .. } => "Sort".hash(&mut hasher),
                PhysicalOp::Agg { strategy, .. } => {
                    "Agg".hash(&mut hasher);
                    std::mem::discriminant(strategy).hash(&mut hasher);
                }
                PhysicalOp::Limit { .. } => "Limit".hash(&mut hasher),
                PhysicalOp::Result { .. } => "Result".hash(&mut hasher),
                _ => {
                    // Fallback for less common operators
                    format!("{:?}", std::mem::discriminant(physical)).hash(&mut hasher);
                }
            }
        }
    }

    for child in children {
        child.0.hash(&mut hasher);
    }

    Fingerprint(hasher.finish())
}
