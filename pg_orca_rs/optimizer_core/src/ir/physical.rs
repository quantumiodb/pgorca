use crate::properties::required::RequiredProperties;
use crate::properties::delivered::DeliveredProperties;
use crate::properties::parallel::Parallelism;
use super::types::*;
use super::scalar::ScalarExpr;

pub trait PhysicalPropertyProvider {
    fn derive_child_required(
        &self,
        child_idx: usize,
        req_props: &RequiredProperties,
    ) -> RequiredProperties;

    fn derive_delivered(
        &self,
        children_delivered: &[DeliveredProperties],
    ) -> DeliveredProperties;
}

impl PhysicalPropertyProvider for PhysicalOp {
    fn derive_child_required(
        &self,
        _child_idx: usize,
        req_props: &RequiredProperties,
    ) -> RequiredProperties {
        match self {
            PhysicalOp::MergeJoin { merge_clauses, .. } => {
                // A robust implementation would map scalar exprs to SortKeys properly
                for _clause in merge_clauses {
                    // Extract column ids from the merge clause's left and right keys
                    // For now, we return empty requirement (fallback to basic property framework logic)
                }
                RequiredProperties::serial()
            }
            PhysicalOp::Agg { strategy: AggStrategy::Sorted, .. } => {
                // Group by columns should be requested as sorting keys.
                RequiredProperties::serial()
            }
            // Gather/GatherMerge: children may produce Partial output — no serial req.
            PhysicalOp::Gather { .. } | PhysicalOp::GatherMerge { .. } => {
                RequiredProperties::none()
            }
            // Sort: child output may be parallel (Sort propagates parallelism in derive_delivered).
            // But we don't want a parallel sort here — require serial for simplicity.
            PhysicalOp::Sort { .. } => RequiredProperties::serial(),
            PhysicalOp::Limit { .. } | PhysicalOp::Unique { .. } => req_props.clone(),
            // All other operators (HashJoin, NestLoop, etc.) require serial children.
            _ => RequiredProperties::serial(),
        }
    }

    fn derive_delivered(
        &self,
        children_delivered: &[DeliveredProperties],
    ) -> DeliveredProperties {
        match self {
            PhysicalOp::Sort { keys } => DeliveredProperties {
                ordering: keys.clone(),
                parallelism: children_delivered.first()
                    .map(|c| c.parallelism.clone())
                    .unwrap_or(Parallelism::Serial),
            },
            PhysicalOp::IndexScan { index_order_keys, .. } => DeliveredProperties {
                ordering: index_order_keys.clone(),
                parallelism: Parallelism::Serial,
            },
            PhysicalOp::Limit { .. } | PhysicalOp::Unique { .. } => {
                if children_delivered.is_empty() {
                    DeliveredProperties::none()
                } else {
                    children_delivered[0].clone()
                }
            }
            PhysicalOp::ParallelSeqScan { num_workers, .. } => DeliveredProperties {
                ordering: Vec::new(),
                parallelism: Parallelism::Partial { num_workers: *num_workers },
            },
            PhysicalOp::Gather { .. } => DeliveredProperties {
                ordering: Vec::new(),
                parallelism: Parallelism::Serial,
            },
            PhysicalOp::GatherMerge { sort_keys, .. } => DeliveredProperties {
                ordering: sort_keys.clone(),
                parallelism: Parallelism::Serial,
            },
            _ => DeliveredProperties::none(),
        }
    }
}


#[derive(Debug, Clone)]
pub enum PhysicalOp {
    // ── Scan (leaf) ─────────────────────────
    SeqScan {
        scanrelid: RteIndex,
    },
    IndexScan {
        scanrelid: RteIndex,
        index_oid: u32,
        scan_direction: ScanDirection,
        index_quals: Vec<ScalarExpr>,
        index_order_keys: Vec<SortKey>,
    },
    IndexOnlyScan {
        scanrelid: RteIndex,
        index_oid: u32,
        index_quals: Vec<ScalarExpr>,
    },
    BitmapHeapScan {
        scanrelid: RteIndex,
        index_oid: u32,
        index_quals: Vec<ScalarExpr>,
    },

    // ── Join (binary) ───────────────────────
    NestLoop {
        join_type: JoinType,
    },
    HashJoin {
        join_type: JoinType,
        hash_clauses: Vec<(ScalarExpr, ScalarExpr)>,
    },
    MergeJoin {
        join_type: JoinType,
        merge_clauses: Vec<MergeClauseInfo>,
    },

    // ── Sort ────────────────────────────────
    Sort {
        keys: Vec<SortKey>,
    },
    IncrementalSort {
        keys: Vec<SortKey>,
        presorted_cols: usize,
    },

    // ── Aggregation ─────────────────────────
    Agg {
        strategy: AggStrategy,
        group_by: Vec<ColumnId>,
        aggregates: Vec<AggExpr>,
    },

    // ── Set operations ──────────────────────
    Append,
    Unique { num_cols: usize },

    // ── Control ─────────────────────────────
    Result {
        resconstantqual: Option<ScalarExpr>,
    },
    Limit {
        offset: Option<ScalarExpr>,
        count: Option<ScalarExpr>,
    },

    // ── Window ──────────────────────────────
    WindowAgg {
        clauses: Vec<WindowClause>,
    },

    // ── Auxiliary ────────────────────────────
    Material,

    // ── Parallel ─────────────────────────────
    /// Parallel sequential scan — produces a partial result stream.
    /// Maps to PG SeqScan with parallel_aware = true.
    ParallelSeqScan {
        scanrelid: RteIndex,
        num_workers: usize,
    },
    /// Gathers parallel worker results into a single serial stream.
    Gather {
        num_workers: usize,
    },
    /// Gathers and merges sorted parallel worker output preserving order.
    GatherMerge {
        num_workers: usize,
        sort_keys: Vec<SortKey>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanDirection { Forward, Backward, NoMovement }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggStrategy { Plain, Sorted, Hashed, Mixed }

#[derive(Debug, Clone)]
pub struct MergeClauseInfo {
    pub left_key: ScalarExpr,
    pub right_key: ScalarExpr,
    pub merge_op: u32,
    pub collation: u32,
    pub nulls_first: bool,
}
