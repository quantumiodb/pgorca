use super::types::*;
use super::scalar::ScalarExpr;

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
