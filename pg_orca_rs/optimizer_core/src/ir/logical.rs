use super::types::*;
use super::scalar::ScalarExpr;

#[derive(Debug, Clone)]
pub enum LogicalOp {
    Get {
        table_id: TableId,
        columns: Vec<ColumnId>,
        rte_index: RteIndex,
    },

    Select {
        predicate: ScalarExpr,
    },

    Project {
        projections: Vec<(ScalarExpr, ColumnId)>,
    },

    Join {
        join_type: JoinType,
        predicate: ScalarExpr,
    },

    Aggregate {
        group_by: Vec<ColumnId>,
        aggregates: Vec<AggExpr>,
    },

    Sort {
        keys: Vec<SortKey>,
    },

    Limit {
        offset: Option<ScalarExpr>,
        count: Option<ScalarExpr>,
    },

    Distinct {
        columns: Vec<ColumnId>,
    },

    Window {
        clauses: Vec<WindowClause>,
    },

    /// Union of multiple child relations (e.g. partitions of a partitioned table).
    /// Each child is a separate Get group in the memo.
    Append,
}

/// Standalone logical expression tree (before Memo insertion)
#[derive(Debug, Clone)]
pub struct LogicalExpr {
    pub op: LogicalOp,
    pub children: Vec<LogicalExpr>,
}
