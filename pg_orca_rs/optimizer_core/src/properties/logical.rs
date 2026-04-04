use crate::ir::types::{ColumnId, TableId};

#[derive(Debug, Clone)]
pub struct LogicalProperties {
    pub output_columns: Vec<ColumnId>,
    pub row_count: f64,
    pub table_ids: Vec<TableId>,
    pub not_null_columns: Vec<ColumnId>,
    pub unique_keys: Vec<Vec<ColumnId>>,
    pub avg_width: f64,
}
