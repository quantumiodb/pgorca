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

impl Default for LogicalProperties {
    fn default() -> Self {
        Self {
            output_columns: vec![],
            row_count: 1000.0,
            table_ids: vec![],
            not_null_columns: vec![],
            unique_keys: vec![],
            avg_width: 32.0,
        }
    }
}
