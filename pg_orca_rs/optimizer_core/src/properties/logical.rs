use crate::ir::types::{ColumnId, TableId};
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub struct LogicalProperties {
    pub output_columns: Vec<ColumnId>,
    pub row_count: f64,
    pub table_ids: Vec<TableId>,
    pub not_null_columns: Vec<ColumnId>,
    pub unique_keys: Vec<Vec<ColumnId>>,
    pub fd_keys: Vec<Vec<ColumnId>>,
    pub equivalence_classes: Vec<HashSet<ColumnId>>,
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
            fd_keys: vec![],
            equivalence_classes: vec![],
            avg_width: 32.0,
        }
    }
}
