use std::collections::HashMap;
use crate::ir::types::{TableId, IndexAmType};
use crate::ir::scalar::ScalarExpr;

#[derive(Debug, Clone)]
pub struct CatalogSnapshot {
    pub tables: HashMap<TableId, TableStats>,
    pub cost_params: CostParams,
}

#[derive(Debug, Clone)]
pub struct CostParams {
    pub seq_page_cost: f64,
    pub random_page_cost: f64,
    pub cpu_tuple_cost: f64,
    pub cpu_index_tuple_cost: f64,
    pub cpu_operator_cost: f64,
    pub effective_cache_size: f64,
    pub work_mem: usize,
}

impl Default for CostParams {
    fn default() -> Self {
        Self {
            seq_page_cost: 1.0,
            random_page_cost: 4.0,
            cpu_tuple_cost: 0.01,
            cpu_index_tuple_cost: 0.005,
            cpu_operator_cost: 0.0025,
            effective_cache_size: 524288.0,  // 4GB in pages
            work_mem: 4 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableStats {
    pub oid: u32,
    pub name: String,
    pub row_count: f64,
    pub page_count: u64,
    pub columns: Vec<ColumnStats>,
    pub indexes: Vec<IndexStats>,
}

#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub attnum: i16,
    pub name: String,
    pub ndistinct: f64,
    pub null_fraction: f64,
    pub avg_width: i32,
    pub correlation: f64,
}

impl Default for ColumnStats {
    fn default() -> Self {
        Self {
            attnum: 0,
            name: String::new(),
            ndistinct: -1.0,
            null_fraction: 0.0,
            avg_width: 4,
            correlation: 0.0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndexStats {
    pub oid: u32,
    pub name: String,
    pub columns: Vec<i16>,
    pub unique: bool,
    pub am_type: IndexAmType,
    pub pages: u64,
    pub tree_height: u32,
    pub predicate: Option<ScalarExpr>,
    pub include_columns: Vec<i16>,
}
