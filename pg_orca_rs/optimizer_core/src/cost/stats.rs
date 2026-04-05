use std::collections::HashMap;
use crate::ir::types::{ColumnId, TableId, IndexAmType, RteIndex};
use crate::ir::scalar::{ScalarExpr, BoolExprType, NullTestType, ConstValue};

// ── Catalog Snapshot ───────────────────────────────────

#[derive(Debug, Clone)]
pub struct CatalogSnapshot {
    pub tables: HashMap<TableId, TableStats>,
    /// Maps RTE index (1-based) → TableId
    pub rte_to_table: HashMap<RteIndex, TableId>,
    pub cost_model: CostModel,
}

impl CatalogSnapshot {
    pub fn get_table_by_rte(&self, rte: RteIndex) -> Option<&TableStats> {
        self.rte_to_table.get(&rte).and_then(|tid| self.tables.get(tid))
    }
}

impl Default for CatalogSnapshot {
    fn default() -> Self {
        Self {
            tables: HashMap::new(),
            rte_to_table: HashMap::new(),
            cost_model: CostModel::default(),
        }
    }
}

// ── Cost Model (replaces CostParams) ──────────────────

/// Cost model parameters.
/// PG standard parameters come from GUCs; damping factors from custom GUCs.
#[derive(Debug, Clone)]
pub struct CostModel {
    // PG standard cost parameters
    pub seq_page_cost: f64,
    pub random_page_cost: f64,
    pub cpu_tuple_cost: f64,
    pub cpu_index_tuple_cost: f64,
    pub cpu_operator_cost: f64,
    pub effective_cache_size: f64,
    pub work_mem: usize,

    // GPORCA-style damping factors (mitigate independence assumption bias)
    pub damping_factor_filter: f64,
    pub damping_factor_join: f64,
    pub damping_factor_groupby: f64,
}

impl Default for CostModel {
    fn default() -> Self {
        Self {
            seq_page_cost: 1.0,
            random_page_cost: 4.0,
            cpu_tuple_cost: 0.01,
            cpu_index_tuple_cost: 0.005,
            cpu_operator_cost: 0.0025,
            effective_cache_size: 524288.0, // 4GB in pages
            work_mem: 4 * 1024 * 1024,
            damping_factor_filter: 0.75,
            damping_factor_join: 0.75,
            damping_factor_groupby: 0.75,
        }
    }
}

// Backward-compatible alias
pub type CostParams = CostModel;

// ── Table & Column Statistics ──────────────────────────

#[derive(Debug, Clone)]
pub struct TableStats {
    pub oid: u32,
    pub name: String,
    pub row_count: f64,
    pub page_count: u64,
    pub columns: Vec<ColumnStats>,
    pub indexes: Vec<IndexStats>,
    /// Maps ColumnId → pg attnum (for predicate-to-index matching)
    pub col_id_to_attnum: HashMap<ColumnId, i16>,
}

/// Histogram boundary value (from pg_statistic stakind=HISTOGRAM).
#[derive(Debug, Clone)]
pub struct HistogramBound {
    pub value: ConstValue,
}

/// Most Common Value entry (from pg_statistic stakind=MCV).
#[derive(Debug, Clone)]
pub struct McvEntry {
    pub value: ConstValue,
    pub frequency: f32,
}

#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub attnum: i16,
    pub name: String,
    pub ndistinct: f64,
    pub null_fraction: f64,
    pub avg_width: i32,
    pub correlation: f64,

    /// Histogram bounds (from stavalues[stakind=HISTOGRAM])
    pub histogram_bounds: Option<Vec<HistogramBound>>,

    /// Most common values (from stavalues[stakind=MCV])
    pub most_common_vals: Option<Vec<McvEntry>>,
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
            histogram_bounds: None,
            most_common_vals: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndexStats {
    pub oid: u32,
    pub name: String,
    /// pg attnum for each index key column (leading columns first)
    pub columns: Vec<i16>,
    pub unique: bool,
    pub am_type: IndexAmType,
    pub pages: u64,
    pub tree_height: u32,
    pub predicate: Option<ScalarExpr>,
    pub include_columns: Vec<i16>,
}

// ── Legacy Selectivity Estimator (kept for compatibility) ──

/// Estimate predicate selectivity (fraction of rows that satisfy the predicate).
pub fn estimate_selectivity(
    predicate: &ScalarExpr,
    catalog: &CatalogSnapshot,
    table_ids: &[TableId],
) -> f64 {
    match predicate {
        ScalarExpr::OpExpr { args, .. } => {
            // For a comparison op, use column statistics if available.
            // We look for a ColumnRef in the args.
            let col_id = args.iter().find_map(|a| {
                if let ScalarExpr::ColumnRef(c) = a { Some(*c) } else { None }
            });
            if let Some(cid) = col_id {
                // Look up column stats across all tables
                for &tid in table_ids {
                    if let Some(ts) = catalog.tables.get(&tid) {
                        let attnum = match ts.col_id_to_attnum.get(&cid) {
                            Some(&a) => a,
                            None => continue,
                        };
                        let col_stats = ts.columns.iter().find(|c| c.attnum == attnum);
                        if let Some(cs) = col_stats {
                            if cs.ndistinct > 0.0 {
                                return (1.0 / cs.ndistinct).min(0.5);
                            } else if cs.ndistinct < 0.0 {
                                // negative = fraction of distinct values
                                return (-1.0 / cs.ndistinct).min(0.5);
                            }
                        }
                    }
                }
            }
            // Default: 1/3 for range predicates
            0.3333
        }
        ScalarExpr::BoolExpr { bool_type, args } => match bool_type {
            BoolExprType::And => args
                .iter()
                .map(|a| estimate_selectivity(a, catalog, table_ids))
                .product::<f64>(),
            BoolExprType::Or => {
                let sels: Vec<f64> = args
                    .iter()
                    .map(|a| estimate_selectivity(a, catalog, table_ids))
                    .collect();
                1.0 - sels.iter().map(|s| 1.0 - s).product::<f64>()
            }
            BoolExprType::Not => {
                1.0 - estimate_selectivity(&args[0], catalog, table_ids)
            }
        },
        ScalarExpr::NullTest { null_test_type, .. } => match null_test_type {
            NullTestType::IsNull => 0.01,
            NullTestType::IsNotNull => 0.99,
        },
        _ => 0.1,
    }
}

/// Check whether a predicate can use the leading column of a B-tree index.
pub fn predicate_matches_btree_index(
    predicate: &ScalarExpr,
    index: &IndexStats,
    table_stats: &TableStats,
) -> bool {
    if index.am_type != IndexAmType::BTree {
        return false;
    }
    let leading_attnum = match index.columns.first() {
        Some(&a) => a,
        None => return false,
    };
    predicate_references_attnum(predicate, leading_attnum, table_stats)
}

fn predicate_references_attnum(
    pred: &ScalarExpr,
    attnum: i16,
    table_stats: &TableStats,
) -> bool {
    match pred {
        ScalarExpr::OpExpr { args, .. } => args.iter().any(|a| {
            if let ScalarExpr::ColumnRef(col_id) = a {
                table_stats.col_id_to_attnum.get(col_id) == Some(&attnum)
            } else {
                false
            }
        }),
        ScalarExpr::BoolExpr { args, .. } => {
            args.iter().any(|a| predicate_references_attnum(a, attnum, table_stats))
        }
        _ => false,
    }
}
