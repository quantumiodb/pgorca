//! Advanced cardinality estimation (M10).
//!
//! - Equivalence classes for join columns (A=B propagation)
//! - Contradiction detection (A>50 AND A<30 → sel=0)
//! - MCV (Most Common Values) and histogram utilization
//! - Improved selectivity for equality, range, IN-list predicates
//! - GPORCA-style damping for multi-predicate AND

use std::collections::HashMap;
use crate::ir::scalar::{ScalarExpr, BoolExprType, ConstValue};
use crate::ir::types::ColumnId;
use crate::utility::union_find::UnionFind;

// ── Equivalence Classes ─────────────────────────────────

/// Tracks column equivalence classes derived from equality join predicates.
/// E.g., `t1.a = t2.b AND t2.b = t3.c` → {t1.a, t2.b, t3.c} in one class.
pub struct EquivalenceClasses {
    uf: UnionFind,
    col_to_idx: HashMap<ColumnId, u32>,
}

impl EquivalenceClasses {
    pub fn new() -> Self {
        Self {
            uf: UnionFind::new(),
            col_to_idx: HashMap::new(),
        }
    }

    fn ensure_col(&mut self, col: ColumnId) -> u32 {
        if let Some(&idx) = self.col_to_idx.get(&col) {
            return idx;
        }
        let idx = self.uf.make_set();
        self.col_to_idx.insert(col, idx);
        idx
    }

    /// Record that col_a = col_b (from an equality predicate).
    pub fn add_equality(&mut self, col_a: ColumnId, col_b: ColumnId) {
        let a = self.ensure_col(col_a);
        let b = self.ensure_col(col_b);
        self.uf.union(a, b);
    }

    /// Check whether two columns are in the same equivalence class.
    pub fn are_equivalent(&mut self, col_a: ColumnId, col_b: ColumnId) -> bool {
        match (self.col_to_idx.get(&col_a).copied(), self.col_to_idx.get(&col_b).copied()) {
            (Some(a), Some(b)) => self.uf.find(a) == self.uf.find(b),
            _ => false,
        }
    }

    /// Extract equality pairs from a predicate and register them.
    pub fn extract_from_predicate(&mut self, pred: &ScalarExpr) {
        match pred {
            ScalarExpr::OpExpr { args, .. } if args.len() == 2 => {
                if let (ScalarExpr::ColumnRef(a), ScalarExpr::ColumnRef(b)) =
                    (&args[0], &args[1])
                {
                    self.add_equality(*a, *b);
                }
            }
            ScalarExpr::BoolExpr { bool_type: BoolExprType::And, args } => {
                for arg in args {
                    self.extract_from_predicate(arg);
                }
            }
            _ => {}
        }
    }
}

// ── Interval Constraints & Contradiction Detection ──────

/// A range constraint on a column: lower <= col <= upper.
#[derive(Debug, Clone)]
pub struct ColumnRange {
    pub lower: Option<f64>, // None = unbounded below
    pub upper: Option<f64>, // None = unbounded above
}

impl ColumnRange {
    pub fn unbounded() -> Self {
        Self { lower: None, upper: None }
    }

    pub fn intersect(&self, other: &ColumnRange) -> Option<ColumnRange> {
        let lower = match (self.lower, other.lower) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        let upper = match (self.upper, other.upper) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        // Contradiction: lower > upper
        if let (Some(lo), Some(hi)) = (lower, upper) {
            if lo > hi {
                return None; // empty range → contradiction
            }
        }
        Some(ColumnRange { lower, upper })
    }

    /// Estimate the fraction of a uniform range [min_val, max_val] that this constraint covers.
    pub fn selectivity(&self, min_val: f64, max_val: f64) -> f64 {
        if max_val <= min_val {
            return 1.0;
        }
        let lo = self.lower.unwrap_or(min_val).max(min_val);
        let hi = self.upper.unwrap_or(max_val).min(max_val);
        if lo > hi {
            return 0.0;
        }
        (hi - lo) / (max_val - min_val)
    }
}

/// Collect range constraints from a predicate tree and detect contradictions.
pub fn collect_column_ranges(pred: &ScalarExpr) -> HashMap<ColumnId, ColumnRange> {
    let mut ranges: HashMap<ColumnId, ColumnRange> = HashMap::new();
    collect_ranges_inner(pred, &mut ranges);
    ranges
}

fn collect_ranges_inner(pred: &ScalarExpr, ranges: &mut HashMap<ColumnId, ColumnRange>) {
    match pred {
        ScalarExpr::OpExpr { op_oid, args, .. } if args.len() == 2 => {
            // Try to extract col <op> const patterns
            let (col_id, const_val, col_is_left) = match (&args[0], &args[1]) {
                (ScalarExpr::ColumnRef(c), ScalarExpr::Const { value, .. }) => {
                    (Some(*c), const_to_f64(value), true)
                }
                (ScalarExpr::Const { value, .. }, ScalarExpr::ColumnRef(c)) => {
                    (Some(*c), const_to_f64(value), false)
                }
                _ => (None, None, true),
            };

            if let (Some(cid), Some(val)) = (col_id, const_val) {
                let new_range = op_to_range(*op_oid, val, col_is_left);
                if let Some(nr) = new_range {
                    let entry = ranges.entry(cid).or_insert_with(ColumnRange::unbounded);
                    if let Some(intersected) = entry.intersect(&nr) {
                        *entry = intersected;
                    } else {
                        // Contradiction: set to empty (lower > upper)
                        *entry = ColumnRange { lower: Some(1.0), upper: Some(0.0) };
                    }
                }
            }
        }
        ScalarExpr::BoolExpr { bool_type: BoolExprType::And, args } => {
            for arg in args {
                collect_ranges_inner(arg, ranges);
            }
        }
        _ => {}
    }
}

/// Map PG operator OID + constant value to a column range.
/// Common PG operator OIDs: 96(=int4), 97(<int4), 521(>int4), 523(>=int4), 522(<=int4)
fn op_to_range(op_oid: u32, val: f64, col_is_left: bool) -> Option<ColumnRange> {
    // Equality operators (any type)
    match op_oid {
        96 | 410 | 416 | 670 | 1054 => {
            // int4eq, int2eq, int8eq, oideq, texteq
            return Some(ColumnRange { lower: Some(val), upper: Some(val) });
        }
        _ => {}
    }

    // Range operators — direction depends on whether col is on left or right
    if col_is_left {
        match op_oid {
            97 | 37 | 412 => Some(ColumnRange { lower: None, upper: Some(val - 1.0) }),  // col < val
            521 | 413 | 76 => Some(ColumnRange { lower: Some(val + 1.0), upper: None }), // col > val
            523 | 415 | 78 => Some(ColumnRange { lower: Some(val), upper: None }),        // col >= val
            522 | 414 | 77 => Some(ColumnRange { lower: None, upper: Some(val) }),        // col <= val
            _ => None,
        }
    } else {
        // val <op> col → reverse direction
        match op_oid {
            97 | 37 | 412 => Some(ColumnRange { lower: Some(val + 1.0), upper: None }),  // val < col
            521 | 413 | 76 => Some(ColumnRange { lower: None, upper: Some(val - 1.0) }), // val > col
            523 | 415 | 78 => Some(ColumnRange { lower: None, upper: Some(val) }),        // val >= col
            522 | 414 | 77 => Some(ColumnRange { lower: Some(val), upper: None }),        // val <= col
            _ => None,
        }
    }
}

fn const_to_f64(value: &ConstValue) -> Option<f64> {
    match value {
        ConstValue::Int16(v) => Some(*v as f64),
        ConstValue::Int32(v) => Some(*v as f64),
        ConstValue::Int64(v) => Some(*v as f64),
        ConstValue::Float32(v) => Some(*v as f64),
        ConstValue::Float64(v) => Some(*v),
        ConstValue::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

// ── MCV & Histogram Helpers ────────────────────────────

use super::stats::{CatalogSnapshot, ColumnStats, McvEntry, HistogramBound};
use crate::ir::types::TableId;

/// Look up a ConstValue in the MCV list and return its frequency if found.
fn mcv_frequency(mcv: &[McvEntry], value: &ConstValue) -> Option<f32> {
    for entry in mcv {
        if const_values_equal(&entry.value, value) {
            return Some(entry.frequency);
        }
    }
    None
}

/// Sum of all MCV frequencies.
fn mcv_total_frequency(mcv: &[McvEntry]) -> f64 {
    mcv.iter().map(|e| e.frequency as f64).sum()
}

/// Estimate selectivity for `col = const` using MCV + ndistinct.
/// Algorithm per DESIGN.md §7.3:
///   1. If value is in MCV, return its frequency.
///   2. If MCV exists but value not in it: (1 - sum(mcv_freq)) / (ndistinct - mcv_count)
///   3. Fallback: 1 / ndistinct
fn equality_selectivity_with_mcv(
    col_stats: &ColumnStats,
    row_count: f64,
    const_value: &ConstValue,
) -> f64 {
    let ndistinct = resolve_ndistinct(col_stats.ndistinct, row_count);
    if ndistinct <= 0.0 {
        return 0.005; // no stats
    }

    if let Some(ref mcv) = col_stats.most_common_vals {
        if !mcv.is_empty() {
            // Check if value is in MCV
            if let Some(freq) = mcv_frequency(mcv, const_value) {
                return freq as f64;
            }
            // Value not in MCV: estimate from remaining distribution
            let mcv_sum = mcv_total_frequency(mcv);
            let remaining_distinct = (ndistinct - mcv.len() as f64).max(1.0);
            let remaining_fraction = (1.0 - mcv_sum).max(0.0);
            return remaining_fraction / remaining_distinct;
        }
    }

    // No MCV: simple 1/ndistinct
    1.0 / ndistinct
}

/// Estimate selectivity for range predicate `col < const` using histogram.
/// Uses linear interpolation across histogram bounds.
fn range_selectivity_with_histogram(
    histogram: &[HistogramBound],
    const_value: &ConstValue,
    is_less_than: bool,
) -> Option<f64> {
    if histogram.len() < 2 {
        return None;
    }

    let target = const_to_f64(const_value)?;
    let n_buckets = histogram.len() - 1;

    // Convert histogram bounds to f64 for interpolation
    let bounds: Vec<f64> = histogram.iter()
        .filter_map(|b| const_to_f64(&b.value))
        .collect();

    if bounds.len() < 2 {
        return None;
    }

    let min_val = bounds[0];
    let max_val = bounds[bounds.len() - 1];

    if is_less_than {
        // col < target
        if target <= min_val {
            return Some(0.0);
        }
        if target >= max_val {
            return Some(1.0);
        }

        // Find the bucket containing target and interpolate
        let mut sel = 0.0;
        for i in 0..bounds.len() - 1 {
            let lo = bounds[i];
            let hi = bounds[i + 1];
            if target <= lo {
                break;
            }
            if target >= hi {
                sel += 1.0 / n_buckets as f64;
            } else {
                // Linear interpolation within bucket
                let frac = (target - lo) / (hi - lo);
                sel += frac / n_buckets as f64;
                break;
            }
        }
        Some(sel.clamp(0.0, 1.0))
    } else {
        // col > target = 1 - sel(col <= target) ≈ 1 - sel(col < target)
        let less_sel = range_selectivity_with_histogram(histogram, const_value, true)?;
        Some((1.0 - less_sel).clamp(0.0, 1.0))
    }
}

/// Resolve ndistinct: positive = absolute count, negative = fraction of rows.
fn resolve_ndistinct(ndistinct: f64, row_count: f64) -> f64 {
    if ndistinct > 0.0 {
        ndistinct
    } else if ndistinct < 0.0 {
        (-ndistinct * row_count).max(1.0)
    } else {
        0.0
    }
}

/// Compare two ConstValues for equality (for MCV lookup).
fn const_values_equal(a: &ConstValue, b: &ConstValue) -> bool {
    match (a, b) {
        (ConstValue::Bool(x), ConstValue::Bool(y)) => x == y,
        (ConstValue::Int16(x), ConstValue::Int16(y)) => x == y,
        (ConstValue::Int32(x), ConstValue::Int32(y)) => x == y,
        (ConstValue::Int64(x), ConstValue::Int64(y)) => x == y,
        (ConstValue::Float32(x), ConstValue::Float32(y)) => x == y,
        (ConstValue::Float64(x), ConstValue::Float64(y)) => x == y,
        (ConstValue::Text(x), ConstValue::Text(y)) => x == y,
        (ConstValue::Bytea(x), ConstValue::Bytea(y)) => x == y,
        (ConstValue::Null, ConstValue::Null) => true,
        // Cross-type numeric comparison for MCV matching
        _ => {
            match (const_to_f64(a), const_to_f64(b)) {
                (Some(fa), Some(fb)) => (fa - fb).abs() < f64::EPSILON,
                _ => false,
            }
        }
    }
}

// ── Improved Selectivity Estimator ──────────────────────

/// Enhanced selectivity estimation using:
/// 1. MCV lookup for equality predicates
/// 2. Histogram interpolation for range predicates
/// 3. Range constraints with contradiction detection
/// 4. GPORCA-style damping for AND composition
/// 5. Equivalence-class-aware join selectivity
pub fn estimate_selectivity_v2(
    predicate: &ScalarExpr,
    catalog: &CatalogSnapshot,
    table_ids: &[TableId],
) -> f64 {
    // 1. Build constraint graph to detect cross-column contradictions
    let mut graph = ColumnConstraintGraph::new();
    graph.add_predicate(predicate);
    
    if graph.is_contradiction() {
        return 0.0;
    }

    // 2. Use recursive estimation (with MCV/histogram support)
    estimate_selectivity_recursive(predicate, catalog, table_ids)
}

fn estimate_selectivity_recursive(
    pred: &ScalarExpr,
    catalog: &CatalogSnapshot,
    table_ids: &[TableId],
) -> f64 {
    match pred {
        ScalarExpr::OpExpr { op_oid, args, .. } => {
            // Equality: use MCV-aware estimation
            if is_equality_op(*op_oid) {
                // col = const: try MCV-based estimation
                if let Some((col_stats, row_count, const_val)) =
                    find_col_const_pair(args, catalog, table_ids)
                {
                    return equality_selectivity_with_mcv(col_stats, row_count, const_val);
                }
                // col = col (join predicate)
                if args.len() == 2 {
                    if let (ScalarExpr::ColumnRef(_), ScalarExpr::ColumnRef(_)) =
                        (&args[0], &args[1])
                    {
                        let nd = args.iter().filter_map(|a| {
                            if let ScalarExpr::ColumnRef(c) = a {
                                get_ndistinct(*c, catalog, table_ids)
                            } else {
                                None
                            }
                        }).fold(1.0f64, f64::max);
                        return (1.0 / nd).max(0.001);
                    }
                }
                return 0.005; // default equality selectivity
            }
            // Range comparison: use histogram if available
            if is_range_op(*op_oid) {
                if let Some((col_stats, _row_count, const_val)) =
                    find_col_const_pair(args, catalog, table_ids)
                {
                    if let Some(ref hist) = col_stats.histogram_bounds {
                        let is_lt = is_less_than_op(*op_oid, args);
                        if let Some(sel) = range_selectivity_with_histogram(hist, const_val, is_lt) {
                            return sel.clamp(0.0001, 1.0);
                        }
                    }
                }
                // Fallback: 1/3 (PG default)
                return 0.3333;
            }
            0.25 // unknown operator
        }
        ScalarExpr::BoolExpr { bool_type, args } => match bool_type {
            BoolExprType::And => {
                // GPORCA-style damping: sort selectivities ascending, then
                // result = s[0] * s[1]^d * s[2]^(d^2) * ...
                // This reduces the impact of each additional predicate,
                // mitigating over-underestimation from the independence assumption.
                let damping = catalog.cost_model.damping_factor_filter;
                let mut sels: Vec<f64> = args.iter()
                    .map(|a| estimate_selectivity_recursive(a, catalog, table_ids))
                    .collect();
                sels.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let mut result = 1.0;
                let mut damp_power = damping; // starts at damping for 2nd predicate
                for (i, &sel) in sels.iter().enumerate() {
                    if i == 0 {
                        result *= sel; // first predicate: full selectivity
                    } else {
                        result *= sel.powf(damp_power); // damped exponent
                        damp_power *= damping; // increase damping for next
                    }
                }
                result.max(0.0001)
            }
            BoolExprType::Or => {
                let sels: Vec<f64> = args.iter()
                    .map(|a| estimate_selectivity_recursive(a, catalog, table_ids))
                    .collect();
                (1.0 - sels.iter().map(|s| 1.0 - s).product::<f64>()).min(1.0)
            }
            BoolExprType::Not => {
                1.0 - estimate_selectivity_recursive(&args[0], catalog, table_ids)
            }
        },
        ScalarExpr::NullTest { null_test_type, arg } => {
            // Use actual null_fraction if available
            if let ScalarExpr::ColumnRef(cid) = arg.as_ref() {
                if let Some(nf) = get_null_fraction(*cid, catalog, table_ids) {
                    return match null_test_type {
                        crate::ir::scalar::NullTestType::IsNull => nf,
                        crate::ir::scalar::NullTestType::IsNotNull => 1.0 - nf,
                    };
                }
            }
            match null_test_type {
                crate::ir::scalar::NullTestType::IsNull => 0.01,
                crate::ir::scalar::NullTestType::IsNotNull => 0.99,
            }
        }
        ScalarExpr::ScalarArrayOp { use_or, .. } => {
            if *use_or { 0.25 } else { 0.75 } // IN list / NOT IN
        }
        ScalarExpr::Const { value: ConstValue::Bool(b), .. } => {
            if *b { 1.0 } else { 0.0 }
        }
        _ => 0.1,
    }
}

// ── Column Constraint Graph ────────────────────────────

/// ColumnConstraintGraph tracks equivalence classes and range constraints
/// to detect contradictions and improve cardinality estimation.
pub struct ColumnConstraintGraph {
    pub equivalence_classes: EquivalenceClasses,
    pub range_constraints: HashMap<ColumnId, ColumnRange>,
}

impl ColumnConstraintGraph {
    pub fn new() -> Self {
        Self {
            equivalence_classes: EquivalenceClasses::new(),
            range_constraints: HashMap::new(),
        }
    }

    /// Extract all constraints (equalities and ranges) from a predicate tree.
    pub fn add_predicate(&mut self, pred: &ScalarExpr) {
        self.equivalence_classes.extract_from_predicate(pred);
        self.collect_ranges_recursive(pred);
        self.propagate_constraints();
    }

    fn collect_ranges_recursive(&mut self, pred: &ScalarExpr) {
        collect_ranges_inner(pred, &mut self.range_constraints);
    }

    /// Propagate range constraints across equivalence classes.
    /// E.g., if A = B and A > 10, then B must also be > 10.
    fn propagate_constraints(&mut self) {
        let mut group_ranges: HashMap<u32, ColumnRange> = HashMap::new();

        // 1. First pass: Aggregate ranges per equivalence class
        for (&col_id, range) in &self.range_constraints {
            let root = self.equivalence_classes.ensure_col_read_only(col_id);
            if let Some(r) = root {
                let entry = group_ranges.entry(r).or_insert_with(ColumnRange::unbounded);
                if let Some(intersected) = entry.intersect(range) {
                    *entry = intersected;
                } else {
                    // Contradiction: set to empty
                    *entry = ColumnRange { lower: Some(1.0), upper: Some(0.0) };
                }
            }
        }

        // 2. Second pass: Push aggregated ranges back to all columns in the class
        let col_ids: Vec<ColumnId> = self.equivalence_classes.col_to_idx.keys().cloned().collect();
        for col_id in col_ids {
            if let Some(r) = self.equivalence_classes.ensure_col_read_only(col_id) {
                if let Some(group_range) = group_ranges.get(&r) {
                    self.range_constraints.insert(col_id, group_range.clone());
                }
            }
        }
    }

    pub fn is_contradiction(&self) -> bool {
        for range in self.range_constraints.values() {
            if let (Some(lo), Some(hi)) = (range.lower, range.upper) {
                if lo > hi {
                    return true;
                }
            }
        }
        false
    }
}

impl EquivalenceClasses {
    fn ensure_col_read_only(&self, col: ColumnId) -> Option<u32> {
        self.col_to_idx.get(&col).map(|&idx| self.uf.find_non_mut(idx))
    }
}

// Update UnionFind to have a non-mutable find if possible, or just use find.
// Actually, find in UnionFind usually needs mut for path compression.
// Let's check UnionFind implementation.


fn is_equality_op(oid: u32) -> bool {
    matches!(oid, 96 | 410 | 416 | 670 | 1054 | 98 | 15) // int4eq, int2eq, int8eq, oideq, texteq, etc.
}

fn is_range_op(oid: u32) -> bool {
    matches!(oid, 97 | 521 | 522 | 523 | 37 | 76 | 77 | 78 | 412 | 413 | 414 | 415)
}

/// Determine if a range operator represents a "less than" direction for the column.
fn is_less_than_op(op_oid: u32, args: &[ScalarExpr]) -> bool {
    let col_is_left = matches!(&args[0], ScalarExpr::ColumnRef(_));
    match op_oid {
        // col < val or col <= val
        97 | 37 | 412 | 522 | 414 | 77 if col_is_left => true,
        // val > col or val >= col
        521 | 413 | 76 | 523 | 415 | 78 if !col_is_left => true,
        _ => false,
    }
}

/// Find a (ColumnStats, row_count, &ConstValue) pair from a binary op's args.
fn find_col_const_pair<'a>(
    args: &'a [ScalarExpr],
    catalog: &'a CatalogSnapshot,
    table_ids: &[TableId],
) -> Option<(&'a ColumnStats, f64, &'a ConstValue)> {
    let (col_id, const_val) = match (&args[0], &args[1]) {
        (ScalarExpr::ColumnRef(c), ScalarExpr::Const { value, .. }) => (Some(*c), Some(value)),
        (ScalarExpr::Const { value, .. }, ScalarExpr::ColumnRef(c)) => (Some(*c), Some(value)),
        _ => (None, None),
    };
    let cid = col_id?;
    let cv = const_val?;
    for &tid in table_ids {
        if let Some(ts) = catalog.tables.get(&tid) {
            if let Some(&attnum) = ts.col_id_to_attnum.get(&cid) {
                if let Some(cs) = ts.columns.iter().find(|c| c.attnum == attnum) {
                    return Some((cs, ts.row_count, cv));
                }
            }
        }
    }
    None
}

fn get_ndistinct(
    col_id: ColumnId,
    catalog: &CatalogSnapshot,
    table_ids: &[TableId],
) -> Option<f64> {
    for &tid in table_ids {
        if let Some(ts) = catalog.tables.get(&tid) {
            if let Some(&attnum) = ts.col_id_to_attnum.get(&col_id) {
                if let Some(cs) = ts.columns.iter().find(|c| c.attnum == attnum) {
                    if cs.ndistinct > 0.0 {
                        return Some(cs.ndistinct);
                    } else if cs.ndistinct < 0.0 {
                        // Negative: fraction of total rows
                        return Some((-cs.ndistinct * ts.row_count).max(1.0));
                    }
                }
            }
        }
    }
    None
}

fn get_null_fraction(
    col_id: ColumnId,
    catalog: &CatalogSnapshot,
    table_ids: &[TableId],
) -> Option<f64> {
    for &tid in table_ids {
        if let Some(ts) = catalog.tables.get(&tid) {
            if let Some(&attnum) = ts.col_id_to_attnum.get(&col_id) {
                if let Some(cs) = ts.columns.iter().find(|c| c.attnum == attnum) {
                    return Some(cs.null_fraction);
                }
            }
        }
    }
    None
}

// ── Tests ───────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cost::stats::*;
    use crate::ir::types::*;

    fn make_catalog_with_stats(col_stats: ColumnStats) -> (CatalogSnapshot, TableId) {
        let tid = TableId(1);
        let mut col_id_to_attnum = HashMap::new();
        col_id_to_attnum.insert(ColumnId(1), 1);
        let ts = TableStats {
            oid: 16384,
            name: "t".into(),
            row_count: 1000.0,
            page_count: 10,
            columns: vec![col_stats],
            indexes: vec![],
            col_id_to_attnum,
        };
        let mut tables = HashMap::new();
        tables.insert(tid, ts);
        let mut rte_to_table = HashMap::new();
        rte_to_table.insert(1u32, tid);
        let catalog = CatalogSnapshot {
            tables,
            rte_to_table,
            cost_model: CostModel::default(),
        };
        (catalog, tid)
    }

    #[test]
    fn test_equivalence_classes() {
        let mut ec = EquivalenceClasses::new();
        let a = ColumnId(1);
        let b = ColumnId(2);
        let c = ColumnId(3);

        ec.add_equality(a, b);
        ec.add_equality(b, c);

        assert!(ec.are_equivalent(a, c));
        assert!(!ec.are_equivalent(a, ColumnId(4)));
    }

    #[test]
    fn test_cross_column_contradiction() {
        // a = b AND a > 10 AND b < 5 → contradiction
        let pred = ScalarExpr::BoolExpr {
            bool_type: BoolExprType::And,
            args: vec![
                ScalarExpr::OpExpr {
                    op_oid: 96, // int4eq
                    return_type: 16,
                    args: vec![
                        ScalarExpr::ColumnRef(ColumnId(1)),
                        ScalarExpr::ColumnRef(ColumnId(2)),
                    ],
                },
                ScalarExpr::OpExpr {
                    op_oid: 521, // int4gt
                    return_type: 16,
                    args: vec![
                        ScalarExpr::ColumnRef(ColumnId(1)),
                        ScalarExpr::Const {
                            type_oid: 23, typmod: -1, collation: 0,
                            value: ConstValue::Int32(10), is_null: false,
                        },
                    ],
                },
                ScalarExpr::OpExpr {
                    op_oid: 97, // int4lt
                    return_type: 16,
                    args: vec![
                        ScalarExpr::ColumnRef(ColumnId(2)),
                        ScalarExpr::Const {
                            type_oid: 23, typmod: -1, collation: 0,
                            value: ConstValue::Int32(5), is_null: false,
                        },
                    ],
                },
            ],
        };
        let sel = estimate_selectivity_v2(&pred, &CatalogSnapshot::default(), &[]);
        assert_eq!(sel, 0.0, "expected contradiction selectivity 0.0");
    }

    #[test]
    fn test_range_propagation() {
        // a = b AND a > 100 AND b < 200 → both should have range (100, 200)
        let pred = ScalarExpr::BoolExpr {
            bool_type: BoolExprType::And,
            args: vec![
                ScalarExpr::OpExpr {
                    op_oid: 96,
                    return_type: 16,
                    args: vec![
                        ScalarExpr::ColumnRef(ColumnId(1)),
                        ScalarExpr::ColumnRef(ColumnId(2)),
                    ],
                },
                ScalarExpr::OpExpr {
                    op_oid: 521,
                    return_type: 16,
                    args: vec![
                        ScalarExpr::ColumnRef(ColumnId(1)),
                        ScalarExpr::Const {
                            type_oid: 23, typmod: -1, collation: 0,
                            value: ConstValue::Int32(100), is_null: false,
                        },
                    ],
                },
                ScalarExpr::OpExpr {
                    op_oid: 97,
                    return_type: 16,
                    args: vec![
                        ScalarExpr::ColumnRef(ColumnId(2)),
                        ScalarExpr::Const {
                            type_oid: 23, typmod: -1, collation: 0,
                            value: ConstValue::Int32(200), is_null: false,
                        },
                    ],
                },
            ],
        };
        
        let mut graph = ColumnConstraintGraph::new();
        graph.add_predicate(&pred);
        
        let range_a = graph.range_constraints.get(&ColumnId(1)).unwrap();
        let range_b = graph.range_constraints.get(&ColumnId(2)).unwrap();
        
        // Both should be [101, 199] due to strict < and > (op_to_range adds/subs 1.0)
        assert_eq!(range_a.lower, Some(101.0));
        assert_eq!(range_a.upper, Some(199.0));
        assert_eq!(range_b.lower, Some(101.0));
        assert_eq!(range_b.upper, Some(199.0));
    }

    #[test]
    fn test_contradiction_detection() {
        // a > 50 AND a < 30 → contradiction
        let pred = ScalarExpr::BoolExpr {
            bool_type: BoolExprType::And,
            args: vec![
                ScalarExpr::OpExpr {
                    op_oid: 521, // int4gt
                    return_type: 16,
                    args: vec![
                        ScalarExpr::ColumnRef(ColumnId(1)),
                        ScalarExpr::Const {
                            type_oid: 23, typmod: -1, collation: 0,
                            value: ConstValue::Int32(50), is_null: false,
                        },
                    ],
                },
                ScalarExpr::OpExpr {
                    op_oid: 97, // int4lt
                    return_type: 16,
                    args: vec![
                        ScalarExpr::ColumnRef(ColumnId(1)),
                        ScalarExpr::Const {
                            type_oid: 23, typmod: -1, collation: 0,
                            value: ConstValue::Int32(30), is_null: false,
                        },
                    ],
                },
            ],
        };
        let ranges = collect_column_ranges(&pred);
        let r = ranges.get(&ColumnId(1)).unwrap();
        assert!(r.lower.unwrap() > r.upper.unwrap(), "expected contradiction");
    }

    #[test]
    fn test_range_no_contradiction() {
        // a > 10 AND a < 100 → valid range
        let pred = ScalarExpr::BoolExpr {
            bool_type: BoolExprType::And,
            args: vec![
                ScalarExpr::OpExpr {
                    op_oid: 521,
                    return_type: 16,
                    args: vec![
                        ScalarExpr::ColumnRef(ColumnId(1)),
                        ScalarExpr::Const {
                            type_oid: 23, typmod: -1, collation: 0,
                            value: ConstValue::Int32(10), is_null: false,
                        },
                    ],
                },
                ScalarExpr::OpExpr {
                    op_oid: 97,
                    return_type: 16,
                    args: vec![
                        ScalarExpr::ColumnRef(ColumnId(1)),
                        ScalarExpr::Const {
                            type_oid: 23, typmod: -1, collation: 0,
                            value: ConstValue::Int32(100), is_null: false,
                        },
                    ],
                },
            ],
        };
        let ranges = collect_column_ranges(&pred);
        let r = ranges.get(&ColumnId(1)).unwrap();
        assert!(r.lower.unwrap() <= r.upper.unwrap(), "expected valid range");
    }

    #[test]
    fn test_range_selectivity() {
        let r = ColumnRange { lower: Some(50.0), upper: Some(100.0) };
        let sel = r.selectivity(0.0, 200.0);
        assert!((sel - 0.25).abs() < 0.01); // 50/200 = 0.25
    }

    #[test]
    fn test_mcv_equality_hit() {
        // Value 42 is in MCV with frequency 0.15
        let col_stats = ColumnStats {
            attnum: 1,
            name: "a".into(),
            ndistinct: 100.0,
            null_fraction: 0.0,
            avg_width: 4,
            correlation: 0.0,
            histogram_bounds: None,
            most_common_vals: Some(vec![
                McvEntry { value: ConstValue::Int32(42), frequency: 0.15 },
                McvEntry { value: ConstValue::Int32(7), frequency: 0.10 },
                McvEntry { value: ConstValue::Int32(99), frequency: 0.08 },
            ]),
        };
        let (catalog, tid) = make_catalog_with_stats(col_stats);

        // col = 42
        let pred = ScalarExpr::OpExpr {
            op_oid: 96, // int4eq
            return_type: 16,
            args: vec![
                ScalarExpr::ColumnRef(ColumnId(1)),
                ScalarExpr::Const {
                    type_oid: 23, typmod: -1, collation: 0,
                    value: ConstValue::Int32(42), is_null: false,
                },
            ],
        };
        let sel = estimate_selectivity_v2(&pred, &catalog, &[tid]);
        assert!((sel - 0.15).abs() < 0.001, "expected MCV frequency 0.15, got {sel}");
    }

    #[test]
    fn test_mcv_equality_miss() {
        // Value 55 is NOT in MCV
        let col_stats = ColumnStats {
            attnum: 1,
            name: "a".into(),
            ndistinct: 100.0,
            null_fraction: 0.0,
            avg_width: 4,
            correlation: 0.0,
            histogram_bounds: None,
            most_common_vals: Some(vec![
                McvEntry { value: ConstValue::Int32(42), frequency: 0.15 },
                McvEntry { value: ConstValue::Int32(7), frequency: 0.10 },
            ]),
        };
        let (catalog, tid) = make_catalog_with_stats(col_stats);

        // col = 55 (not in MCV)
        let pred = ScalarExpr::OpExpr {
            op_oid: 96,
            return_type: 16,
            args: vec![
                ScalarExpr::ColumnRef(ColumnId(1)),
                ScalarExpr::Const {
                    type_oid: 23, typmod: -1, collation: 0,
                    value: ConstValue::Int32(55), is_null: false,
                },
            ],
        };
        let sel = estimate_selectivity_v2(&pred, &catalog, &[tid]);
        // Expected: (1.0 - 0.25) / (100 - 2) = 0.75 / 98 ≈ 0.00765
        assert!((sel - 0.75 / 98.0).abs() < 0.001, "got {sel}");
    }

    #[test]
    fn test_histogram_range_selectivity() {
        // Histogram with 5 equi-width buckets: [0, 20, 40, 60, 80, 100]
        let col_stats = ColumnStats {
            attnum: 1,
            name: "a".into(),
            ndistinct: 100.0,
            null_fraction: 0.0,
            avg_width: 4,
            correlation: 0.0,
            histogram_bounds: Some(vec![
                HistogramBound { value: ConstValue::Int32(0) },
                HistogramBound { value: ConstValue::Int32(20) },
                HistogramBound { value: ConstValue::Int32(40) },
                HistogramBound { value: ConstValue::Int32(60) },
                HistogramBound { value: ConstValue::Int32(80) },
                HistogramBound { value: ConstValue::Int32(100) },
            ]),
            most_common_vals: None,
        };
        let (catalog, tid) = make_catalog_with_stats(col_stats);

        // col < 50 → should be ~50% (2.5 buckets out of 5)
        let pred = ScalarExpr::OpExpr {
            op_oid: 97, // int4lt
            return_type: 16,
            args: vec![
                ScalarExpr::ColumnRef(ColumnId(1)),
                ScalarExpr::Const {
                    type_oid: 23, typmod: -1, collation: 0,
                    value: ConstValue::Int32(50), is_null: false,
                },
            ],
        };
        let sel = estimate_selectivity_v2(&pred, &catalog, &[tid]);
        assert!((sel - 0.5).abs() < 0.01, "expected ~0.5, got {sel}");
    }

    #[test]
    fn test_and_damping() {
        // Two predicates with AND: damping should increase selectivity vs naive product
        let col_stats = ColumnStats {
            attnum: 1,
            name: "a".into(),
            ndistinct: 10.0,
            ..Default::default()
        };
        let (catalog, tid) = make_catalog_with_stats(col_stats);

        // a = 5 AND a = 5  (contrived but tests damping path)
        let pred_single = ScalarExpr::OpExpr {
            op_oid: 96,
            return_type: 16,
            args: vec![
                ScalarExpr::ColumnRef(ColumnId(1)),
                ScalarExpr::Const {
                    type_oid: 23, typmod: -1, collation: 0,
                    value: ConstValue::Int32(5), is_null: false,
                },
            ],
        };
        let _single_sel = estimate_selectivity_recursive(&pred_single, &catalog, &[tid]);

        // Two independent predicates with AND (using two different ops to avoid contradiction)
        let pred_and = ScalarExpr::BoolExpr {
            bool_type: BoolExprType::And,
            args: vec![
                ScalarExpr::OpExpr {
                    op_oid: 97, // int4lt
                    return_type: 16,
                    args: vec![
                        ScalarExpr::ColumnRef(ColumnId(1)),
                        ScalarExpr::Const {
                            type_oid: 23, typmod: -1, collation: 0,
                            value: ConstValue::Int32(50), is_null: false,
                        },
                    ],
                },
                ScalarExpr::OpExpr {
                    op_oid: 521, // int4gt
                    return_type: 16,
                    args: vec![
                        ScalarExpr::ColumnRef(ColumnId(1)),
                        ScalarExpr::Const {
                            type_oid: 23, typmod: -1, collation: 0,
                            value: ConstValue::Int32(10), is_null: false,
                        },
                    ],
                },
            ],
        };
        let and_sel = estimate_selectivity_recursive(&pred_and, &catalog, &[tid]);
        // With damping factor 0.75, AND selectivity should be: sel1 * sel2 * 0.75
        // This is larger than naive sel1 * sel2
        let naive = 0.3333 * 0.3333;
        assert!(and_sel > naive, "damped {and_sel} should be > naive {naive}");
    }
}
