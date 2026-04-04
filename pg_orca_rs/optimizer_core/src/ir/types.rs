use std::ops::Add;

// ── Identity types ──────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnId(pub u32);

pub type RteIndex = u32;

// ── Column reference (carries PG Var info for Phase 3) ──

#[derive(Debug, Clone)]
pub struct ColumnRef {
    pub id: ColumnId,
    pub table_id: TableId,
    pub name: String,
    pub pg_varno: u32,
    pub pg_varattno: i16,
    pub pg_vartype: u32,
    pub pg_vartypmod: i32,
    pub pg_varcollid: u32,
}

// ── Join type ───────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    AntiSemi,
}

// ── Sort key ────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SortKey {
    pub column: ColumnId,
    pub ascending: bool,
    pub nulls_first: bool,
    pub sort_op_oid: u32,
    pub collation_oid: u32,
}

// ── Aggregate expression ────────────────────────────────

#[derive(Debug, Clone)]
pub struct AggExpr {
    pub agg_func_oid: u32,
    pub args: Vec<super::scalar::ScalarExpr>,
    pub distinct: bool,
    pub filter: Option<Box<super::scalar::ScalarExpr>>,
    pub result_type: u32,
}

// ── Window function clause ──────────────────────────────

#[derive(Debug, Clone)]
pub struct WindowClause {
    pub partition_by: Vec<ColumnId>,
    pub order_by: Vec<SortKey>,
    pub frame_options: i32,  // PG's FRAMEOPTION_* flags
    pub winref: u32,         // matches WindowFunc.winref
}

// ── Index AM type ───────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexAmType {
    BTree,
    Hash,
    GiST,
    GIN,
    BRIN,
    SPGiST,
}

// ── Cost (startup, total) pair ──────────────────────────

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Cost {
    pub startup: f64,
    pub total: f64,
}

impl Cost {
    pub fn zero() -> Self {
        Self { startup: 0.0, total: 0.0 }
    }

    pub fn infinity() -> Self {
        Self { startup: f64::INFINITY, total: f64::INFINITY }
    }
}

impl Add for Cost {
    type Output = Self;
    fn add(self, rhs: Self) -> Self {
        Self {
            startup: self.startup + rhs.startup,
            total: self.total + rhs.total,
        }
    }
}

impl PartialOrd for Cost {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.total.partial_cmp(&other.total)
    }
}
