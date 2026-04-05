use std::collections::HashMap;
use std::sync::OnceLock;

use crate::ir::types::{Cost, SortKey};
use crate::properties::logical::LogicalProperties;
use crate::properties::required::RequiredPropsKey;
use crate::properties::delivered::DeliveredProperties;

use super::expr::ExprId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GroupId(pub u32);

pub struct Group {
    pub id: GroupId,
    pub exprs: Vec<ExprId>,
    pub logical_props: OnceLock<LogicalProperties>,
    pub stats: OnceLock<GroupStats>,
    pub winners: HashMap<RequiredPropsKey, Winner>,
    pub explored: bool,
    pub implemented: bool,
}

impl Group {
    pub fn new(id: GroupId) -> Self {
        Self {
            id,
            exprs: Vec::new(),
            logical_props: OnceLock::new(),
            stats: OnceLock::new(),
            winners: HashMap::new(),
            explored: false,
            implemented: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GroupStats {
    pub row_count: f64,
    pub avg_width: f64,
}

/// Which enforcer node (if any) must be injected above this winner.
#[derive(Debug, Clone, PartialEq)]
pub enum EnforcerKind {
    None,
    /// Insert a Sort node with these keys.
    Sort { keys: Vec<SortKey> },
    /// Insert a Gather node (converts Partial → Serial, unordered).
    Gather { num_workers: usize },
    /// Insert a GatherMerge node (converts Partial → Serial, preserving order).
    GatherMerge { num_workers: usize, sort_keys: Vec<SortKey> },
    /// Insert Gather then Sort (parallel output that also needs ordering).
    GatherThenSort { num_workers: usize, sort_keys: Vec<SortKey> },
}

#[derive(Debug, Clone)]
pub struct Winner {
    pub expr_id: ExprId,
    pub cost: Cost,
    pub delivered_props: DeliveredProperties,
    pub needs_enforcer: bool,
    pub enforcer: EnforcerKind,
}
