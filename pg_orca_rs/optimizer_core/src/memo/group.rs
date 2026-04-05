use std::collections::HashMap;
use std::sync::OnceLock;

use crate::ir::types::Cost;
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

#[derive(Debug, Clone)]
pub struct Winner {
    pub expr_id: ExprId,
    pub cost: Cost,
    pub delivered_props: DeliveredProperties,
    pub needs_enforcer: bool,
}
