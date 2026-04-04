pub mod ir;
pub mod memo;
pub mod rules;
pub mod cost;
pub mod properties;
pub mod simplify;
pub mod search;
pub mod utility;
pub mod plan;

use cost::stats::CatalogSnapshot;
use ir::logical::LogicalExpr;
use plan::extract::PhysicalPlan;

/// Top-level optimizer entry point.
/// Takes a logical expression tree and catalog snapshot,
/// returns an optimized physical plan.
pub fn optimize(
    input: LogicalExpr,
    catalog: &CatalogSnapshot,
) -> Result<PhysicalPlan, OptimizerError> {
    search::engine::optimize(input, catalog)
}

/// Optimizer error types
#[derive(Debug)]
pub enum OptimizerError {
    GroupLimitExceeded,
    SearchTimeout,
    NoFeasiblePlan,
    RuleApplicationError(String),
    InternalError(String),
}

impl std::fmt::Display for OptimizerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GroupLimitExceeded => write!(f, "memo group limit exceeded"),
            Self::SearchTimeout => write!(f, "search timeout"),
            Self::NoFeasiblePlan => write!(f, "no feasible plan found"),
            Self::RuleApplicationError(msg) => write!(f, "rule error: {}", msg),
            Self::InternalError(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for OptimizerError {}
