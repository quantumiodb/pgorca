use crate::memo::{GroupId, ExprId};
use crate::properties::required::RequiredProperties;

#[derive(Debug, Clone)]
pub enum Task {
    OptimizeGroup {
        group_id: GroupId,
        required: RequiredProperties,
        upper_bound: f64,
        state: OptimizeGroupState,
    },
    OptimizeExpr {
        group_id: GroupId,
        expr_id: ExprId,
        required: RequiredProperties,
        upper_bound: f64,
        state: OptimizeExprState,
    },
    DeriveLogicalProps {
        group_id: GroupId,
        state: DeriveLogicalPropsState,
    },
}

#[derive(Debug, Clone)]
pub enum OptimizeGroupState {
    Init,
    OptimizingExprs {
        expr_idx: usize,
    },
}

#[derive(Debug, Clone)]
pub enum OptimizeExprState {
    Init,
    WaitingForChild {
        child_idx: usize,
        accumulated_cost: f64,
        child_costs: Vec<f64>,
        child_rows: Vec<f64>,
    },
}

#[derive(Debug, Clone)]
pub enum DeriveLogicalPropsState {
    Init,
    WaitingForChildren {
        expr_id: ExprId,
        child_idx: usize,
    },
}
