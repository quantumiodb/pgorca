pub mod expr_builders;
pub mod plan_builders;
pub mod planned_stmt;
pub mod sanity_check;

use pgrx::pg_sys;
use optimizer_core::ir::physical::PhysicalOp;
use optimizer_core::plan::extract::PhysicalPlan;
use crate::inbound::column_mapping::ColumnMapping;

#[derive(Debug)]
pub enum OutboundError {
    PlanBuildError(String),
    VarMappingError(String),
    SanityCheckFailed(String),
    UnsupportedPhysicalOp(String),
}

impl std::fmt::Display for OutboundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PlanBuildError(s) => write!(f, "plan build: {}", s),
            Self::VarMappingError(s) => write!(f, "var mapping: {}", s),
            Self::SanityCheckFailed(s) => write!(f, "sanity check: {}", s),
            Self::UnsupportedPhysicalOp(s) => write!(f, "unsupported physical op: {}", s),
        }
    }
}

/// Convert a PhysicalPlan to a PG PlannedStmt.
pub unsafe fn generate_planned_stmt(
    plan: &PhysicalPlan,
    query: &pg_sys::Query,
    col_map: &ColumnMapping,
) -> Result<*mut pg_sys::PlannedStmt, OutboundError> {
    // Build the Plan tree
    let pg_plan = build_plan_node(plan, col_map)?;

    // Collect relation OIDs
    let mut relation_oids: *mut pg_sys::List = std::ptr::null_mut();
    collect_relation_oids(plan, col_map, &mut relation_oids);

    // Assemble PlannedStmt
    let stmt = planned_stmt::build_planned_stmt(query, pg_plan, relation_oids)?;

    // Sanity check
    sanity_check::sanity_check(stmt, query)?;

    Ok(stmt)
}

unsafe fn build_plan_node(
    plan: &PhysicalPlan,
    col_map: &ColumnMapping,
) -> Result<*mut pg_sys::Plan, OutboundError> {
    match &plan.op {
        PhysicalOp::SeqScan { scanrelid } => {
            let target_list = expr_builders::build_target_list_for_scan(plan, col_map)?;
            plan_builders::build_seq_scan(
                *scanrelid,
                target_list,
                std::ptr::null_mut(), // no qual in M1
                plan.rows,
                &plan.cost,
                plan.width as i32,
            )
        }
        other => Err(OutboundError::UnsupportedPhysicalOp(format!("{:?}", other))),
    }
}

unsafe fn collect_relation_oids(
    plan: &PhysicalPlan,
    col_map: &ColumnMapping,
    list: &mut *mut pg_sys::List,
) {
    match &plan.op {
        PhysicalOp::SeqScan { scanrelid } => {
            // Find the OID for this RTE index
            for col_ref in col_map.all_columns() {
                if col_ref.pg_varno == *scanrelid {
                    // We need the table OID, which we stored... we don't have it directly in col_map
                    // For M1, we'll get it from the query's rtable in planned_stmt
                    break;
                }
            }
        }
        _ => {}
    }
    for child in &plan.children {
        collect_relation_oids(child, col_map, list);
    }
}
