use pgrx::pg_sys;

use optimizer_core::ir::types::Cost;
use crate::utils::palloc::palloc_node;
use super::OutboundError;

/// Build a T_SeqScan plan node.
pub unsafe fn build_seq_scan(
    scanrelid: u32,
    target_list: *mut pg_sys::List,
    qual: *mut pg_sys::List,
    rows: f64,
    cost: &Cost,
    width: i32,
) -> Result<*mut pg_sys::Plan, OutboundError> {
    // In PG17, SeqScan is a type alias for Scan
    let scan = palloc_node::<pg_sys::SeqScan>(pg_sys::NodeTag::T_SeqScan);

    // Set scan fields
    (*scan).scan.scanrelid = scanrelid;

    // Set plan fields
    let plan = &mut (*scan).scan.plan;
    plan.targetlist = target_list;
    plan.qual = qual;
    plan.startup_cost = cost.startup;
    plan.total_cost = cost.total;
    plan.plan_rows = rows;
    plan.plan_width = width;
    plan.lefttree = std::ptr::null_mut();
    plan.righttree = std::ptr::null_mut();
    plan.parallel_aware = false;
    plan.parallel_safe = false;

    Ok(plan as *mut pg_sys::Plan)
}
