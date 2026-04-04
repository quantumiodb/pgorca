use pgrx::pg_sys;

use crate::utils::pg_list::list_length;
use super::OutboundError;

/// Validate the generated plan tree before returning to PG.
pub unsafe fn sanity_check(
    stmt: *mut pg_sys::PlannedStmt,
    query: &pg_sys::Query,
) -> Result<(), OutboundError> {
    if stmt.is_null() {
        return Err(OutboundError::SanityCheckFailed("stmt is null".into()));
    }

    let plan = (*stmt).planTree;
    if plan.is_null() {
        return Err(OutboundError::SanityCheckFailed("planTree is null".into()));
    }

    // Check plan_rows is non-negative
    if (*plan).plan_rows < 0.0 {
        return Err(OutboundError::SanityCheckFailed("negative plan_rows".into()));
    }

    // Check targetlist is non-null
    if (*plan).targetlist.is_null() {
        return Err(OutboundError::SanityCheckFailed("null targetlist".into()));
    }

    // For scan nodes, verify scanrelid is in range
    let node_tag = (*plan).type_;
    if node_tag == pg_sys::NodeTag::T_SeqScan {
        let scan = plan as *mut pg_sys::Scan;
        let scanrelid = (*scan).scanrelid;
        let rtable_len = list_length(query.rtable);
        if scanrelid < 1 || scanrelid as i32 > rtable_len {
            return Err(OutboundError::SanityCheckFailed(
                format!("scanrelid {} out of range [1, {}]", scanrelid, rtable_len)
            ));
        }
    }

    Ok(())
}
