use pgrx::pg_sys;

use crate::utils::palloc::palloc_node;
use super::OutboundError;

/// Assemble a PlannedStmt from a Plan tree.
pub unsafe fn build_planned_stmt(
    query: &pg_sys::Query,
    plan_tree: *mut pg_sys::Plan,
    relation_oids: *mut pg_sys::List,
) -> Result<*mut pg_sys::PlannedStmt, OutboundError> {
    let stmt = palloc_node::<pg_sys::PlannedStmt>(pg_sys::NodeTag::T_PlannedStmt);

    (*stmt).commandType = query.commandType;
    (*stmt).queryId = query.queryId;
    (*stmt).hasReturning = false;
    (*stmt).hasModifyingCTE = false;
    (*stmt).canSetTag = query.canSetTag;
    (*stmt).planTree = plan_tree;
    (*stmt).rtable = query.rtable;  // Share, don't copy
    (*stmt).permInfos = query.rteperminfos;
    (*stmt).resultRelations = std::ptr::null_mut();
    (*stmt).appendRelations = std::ptr::null_mut();
    (*stmt).subplans = std::ptr::null_mut();
    (*stmt).relationOids = relation_oids;
    (*stmt).paramExecTypes = std::ptr::null_mut();
    (*stmt).stmt_location = query.stmt_location;
    (*stmt).stmt_len = query.stmt_len;

    // Build relationOids from rtable if not already populated
    if (*stmt).relationOids.is_null() {
        let mut oid_list: *mut pg_sys::List = std::ptr::null_mut();
        let rtes = crate::utils::pg_list::list_iter::<pg_sys::RangeTblEntry>(query.rtable);
        for rte_ptr in &rtes {
            let rte = &**rte_ptr;
            if rte.rtekind == pg_sys::RTEKind::RTE_RELATION {
                oid_list = pg_sys::lappend_oid(oid_list, rte.relid);
            }
        }
        (*stmt).relationOids = oid_list;
    }

    Ok(stmt)
}
