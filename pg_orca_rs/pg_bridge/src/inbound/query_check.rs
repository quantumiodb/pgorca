use pgrx::pg_sys;
use super::InboundError;
use crate::utils::pg_list::list_length;

/// Check if a query is within the supported scope.
pub unsafe fn is_supported_query(query: &pg_sys::Query) -> Result<(), InboundError> {
    if query.commandType != pg_sys::CmdType::CMD_SELECT {
        return Err(InboundError::UnsupportedFeature("non-SELECT".into()));
    }
    if query.hasSubLinks {
        return Err(InboundError::UnsupportedFeature("sublinks".into()));
    }
    // Window functions: supported via WindowAgg node
    if query.hasRecursive {
        return Err(InboundError::UnsupportedFeature("recursive".into()));
    }
    if !query.setOperations.is_null() {
        return Err(InboundError::UnsupportedFeature("set operations".into()));
    }
    // CTEs: PG inlines non-recursive single-use CTEs before we see them.
    // Materialized/multi-use CTEs still have cteList entries — reject those.
    if list_length(query.cteList) > 0 {
        return Err(InboundError::UnsupportedFeature("materialized CTE".into()));
    }
    if !query.utilityStmt.is_null() {
        return Err(InboundError::UnsupportedFeature("utility".into()));
    }
    if query.hasTargetSRFs {
        return Err(InboundError::UnsupportedFeature("target SRFs".into()));
    }

    // Check RTEs
    check_range_table(query.rtable)?;

    Ok(())
}

unsafe fn check_range_table(rtable: *mut pg_sys::List) -> Result<(), InboundError> {
    if rtable.is_null() {
        return Err(InboundError::UnsupportedFeature("empty range table".into()));
    }
    let rtes = crate::utils::pg_list::list_iter::<pg_sys::RangeTblEntry>(rtable);
    for rte in &rtes {
        let kind = (**rte).rtekind;
        match kind {
            pg_sys::RTEKind::RTE_RELATION => {}
            pg_sys::RTEKind::RTE_JOIN => {}
            pg_sys::RTEKind::RTE_RESULT => {}
            pg_sys::RTEKind::RTE_SUBQUERY => {
                // Inlined CTEs appear as RTE_SUBQUERY — reject for now
                // (full subquery support requires recursive planning)
                return Err(InboundError::UnsupportedFeature(
                    "subquery RTE".into()
                ));
            }
            #[cfg(feature = "pg18")]
            pg_sys::RTEKind::RTE_GROUP => {}
            _ => return Err(InboundError::UnsupportedFeature(
                format!("unsupported RTE kind: {:?}", kind)
            )),
        }
    }
    Ok(())
}
