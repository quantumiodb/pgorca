use std::ffi::CString;

use pgrx::pg_sys;

use optimizer_core::ir::types::ColumnRef;
use optimizer_core::plan::extract::PhysicalPlan;
use crate::inbound::column_mapping::ColumnMapping;
use crate::utils::palloc::palloc_node;
use super::OutboundError;

/// Build a PG Var node from a ColumnRef.
pub unsafe fn build_var(col: &ColumnRef) -> *mut pg_sys::Var {
    let var = palloc_node::<pg_sys::Var>(pg_sys::NodeTag::T_Var);
    (*var).varno = col.pg_varno as i32;
    (*var).varattno = col.pg_varattno;
    (*var).vartype = pg_sys::Oid::from(col.pg_vartype);
    (*var).vartypmod = col.pg_vartypmod;
    (*var).varcollid = pg_sys::Oid::from(col.pg_varcollid);
    (*var).varlevelsup = 0;
    (*var).varnosyn = col.pg_varno;
    (*var).varattnosyn = col.pg_varattno;
    (*var).location = -1;
    var
}

/// Build a PG TargetEntry node.
pub unsafe fn build_target_entry(
    expr: *mut pg_sys::Expr,
    resno: i16,
    name: &str,
    resjunk: bool,
) -> *mut pg_sys::TargetEntry {
    let te = palloc_node::<pg_sys::TargetEntry>(pg_sys::NodeTag::T_TargetEntry);
    (*te).expr = expr;
    (*te).resno = resno;
    (*te).resjunk = resjunk;

    // Copy name into palloc'd memory
    if !name.is_empty() {
        let cname = CString::new(name).unwrap_or_default();
        let len = cname.as_bytes_with_nul().len();
        let dst = pg_sys::palloc(len) as *mut i8;
        std::ptr::copy_nonoverlapping(cname.as_ptr(), dst, len);
        (*te).resname = dst;
    } else {
        (*te).resname = std::ptr::null_mut();
    }

    te
}

/// Build a target list for a scan node from the PhysicalPlan's output columns.
pub unsafe fn build_target_list_for_scan(
    plan: &PhysicalPlan,
    col_map: &ColumnMapping,
) -> Result<*mut pg_sys::List, OutboundError> {
    let mut list: *mut pg_sys::List = std::ptr::null_mut();

    for (i, te) in plan.target_list.iter().enumerate() {
        let col_ref = col_map.get_column_ref(te.col_id)
            .ok_or_else(|| OutboundError::VarMappingError(
                format!("column {:?} not found in mapping", te.col_id)
            ))?;

        let var = build_var(col_ref);
        let pg_te = build_target_entry(
            var as *mut pg_sys::Expr,
            (i + 1) as i16,
            &col_ref.name,
            te.resjunk,
        );

        list = pg_sys::lappend(list, pg_te as *mut std::ffi::c_void);
    }

    Ok(list)
}
