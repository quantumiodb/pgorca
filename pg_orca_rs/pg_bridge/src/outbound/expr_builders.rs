use std::ffi::CString;
use std::str::FromStr;

use pgrx::{pg_sys, IntoDatum};

use optimizer_core::ir::scalar::{ScalarExpr, BoolExprType, NullTestType, ConstValue};
use optimizer_core::ir::types::ColumnRef;
use optimizer_core::plan::extract::PhysicalPlan;
use crate::inbound::column_mapping::ColumnMapping;
use crate::utils::palloc::palloc_node;
use crate::utils::pg_list::list_iter;
use super::OutboundError;

// ── Var builder ──────────────────────────────────────────────────────────────

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

// PG17 changed OUTER_VAR/INNER_VAR from 65000/65001 to -2/-1
pub const OUTER_VAR: i32 = -2;
pub const INNER_VAR: i32 = -1;

/// Build a PG Var node with overridden varno (e.g. OUTER_VAR / INNER_VAR).
pub unsafe fn build_var_with_varno(
    col: &ColumnRef,
    varno: i32,
    varattno: i16,
) -> *mut pg_sys::Var {
    let var = palloc_node::<pg_sys::Var>(pg_sys::NodeTag::T_Var);
    (*var).varno = varno;
    (*var).varattno = varattno;
    (*var).vartype = pg_sys::Oid::from(col.pg_vartype);
    (*var).vartypmod = col.pg_vartypmod;
    (*var).varcollid = pg_sys::Oid::from(col.pg_varcollid);
    (*var).varlevelsup = 0;
    (*var).varnosyn = col.pg_varno;
    (*var).varattnosyn = col.pg_varattno;
    (*var).location = -1;
    var
}

// ── TargetEntry builder ──────────────────────────────────────────────────────

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

// ── Scan target list ─────────────────────────────────────────────────────────

/// Build the target list for a scan node from the PhysicalPlan's output columns.
pub unsafe fn build_target_list_for_scan(
    plan: &PhysicalPlan,
    col_map: &ColumnMapping,
) -> Result<*mut pg_sys::List, OutboundError> {
    let mut list: *mut pg_sys::List = std::ptr::null_mut();
    for (i, te) in plan.target_list.iter().enumerate() {
        let col_ref = col_map.get_column_ref(te.col_id)
            .ok_or_else(|| OutboundError::VarMappingError(
                format!("column {:?} not in mapping", te.col_id)
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

/// Build a target list for a join output — uses OUTER_VAR / INNER_VAR.
pub unsafe fn build_join_target_list(
    outer_plan: *mut pg_sys::Plan,
    inner_plan: *mut pg_sys::Plan,
) -> *mut pg_sys::List {
    let mut list: *mut pg_sys::List = std::ptr::null_mut();
    let mut resno: i16 = 1;

    // Copy outer target entries with varno = OUTER_VAR
    let outer_tes = list_iter::<pg_sys::TargetEntry>((*outer_plan).targetlist);
    for (i, te_ptr) in outer_tes.iter().enumerate() {
        let te = &**te_ptr;
        // Remap the Var to point into the outer plan's output
        let new_var = if !te.expr.is_null() && (*te.expr).type_ == pg_sys::NodeTag::T_Var {
            let orig = te.expr as *mut pg_sys::Var;
            let v = palloc_node::<pg_sys::Var>(pg_sys::NodeTag::T_Var);
            (*v).varno = OUTER_VAR;
            (*v).varattno = (i + 1) as i16;
            (*v).vartype = (*orig).vartype;
            (*v).vartypmod = (*orig).vartypmod;
            (*v).varcollid = (*orig).varcollid;
            (*v).varlevelsup = 0;
            (*v).varnosyn = (*orig).varnosyn;
            (*v).varattnosyn = (*orig).varattnosyn;
            (*v).location = -1;
            v
        } else {
            palloc_node::<pg_sys::Var>(pg_sys::NodeTag::T_Var)
        };
        let new_te = build_target_entry(new_var as *mut pg_sys::Expr, resno, "", te.resjunk);
        (*new_te).resname = te.resname;
        list = pg_sys::lappend(list, new_te as *mut std::ffi::c_void);
        resno += 1;
    }

    // Copy inner target entries with varno = INNER_VAR
    let inner_tes = list_iter::<pg_sys::TargetEntry>((*inner_plan).targetlist);
    for (i, te_ptr) in inner_tes.iter().enumerate() {
        let te = &**te_ptr;
        let new_var = if !te.expr.is_null() && (*te.expr).type_ == pg_sys::NodeTag::T_Var {
            let orig = te.expr as *mut pg_sys::Var;
            let v = palloc_node::<pg_sys::Var>(pg_sys::NodeTag::T_Var);
            (*v).varno = INNER_VAR;
            (*v).varattno = (i + 1) as i16;
            (*v).vartype = (*orig).vartype;
            (*v).vartypmod = (*orig).vartypmod;
            (*v).varcollid = (*orig).varcollid;
            (*v).varlevelsup = 0;
            (*v).varnosyn = (*orig).varnosyn;
            (*v).varattnosyn = (*orig).varattnosyn;
            (*v).location = -1;
            v
        } else {
            palloc_node::<pg_sys::Var>(pg_sys::NodeTag::T_Var)
        };
        let new_te = build_target_entry(new_var as *mut pg_sys::Expr, resno, "", te.resjunk);
        (*new_te).resname = te.resname;
        list = pg_sys::lappend(list, new_te as *mut std::ffi::c_void);
        resno += 1;
    }

    list
}

// ── Scalar expression builder ────────────────────────────────────────────────

/// Convert a ScalarExpr to a PG Expr node.
pub unsafe fn build_scalar_expr(
    expr: &ScalarExpr,
    col_map: &ColumnMapping,
) -> Result<*mut pg_sys::Expr, OutboundError> {
    match expr {
        ScalarExpr::ColumnRef(col_id) => {
            let col_ref = col_map.get_column_ref(*col_id)
                .ok_or_else(|| OutboundError::VarMappingError(
                    format!("column {:?} not found", col_id)
                ))?;
            Ok(build_var(col_ref) as *mut pg_sys::Expr)
        }

        ScalarExpr::Const { type_oid, typmod, collation, value, is_null } => {
            Ok(build_const(*type_oid, *typmod, *collation, value, *is_null) as *mut pg_sys::Expr)
        }

        ScalarExpr::OpExpr { op_oid, return_type, args } => {
            let op_node = palloc_node::<pg_sys::OpExpr>(pg_sys::NodeTag::T_OpExpr);
            (*op_node).opno = pg_sys::Oid::from(*op_oid);
            (*op_node).opfuncid = pg_sys::get_opcode(pg_sys::Oid::from(*op_oid));
            (*op_node).opresulttype = pg_sys::Oid::from(*return_type);
            (*op_node).opretset = false;
            (*op_node).opcollid = pg_sys::Oid::from(0u32);
            (*op_node).inputcollid = pg_sys::Oid::from(0u32);
            (*op_node).location = -1;

            let mut arg_list: *mut pg_sys::List = std::ptr::null_mut();
            for arg in args {
                let arg_expr = build_scalar_expr(arg, col_map)?;
                arg_list = pg_sys::lappend(arg_list, arg_expr as *mut std::ffi::c_void);
            }
            (*op_node).args = arg_list;
            Ok(op_node as *mut pg_sys::Expr)
        }

        ScalarExpr::BoolExpr { bool_type, args } => {
            let boolop = match bool_type {
                BoolExprType::And => pg_sys::BoolExprType::AND_EXPR,
                BoolExprType::Or => pg_sys::BoolExprType::OR_EXPR,
                BoolExprType::Not => pg_sys::BoolExprType::NOT_EXPR,
            };
            let bool_node = palloc_node::<pg_sys::BoolExpr>(pg_sys::NodeTag::T_BoolExpr);
            (*bool_node).boolop = boolop;
            (*bool_node).location = -1;

            let mut arg_list: *mut pg_sys::List = std::ptr::null_mut();
            for arg in args {
                let arg_expr = build_scalar_expr(arg, col_map)?;
                arg_list = pg_sys::lappend(arg_list, arg_expr as *mut std::ffi::c_void);
            }
            (*bool_node).args = arg_list;
            Ok(bool_node as *mut pg_sys::Expr)
        }

        ScalarExpr::NullTest { arg, null_test_type } => {
            let nt = palloc_node::<pg_sys::NullTest>(pg_sys::NodeTag::T_NullTest);
            (*nt).nulltesttype = match null_test_type {
                NullTestType::IsNull => pg_sys::NullTestType::IS_NULL,
                NullTestType::IsNotNull => pg_sys::NullTestType::IS_NOT_NULL,
            };
            (*nt).arg = build_scalar_expr(arg, col_map)? as *mut pg_sys::Expr;
            (*nt).argisrow = false;
            (*nt).location = -1;
            Ok(nt as *mut pg_sys::Expr)
        }

        ScalarExpr::FuncExpr { func_oid, return_type, args, func_variadic } => {
            let fe = palloc_node::<pg_sys::FuncExpr>(pg_sys::NodeTag::T_FuncExpr);
            (*fe).funcid = pg_sys::Oid::from(*func_oid);
            (*fe).funcresulttype = pg_sys::Oid::from(*return_type);
            (*fe).funcvariadic = *func_variadic;
            (*fe).funcformat = pg_sys::CoercionForm::COERCE_EXPLICIT_CALL;
            (*fe).location = -1;

            let mut arg_list: *mut pg_sys::List = std::ptr::null_mut();
            for arg in args {
                let arg_expr = build_scalar_expr(arg, col_map)?;
                arg_list = pg_sys::lappend(arg_list, arg_expr as *mut std::ffi::c_void);
            }
            (*fe).args = arg_list;
            Ok(fe as *mut pg_sys::Expr)
        }

        ScalarExpr::Cast { arg, target_type, typmod } => {
            let cv = palloc_node::<pg_sys::CoerceViaIO>(pg_sys::NodeTag::T_CoerceViaIO);
            (*cv).arg = build_scalar_expr(arg, col_map)?;
            (*cv).resulttype = pg_sys::Oid::from(*target_type);
            (*cv).resultcollid = pg_sys::Oid::from(0u32);
            (*cv).coerceformat = pg_sys::CoercionForm::COERCE_IMPLICIT_CAST;
            (*cv).location = -1;
            Ok(cv as *mut pg_sys::Expr)
        }

        ScalarExpr::AggRef(agg_expr) => {
            build_aggref(agg_expr, col_map)
        }

        ScalarExpr::Param { param_id, param_type } => {
            let p = palloc_node::<pg_sys::Param>(pg_sys::NodeTag::T_Param);
            (*p).paramkind = pg_sys::ParamKind::PARAM_EXTERN;
            (*p).paramid = *param_id as i32;
            (*p).paramtype = pg_sys::Oid::from(*param_type);
            (*p).paramtypmod = -1;
            (*p).paramcollid = pg_sys::Oid::from(0u32);
            (*p).location = -1;
            Ok(p as *mut pg_sys::Expr)
        }

        ScalarExpr::ScalarArrayOp { op_oid, use_or, scalar, array } => {
            let sao = palloc_node::<pg_sys::ScalarArrayOpExpr>(pg_sys::NodeTag::T_ScalarArrayOpExpr);
            (*sao).opno = pg_sys::Oid::from(*op_oid);
            (*sao).opfuncid = pg_sys::get_opcode(pg_sys::Oid::from(*op_oid));
            (*sao).useOr = *use_or;
            (*sao).inputcollid = pg_sys::Oid::from(0u32);
            (*sao).location = -1;

            let scalar_expr = build_scalar_expr(scalar, col_map)?;
            let array_expr = build_scalar_expr(array, col_map)?;
            let mut arg_list: *mut pg_sys::List = std::ptr::null_mut();
            arg_list = pg_sys::lappend(arg_list, scalar_expr as *mut std::ffi::c_void);
            arg_list = pg_sys::lappend(arg_list, array_expr as *mut std::ffi::c_void);
            (*sao).args = arg_list;
            Ok(sao as *mut pg_sys::Expr)
        }

        ScalarExpr::WindowFunc {
            winfnoid, wintype, wincollid, inputcollid,
            args, winref, winstar, winagg,
        } => {
            let wf = palloc_node::<pg_sys::WindowFunc>(pg_sys::NodeTag::T_WindowFunc);
            (*wf).winfnoid = pg_sys::Oid::from(*winfnoid);
            (*wf).wintype = pg_sys::Oid::from(*wintype);
            (*wf).wincollid = pg_sys::Oid::from(*wincollid);
            (*wf).inputcollid = pg_sys::Oid::from(*inputcollid);
            (*wf).winref = *winref;
            (*wf).winstar = *winstar;
            (*wf).winagg = *winagg;
            (*wf).aggfilter = std::ptr::null_mut();
            (*wf).location = -1;

            let mut arg_list: *mut pg_sys::List = std::ptr::null_mut();
            for arg in args {
                let arg_expr = build_scalar_expr(arg, col_map)?;
                arg_list = pg_sys::lappend(arg_list, arg_expr as *mut std::ffi::c_void);
            }
            (*wf).args = arg_list;
            Ok(wf as *mut pg_sys::Expr)
        }

        ScalarExpr::CaseExpr { arg, when_clauses, default, result_type } => {
            let ce = palloc_node::<pg_sys::CaseExpr>(pg_sys::NodeTag::T_CaseExpr);
            (*ce).casetype = pg_sys::Oid::from(*result_type);
            (*ce).casecollid = pg_sys::Oid::from(0u32);
            (*ce).location = -1;

            if let Some(a) = arg {
                (*ce).arg = build_scalar_expr(a, col_map)? as *mut pg_sys::Expr;
            }

            let mut args_list: *mut pg_sys::List = std::ptr::null_mut();
            for (cond, result) in when_clauses {
                let cw = palloc_node::<pg_sys::CaseWhen>(pg_sys::NodeTag::T_CaseWhen);
                (*cw).expr = build_scalar_expr(cond, col_map)? as *mut pg_sys::Expr;
                (*cw).result = build_scalar_expr(result, col_map)? as *mut pg_sys::Expr;
                (*cw).location = -1;
                args_list = pg_sys::lappend(args_list, cw as *mut std::ffi::c_void);
            }
            (*ce).args = args_list;

            if let Some(d) = default {
                (*ce).defresult = build_scalar_expr(d, col_map)? as *mut pg_sys::Expr;
            }

            Ok(ce as *mut pg_sys::Expr)
        }

        ScalarExpr::Coalesce { args, result_type } => {
            let ce = palloc_node::<pg_sys::CoalesceExpr>(pg_sys::NodeTag::T_CoalesceExpr);
            (*ce).coalescetype = pg_sys::Oid::from(*result_type);
            (*ce).coalescecollid = pg_sys::Oid::from(0u32);
            (*ce).location = -1;

            let mut args_list: *mut pg_sys::List = std::ptr::null_mut();
            for arg in args {
                let arg_expr = build_scalar_expr(arg, col_map)?;
                args_list = pg_sys::lappend(args_list, arg_expr as *mut std::ffi::c_void);
            }
            (*ce).args = args_list;
            Ok(ce as *mut pg_sys::Expr)
        }

        other => Err(OutboundError::PlanBuildError(
            format!("unsupported ScalarExpr variant: {:?}", std::mem::discriminant(other))
        )),
    }
}

/// Build a PG Const node.
unsafe fn build_const(
    type_oid: u32,
    typmod: i32,
    collation: u32,
    value: &ConstValue,
    is_null: bool,
) -> *mut pg_sys::Const {
    let c = palloc_node::<pg_sys::Const>(pg_sys::NodeTag::T_Const);
    (*c).consttype = pg_sys::Oid::from(type_oid);
    (*c).consttypmod = typmod;
    (*c).constcollid = pg_sys::Oid::from(collation);
    (*c).constisnull = is_null;
    (*c).location = -1;

    let (byval, len, datum): (bool, i32, pg_sys::Datum) = match value {
        ConstValue::Bool(b) => (true, 1, pg_sys::Datum::from(*b as usize)),
        ConstValue::Int16(v) => (true, 2, pg_sys::Datum::from(*v as usize)),
        ConstValue::Int32(v) => (true, 4, pg_sys::Datum::from(*v as usize)),
        ConstValue::Int64(v) => (true, 8, pg_sys::Datum::from(*v as usize)),
        ConstValue::Float32(v) => (true, 4, pg_sys::Datum::from(v.to_bits() as usize)),
        ConstValue::Float64(v) => (true, 8, pg_sys::Datum::from(v.to_bits() as usize)),
        ConstValue::Text(s) => {
            let bytes = s.as_bytes();
            let total_len = 4 + bytes.len();
            let buf = pg_sys::palloc(total_len) as *mut u8;
            let header = ((total_len as u32) << 2) | 0;
            *(buf as *mut u32) = header;
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf.add(4), bytes.len());
            (false, -1, pg_sys::Datum::from(buf as usize))
        }
        ConstValue::Numeric(s) => {
            let n = pgrx::AnyNumeric::from_str(s).unwrap_or_else(|_| pgrx::AnyNumeric::from(0));
            let datum = n.into_datum().expect("failed to convert Numeric to datum");
            (false, -1, datum)
        }
        ConstValue::Date(v) => (true, 4, pg_sys::Datum::from(*v)),
        ConstValue::Timestamp(v) => (true, 8, pg_sys::Datum::from(*v)),
        ConstValue::TimestampTz(v) => (true, 8, pg_sys::Datum::from(*v)),
        ConstValue::Money(v) => (true, 8, pg_sys::Datum::from(*v)),
        ConstValue::Char(v) => (true, 1, pg_sys::Datum::from(*v as usize)),
        ConstValue::Oid(v) => (true, 4, pg_sys::Datum::from(*v as usize)),
        ConstValue::Uuid(bytes) => {
            let uuid = pgrx::Uuid::from_bytes(*bytes);
            let datum = uuid.into_datum().expect("failed to convert Uuid to datum");
            (false, 16, datum)
        }
        ConstValue::Lsn(v) => (true, 8, pg_sys::Datum::from(*v as usize)),
        ConstValue::Null => (true, -1, pg_sys::Datum::from(0usize)),
        ConstValue::Bytea(b) => {
            let total_len = 4 + b.len();
            let buf = pg_sys::palloc(total_len) as *mut u8;
            let header = ((total_len as u32) << 2) | 0;
            *(buf as *mut u32) = header;
            std::ptr::copy_nonoverlapping(b.as_ptr(), buf.add(4), b.len());
            (false, -1, pg_sys::Datum::from(buf as usize))
        }
        ConstValue::Time(micros) => (true, 8, pg_sys::Datum::from(*micros)),
        ConstValue::TimeTz { micros, offset } => {
            // TimeTzADT is a pass-by-reference struct { time: i64, zone: i32 }
            let buf = pg_sys::palloc(std::mem::size_of::<pg_sys::TimeTzADT>()) as *mut pg_sys::TimeTzADT;
            (*buf).time = *micros;
            (*buf).zone = *offset;
            (false, std::mem::size_of::<pg_sys::TimeTzADT>() as i32, pg_sys::Datum::from(buf as usize))
        }
        ConstValue::Interval { months, days, micros } => {
            // pg_sys::Interval is { time: i64, day: i32, month: i32 }
            let buf = pg_sys::palloc(std::mem::size_of::<pg_sys::Interval>()) as *mut pg_sys::Interval;
            (*buf).time = *micros;
            (*buf).day = *days;
            (*buf).month = *months;
            (false, std::mem::size_of::<pg_sys::Interval>() as i32, pg_sys::Datum::from(buf as usize))
        }
        ConstValue::Bit(s) | ConstValue::Json(s) | ConstValue::Jsonb(s)
        | ConstValue::OpaqueText(s) => {
            // Use PG input function to reconstruct the datum from text representation.
            // This covers bit/varbit, json, jsonb, inet, cidr, macaddr, xml, range types,
            // geometric types, tsvector, tsquery, jsonpath, enums, and other opaque types.
            let mut input_fn = pg_sys::Oid::INVALID;
            let mut ioparam = pg_sys::Oid::INVALID;
            let pg_type_oid = pg_sys::Oid::from(type_oid);
            pg_sys::getTypeInputInfo(pg_type_oid, &mut input_fn, &mut ioparam);
            let cstr = std::ffi::CString::new(s.as_str()).unwrap_or_default();
            let d = pg_sys::OidInputFunctionCall(input_fn, cstr.as_ptr() as *mut _, ioparam, typmod);
            // Use the type's actual byval/typlen so PG interprets the datum correctly.
            // Enum types are stored by value (Oid, 4 bytes); varlena types are not.
            let byval = pg_sys::get_typbyval(pg_type_oid);
            let len = pg_sys::get_typlen(pg_type_oid) as i32;
            (byval, len, d)
        }
    };

    (*c).constbyval = byval;
    (*c).constlen = len;
    (*c).constvalue = datum;
    c
}

/// Build a PG Aggref node, looking up metadata from pg_aggregate catalog.
pub unsafe fn build_aggref(
    agg: &optimizer_core::ir::types::AggExpr,
    col_map: &ColumnMapping,
) -> Result<*mut pg_sys::Expr, OutboundError> {
    let aggfnoid = pg_sys::Oid::from(agg.agg_func_oid);
    let aggref = palloc_node::<pg_sys::Aggref>(pg_sys::NodeTag::T_Aggref);
    (*aggref).aggfnoid = aggfnoid;
    (*aggref).aggtype = pg_sys::Oid::from(agg.result_type);
    (*aggref).aggsplit = pg_sys::AggSplit::AGGSPLIT_SIMPLE;
    (*aggref).aggdistinct = std::ptr::null_mut();
    (*aggref).aggfilter = std::ptr::null_mut();
    (*aggref).aggorder = std::ptr::null_mut();
    (*aggref).aggdirectargs = std::ptr::null_mut();
    (*aggref).location = -1;

    // Look up pg_aggregate for aggtranstype
    let agg_tup = pg_sys::SearchSysCache1(
        pg_sys::SysCacheIdentifier::AGGFNOID as i32,
        aggfnoid.into(),
    );
    if !agg_tup.is_null() {
        let mut isnull = false;
        // Anum_pg_aggregate_aggtranstype = 17 in PG17
        let transtype_datum = pg_sys::SysCacheGetAttr(
            pg_sys::SysCacheIdentifier::AGGFNOID as i32,
            agg_tup,
            17, // Anum_pg_aggregate_aggtranstype
            &mut isnull,
        );
        if !isnull {
            (*aggref).aggtranstype = pg_sys::Oid::from(transtype_datum.value() as u32);
        }
        pg_sys::ReleaseSysCache(agg_tup);
    }

    // Build aggargtypes list from argument types
    let mut argtypes_list: *mut pg_sys::List = std::ptr::null_mut();
    for arg in &agg.args {
        if let optimizer_core::ir::scalar::ScalarExpr::ColumnRef(cid) = arg {
            if let Some(cr) = col_map.get_column_ref(*cid) {
                argtypes_list = pg_sys::lappend_oid(argtypes_list, pg_sys::Oid::from(cr.pg_vartype));
            }
        }
    }
    (*aggref).aggargtypes = argtypes_list;
    (*aggref).aggcollid = pg_sys::Oid::from(0u32);
    (*aggref).inputcollid = pg_sys::Oid::from(0u32);

    // Build args as TargetEntry list
    let mut arg_list: *mut pg_sys::List = std::ptr::null_mut();
    for (i, arg) in agg.args.iter().enumerate() {
        let arg_expr = build_scalar_expr(arg, col_map)?;
        let te = build_target_entry(arg_expr, (i + 1) as i16, "", false);
        arg_list = pg_sys::lappend(arg_list, te as *mut std::ffi::c_void);
    }
    (*aggref).args = arg_list;

    if let Some(filter) = &agg.filter {
        (*aggref).aggfilter = build_scalar_expr(filter, col_map)? as *mut pg_sys::Expr;
    }

    Ok(aggref as *mut pg_sys::Expr)
}

/// Build a target list for an Agg node: group-by Var columns (using OUTER_VAR) + Aggref expressions.
///
/// Vars in the Agg target list must reference OUTER_VAR because the executor reads
/// from the child plan's output slot, not directly from the table.
pub unsafe fn build_agg_target_list(
    plan: &PhysicalPlan,
    group_by: &[optimizer_core::ir::types::ColumnId],
    aggregates: &[optimizer_core::ir::types::AggExpr],
    child_plan: *mut pg_sys::Plan,
    col_map: &ColumnMapping,
) -> Result<*mut pg_sys::List, OutboundError> {
    let mut list: *mut pg_sys::List = std::ptr::null_mut();
    let mut resno: i16 = 1;

    // First: group-by columns as Var nodes referencing OUTER_VAR (child plan output)
    for col_id in group_by {
        let col_ref = col_map.get_column_ref(*col_id)
            .ok_or_else(|| OutboundError::VarMappingError(
                format!("agg group-by col {:?} not found", col_id)
            ))?;
        // Find position of this column in child plan's target list
        let child_attno = find_col_in_target_list(
            col_ref.pg_varno, col_ref.pg_varattno, child_plan
        ).unwrap_or(col_ref.pg_varattno);

        let var = build_var_with_varno(col_ref, OUTER_VAR, child_attno);
        let te = build_target_entry(var as *mut pg_sys::Expr, resno, &col_ref.name, false);
        (*te).ressortgroupref = resno as u32;
        list = pg_sys::lappend(list, te as *mut std::ffi::c_void);
        resno += 1;
    }

    // Then: aggregate expressions as Aggref nodes
    for agg_expr in aggregates {
        let aggref = build_aggref(agg_expr, col_map)?;
        let te = build_target_entry(aggref, resno, "", false);
        list = pg_sys::lappend(list, te as *mut std::ffi::c_void);
        resno += 1;
    }

    // If no group-by columns and no aggregates, fall back to scan target list
    if list.is_null() {
        return build_target_list_for_scan(plan, col_map);
    }

    Ok(list)
}

/// Find the 1-based position of (varno, varattno) in a plan's target list.
unsafe fn find_col_in_target_list(varno: u32, varattno: i16, plan: *mut pg_sys::Plan) -> Option<i16> {
    if plan.is_null() || (*plan).targetlist.is_null() {
        return None;
    }
    let tes = list_iter::<pg_sys::TargetEntry>((*plan).targetlist);
    for (i, te_ptr) in tes.iter().enumerate() {
        let te = &**te_ptr;
        if !te.expr.is_null() && (*te.expr).type_ == pg_sys::NodeTag::T_Var {
            let v = te.expr as *const pg_sys::Var;
            if (*v).varno as u32 == varno && (*v).varattno == varattno {
                return Some((i + 1) as i16);
            }
        }
    }
    None
}

/// Build a qual list from a vec of ScalarExpr predicates.
pub unsafe fn build_qual_list(
    predicates: &[ScalarExpr],
    col_map: &ColumnMapping,
) -> Result<*mut pg_sys::List, OutboundError> {
    let mut list: *mut pg_sys::List = std::ptr::null_mut();
    for pred in predicates {
        let expr = build_scalar_expr(pred, col_map)?;
        list = pg_sys::lappend(list, expr as *mut std::ffi::c_void);
    }
    Ok(list)
}
