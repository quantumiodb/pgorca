use pgrx::{pg_sys, FromDatum};
use optimizer_core::ir::scalar::{ScalarExpr, BoolExprType, NullTestType, ConstValue};
use optimizer_core::ir::types::AggExpr;
use super::InboundError;
use super::column_mapping::ColumnMapping;
use crate::utils::pg_list::list_iter;

/// Convert a PG scalar expression node into our ScalarExpr IR.
pub unsafe fn convert_scalar(
    node: *mut pg_sys::Node,
    col_map: &ColumnMapping,
) -> Result<ScalarExpr, InboundError> {
    if node.is_null() {
        return Err(InboundError::TranslationError("null scalar node".into()));
    }

    let tag = (*node).type_;
    match tag {
        pg_sys::NodeTag::T_Var => convert_var(node as *mut pg_sys::Var, col_map),
        pg_sys::NodeTag::T_Const => convert_const(node as *mut pg_sys::Const),
        pg_sys::NodeTag::T_OpExpr => convert_op_expr(node as *mut pg_sys::OpExpr, col_map),
        pg_sys::NodeTag::T_FuncExpr => convert_func_expr(node as *mut pg_sys::FuncExpr, col_map),
        pg_sys::NodeTag::T_BoolExpr => convert_bool_expr(node as *mut pg_sys::BoolExpr, col_map),
        pg_sys::NodeTag::T_NullTest => convert_null_test(node as *mut pg_sys::NullTest, col_map),
        pg_sys::NodeTag::T_RelabelType => {
            // Type relabeling — just convert the inner arg
            let rt = node as *mut pg_sys::RelabelType;
            convert_scalar((*rt).arg as *mut pg_sys::Node, col_map)
        }
        pg_sys::NodeTag::T_BooleanTest => {
            // e.g. col IS TRUE — treat as OpExpr for now
            Err(InboundError::UnsupportedFeature("BooleanTest".into()))
        }
        pg_sys::NodeTag::T_Aggref => convert_aggref(node as *mut pg_sys::Aggref, col_map),
        pg_sys::NodeTag::T_CoerceViaIO => {
            let cv = node as *mut pg_sys::CoerceViaIO;
            let inner = convert_scalar((*cv).arg as *mut pg_sys::Node, col_map)?;
            Ok(ScalarExpr::Cast {
                arg: Box::new(inner),
                target_type: (*cv).resulttype.to_u32(),
                typmod: -1,
            })
        }
        pg_sys::NodeTag::T_ScalarArrayOpExpr => {
            convert_scalar_array_op(node as *mut pg_sys::ScalarArrayOpExpr, col_map)
        }
        pg_sys::NodeTag::T_WindowFunc => {
            convert_window_func(node as *mut pg_sys::WindowFunc, col_map)
        }
        _ => Err(InboundError::UnsupportedFeature(
            format!("scalar node tag {:?}", tag)
        )),
    }
}

unsafe fn convert_var(
    var: *mut pg_sys::Var,
    col_map: &ColumnMapping,
) -> Result<ScalarExpr, InboundError> {
    let varno = (*var).varno as u32;
    let varattno = (*var).varattno;
    let col_id = col_map.lookup_var(varno, varattno)
        .ok_or_else(|| InboundError::TranslationError(
            format!("Var({}, {}) not found in column map", varno, varattno)
        ))?;
    Ok(ScalarExpr::ColumnRef(col_id))
}

unsafe fn convert_const(c: *mut pg_sys::Const) -> Result<ScalarExpr, InboundError> {
    let type_oid = (*c).consttype.to_u32();
    let typmod = (*c).consttypmod;
    let collation = (*c).constcollid.to_u32();
    let is_null = (*c).constisnull;
    let datum = (*c).constvalue;

    let value = if is_null {
        ConstValue::Null
    } else {
        match (*c).consttype {
            pg_sys::BOOLOID => ConstValue::Bool(bool::from_datum(datum, false).unwrap()),
            pg_sys::INT2OID => ConstValue::Int16(i16::from_datum(datum, false).unwrap()),
            pg_sys::INT4OID => ConstValue::Int32(i32::from_datum(datum, false).unwrap()),
            pg_sys::INT8OID => ConstValue::Int64(i64::from_datum(datum, false).unwrap()),
            pg_sys::FLOAT4OID => ConstValue::Float32(f32::from_datum(datum, false).unwrap()),
            pg_sys::FLOAT8OID => ConstValue::Float64(f64::from_datum(datum, false).unwrap()),
            pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID | pg_sys::NAMEOID => {
                let s = String::from_datum(datum, false).unwrap_or_default();
                ConstValue::Text(s)
            }
            pg_sys::NUMERICOID => {
                let n = pgrx::AnyNumeric::from_datum(datum, false).map(|n| n.to_string()).unwrap_or_default();
                ConstValue::Numeric(n)
            }
            pg_sys::DATEOID => {
                // PG date is 4 bytes
                ConstValue::Date(i32::from_datum(datum, false).unwrap())
            }
            pg_sys::TIMESTAMPOID => {
                // PG timestamp is 8 bytes
                ConstValue::Timestamp(i64::from_datum(datum, false).unwrap())
            }
            pg_sys::TIMESTAMPTZOID => {
                // PG timestamptz is 8 bytes
                ConstValue::TimestampTz(i64::from_datum(datum, false).unwrap())
            }
            pg_sys::MONEYOID => {
                // money is 8 bytes
                ConstValue::Money(i64::from_datum(datum, false).unwrap())
            }
            pg_sys::CHAROID => {
                ConstValue::Char(i8::from_datum(datum, false).unwrap() as u8)
            }
            pg_sys::OIDOID => {
                ConstValue::Oid(pg_sys::Oid::from_datum(datum, false).unwrap().to_u32())
            }
            pg_sys::UUIDOID => {
                let uuid = pgrx::Uuid::from_datum(datum, false).unwrap();
                ConstValue::Uuid(*uuid.as_bytes())
            }
            pg_sys::PG_LSNOID => {
                // LSN is stored as u64, but we can use i64::from_datum
                ConstValue::Lsn(i64::from_datum(datum, false).unwrap() as u64)
            }
            _ => {
                // Fallback for unknown types: treat as 64-bit value if possible
                ConstValue::Int64(datum.value() as i64)
            }
        }
    };

    Ok(ScalarExpr::Const { type_oid, typmod, collation, value, is_null })
}

unsafe fn convert_op_expr(
    op: *mut pg_sys::OpExpr,
    col_map: &ColumnMapping,
) -> Result<ScalarExpr, InboundError> {
    let op_oid = (*op).opno.to_u32();
    let return_type = (*op).opresulttype.to_u32();
    let args_list = (*op).args;
    let arg_ptrs = list_iter::<pg_sys::Node>(args_list);
    let mut args = Vec::new();
    for arg_ptr in &arg_ptrs {
        args.push(convert_scalar(*arg_ptr as *mut pg_sys::Node, col_map)?);
    }
    Ok(ScalarExpr::OpExpr { op_oid, return_type, args })
}

unsafe fn convert_func_expr(
    fe: *mut pg_sys::FuncExpr,
    col_map: &ColumnMapping,
) -> Result<ScalarExpr, InboundError> {
    let func_oid = (*fe).funcid.to_u32();
    let return_type = (*fe).funcresulttype.to_u32();
    let func_variadic = (*fe).funcvariadic;
    let args_list = (*fe).args;
    let arg_ptrs = list_iter::<pg_sys::Node>(args_list);
    let mut args = Vec::new();
    for arg_ptr in &arg_ptrs {
        args.push(convert_scalar(*arg_ptr as *mut pg_sys::Node, col_map)?);
    }
    Ok(ScalarExpr::FuncExpr { func_oid, return_type, args, func_variadic })
}

unsafe fn convert_bool_expr(
    be: *mut pg_sys::BoolExpr,
    col_map: &ColumnMapping,
) -> Result<ScalarExpr, InboundError> {
    let bool_type = match (*be).boolop {
        pg_sys::BoolExprType::AND_EXPR => BoolExprType::And,
        pg_sys::BoolExprType::OR_EXPR => BoolExprType::Or,
        pg_sys::BoolExprType::NOT_EXPR => BoolExprType::Not,
        _ => return Err(InboundError::UnsupportedFeature("unknown BoolExprType".into())),
    };
    let arg_ptrs = list_iter::<pg_sys::Node>((*be).args);
    let mut args = Vec::new();
    for arg_ptr in &arg_ptrs {
        args.push(convert_scalar(*arg_ptr as *mut pg_sys::Node, col_map)?);
    }
    Ok(ScalarExpr::BoolExpr { bool_type, args })
}

unsafe fn convert_null_test(
    nt: *mut pg_sys::NullTest,
    col_map: &ColumnMapping,
) -> Result<ScalarExpr, InboundError> {
    let null_test_type = match (*nt).nulltesttype {
        pg_sys::NullTestType::IS_NULL => NullTestType::IsNull,
        pg_sys::NullTestType::IS_NOT_NULL => NullTestType::IsNotNull,
        _ => return Err(InboundError::UnsupportedFeature("unknown NullTestType".into())),
    };
    let arg = convert_scalar((*nt).arg as *mut pg_sys::Node, col_map)?;
    Ok(ScalarExpr::NullTest { arg: Box::new(arg), null_test_type })
}

unsafe fn convert_aggref(
    agg: *mut pg_sys::Aggref,
    col_map: &ColumnMapping,
) -> Result<ScalarExpr, InboundError> {
    let agg_func_oid = (*agg).aggfnoid.to_u32();
    let result_type = (*agg).aggtype.to_u32();
    let distinct = (*agg).aggdistinct != std::ptr::null_mut();

    let arg_ptrs = list_iter::<pg_sys::TargetEntry>((*agg).args);
    let mut args = Vec::new();
    for te_ptr in &arg_ptrs {
        let te = *te_ptr;
        args.push(convert_scalar((*te).expr as *mut pg_sys::Node, col_map)?);
    }

    let filter = if !(*agg).aggfilter.is_null() {
        Some(Box::new(convert_scalar((*agg).aggfilter as *mut pg_sys::Node, col_map)?))
    } else {
        None
    };

    Ok(ScalarExpr::AggRef(AggExpr { agg_func_oid, args, distinct, filter, result_type }))
}

unsafe fn convert_scalar_array_op(
    sao: *mut pg_sys::ScalarArrayOpExpr,
    col_map: &ColumnMapping,
) -> Result<ScalarExpr, InboundError> {
    let op_oid = (*sao).opno.to_u32();
    let use_or = (*sao).useOr;
    let arg_ptrs = list_iter::<pg_sys::Node>((*sao).args);
    let mut args: Vec<_> = Vec::new();
    for arg_ptr in &arg_ptrs {
        args.push(convert_scalar(*arg_ptr as *mut pg_sys::Node, col_map)?);
    }
    if args.len() < 2 {
        return Err(InboundError::TranslationError(
            "ScalarArrayOpExpr needs 2 args".into()
        ));
    }
    let array = Box::new(args.remove(1));
    let scalar = Box::new(args.remove(0));
    Ok(ScalarExpr::ScalarArrayOp { op_oid, use_or, scalar, array })
}

unsafe fn convert_window_func(
    wf: *mut pg_sys::WindowFunc,
    col_map: &ColumnMapping,
) -> Result<ScalarExpr, InboundError> {
    let winfnoid = (*wf).winfnoid.to_u32();
    let wintype = (*wf).wintype.to_u32();
    let wincollid = (*wf).wincollid.to_u32();
    let inputcollid = (*wf).inputcollid.to_u32();
    let winref = (*wf).winref;
    let winstar = (*wf).winstar;
    let winagg = (*wf).winagg;

    let arg_nodes = crate::utils::pg_list::list_iter::<pg_sys::Node>((*wf).args);
    let mut args = Vec::new();
    for arg_node in &arg_nodes {
        args.push(convert_scalar(*arg_node, col_map)?);
    }

    Ok(ScalarExpr::WindowFunc {
        winfnoid,
        wintype,
        wincollid,
        inputcollid,
        args,
        winref,
        winstar,
        winagg,
    })
}
