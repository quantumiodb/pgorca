use pgrx::pg_sys;
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

    let value = if is_null {
        ConstValue::Null
    } else {
        let datum = (*c).constvalue;
        // Decode based on type OID
        match type_oid {
            16 => ConstValue::Bool(datum.value() != 0), // bool
            21 => ConstValue::Int16(datum.value() as i16), // int2
            23 => ConstValue::Int32(datum.value() as i32), // int4
            20 => ConstValue::Int64(datum.value() as i64), // int8
            700 => ConstValue::Float32(f32::from_bits(datum.value() as u32)), // float4
            701 => ConstValue::Float64(f64::from_bits(datum.value() as u64)), // float8
            25 | 1043 => {
                // text / varchar — varlena datum
                let ptr = datum.cast_mut_ptr::<u8>();
                if ptr.is_null() {
                    ConstValue::Text(String::new())
                } else {
                    // skip 4-byte varlena header
                    let header = *(ptr as *const u32);
                    let len = (header >> 2) as usize;
                    let data = std::slice::from_raw_parts(ptr.add(4), len.saturating_sub(4));
                    ConstValue::Text(String::from_utf8_lossy(data).into_owned())
                }
            }
            _ => ConstValue::Int64(datum.value() as i64), // best-effort for unknown types
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
