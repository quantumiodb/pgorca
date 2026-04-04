use super::types::{ColumnId, AggExpr};

#[derive(Debug, Clone)]
pub enum ScalarExpr {
    ColumnRef(ColumnId),

    Const {
        type_oid: u32,
        typmod: i32,
        collation: u32,
        value: ConstValue,
        is_null: bool,
    },

    OpExpr {
        op_oid: u32,
        return_type: u32,
        args: Vec<ScalarExpr>,
    },

    FuncExpr {
        func_oid: u32,
        return_type: u32,
        args: Vec<ScalarExpr>,
        func_variadic: bool,
    },

    BoolExpr {
        bool_type: BoolExprType,
        args: Vec<ScalarExpr>,
    },

    NullTest {
        arg: Box<ScalarExpr>,
        null_test_type: NullTestType,
    },

    CaseExpr {
        arg: Option<Box<ScalarExpr>>,
        when_clauses: Vec<(ScalarExpr, ScalarExpr)>,
        default: Option<Box<ScalarExpr>>,
        result_type: u32,
    },

    Coalesce {
        args: Vec<ScalarExpr>,
        result_type: u32,
    },

    Cast {
        arg: Box<ScalarExpr>,
        target_type: u32,
        typmod: i32,
    },

    AggRef(AggExpr),

    Param {
        param_id: u32,
        param_type: u32,
    },

    ScalarArrayOp {
        op_oid: u32,
        use_or: bool,
        scalar: Box<ScalarExpr>,
        array: Box<ScalarExpr>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoolExprType { And, Or, Not }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullTestType { IsNull, IsNotNull }

#[derive(Debug, Clone)]
pub enum ConstValue {
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Text(String),
    Bytea(Vec<u8>),
    Null,
}
