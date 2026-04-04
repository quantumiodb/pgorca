use pgrx::pg_sys;

use optimizer_core::ir::physical::{ScanDirection, AggStrategy};
use optimizer_core::ir::types::{Cost, JoinType, SortKey, AggExpr, ColumnId};
use optimizer_core::ir::physical::MergeClauseInfo;
use optimizer_core::ir::scalar::ScalarExpr;
use crate::inbound::column_mapping::ColumnMapping;
use crate::utils::palloc::palloc_node;
use super::expr_builders::{build_scalar_expr, build_qual_list};
use super::OutboundError;

/// Look up the equality operator OID for a PG type using the type cache.
unsafe fn get_eq_operator(typid: pg_sys::Oid) -> pg_sys::Oid {
    let cache = pg_sys::lookup_type_cache(typid, pg_sys::TYPECACHE_EQ_OPR as i32);
    if cache.is_null() { pg_sys::Oid::from(0u32) } else { (*cache).eq_opr }
}

/// Look up the less-than operator OID for a PG type.
unsafe fn get_lt_operator(typid: pg_sys::Oid) -> pg_sys::Oid {
    let cache = pg_sys::lookup_type_cache(typid, pg_sys::TYPECACHE_LT_OPR as i32);
    if cache.is_null() { pg_sys::Oid::from(0u32) } else { (*cache).lt_opr }
}

/// Look up the btree opfamily for a PG type.
unsafe fn get_btree_opfamily(typid: pg_sys::Oid) -> pg_sys::Oid {
    let cache = pg_sys::lookup_type_cache(typid, pg_sys::TYPECACHE_BTREE_OPFAMILY as i32);
    if cache.is_null() { pg_sys::Oid::from(0u32) } else { (*cache).btree_opf }
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn pg_join_type(jt: JoinType) -> u32 {
    match jt {
        JoinType::Inner => pg_sys::JoinType::JOIN_INNER,
        JoinType::Left => pg_sys::JoinType::JOIN_LEFT,
        JoinType::Right => pg_sys::JoinType::JOIN_RIGHT,
        JoinType::Full => pg_sys::JoinType::JOIN_FULL,
        JoinType::Semi => pg_sys::JoinType::JOIN_SEMI,
        JoinType::AntiSemi => pg_sys::JoinType::JOIN_ANTI,
    }
}

unsafe fn set_plan_fields(
    plan: *mut pg_sys::Plan,
    target_list: *mut pg_sys::List,
    qual: *mut pg_sys::List,
    left: *mut pg_sys::Plan,
    right: *mut pg_sys::Plan,
    cost: &Cost,
    rows: f64,
    width: i32,
) {
    (*plan).targetlist = target_list;
    (*plan).qual = qual;
    (*plan).lefttree = left;
    (*plan).righttree = right;
    (*plan).startup_cost = cost.startup;
    (*plan).total_cost = cost.total;
    (*plan).plan_rows = rows;
    (*plan).plan_width = width;
    (*plan).parallel_aware = false;
    (*plan).parallel_safe = false;
}

// ── SeqScan ───────────────────────────────────────────────────────────────────

pub unsafe fn build_seq_scan(
    scanrelid: u32,
    target_list: *mut pg_sys::List,
    qual: *mut pg_sys::List,
    rows: f64,
    cost: &Cost,
    width: i32,
) -> Result<*mut pg_sys::Plan, OutboundError> {
    let scan = palloc_node::<pg_sys::SeqScan>(pg_sys::NodeTag::T_SeqScan);
    (*scan).scan.scanrelid = scanrelid;
    set_plan_fields(
        &mut (*scan).scan.plan,
        target_list, qual,
        std::ptr::null_mut(), std::ptr::null_mut(),
        cost, rows, width,
    );
    Ok(&mut (*scan).scan.plan as *mut pg_sys::Plan)
}

// ── IndexScan ─────────────────────────────────────────────────────────────────

pub unsafe fn build_index_scan(
    scanrelid: u32,
    index_oid: u32,
    scan_direction: ScanDirection,
    index_quals: &[ScalarExpr],
    heap_quals: &[ScalarExpr],
    target_list: *mut pg_sys::List,
    col_map: &ColumnMapping,
    rows: f64,
    cost: &Cost,
    width: i32,
) -> Result<*mut pg_sys::Plan, OutboundError> {
    let idx_scan = palloc_node::<pg_sys::IndexScan>(pg_sys::NodeTag::T_IndexScan);
    (*idx_scan).scan.scanrelid = scanrelid;
    (*idx_scan).indexid = pg_sys::Oid::from(index_oid);

    let pg_direction = match scan_direction {
        ScanDirection::Forward | ScanDirection::NoMovement => pg_sys::ScanDirection::ForwardScanDirection,
        ScanDirection::Backward => pg_sys::ScanDirection::BackwardScanDirection,
    };
    (*idx_scan).indexorderdir = pg_direction;

    // Build index qual list
    let iq_list = build_qual_list(index_quals, col_map)?;
    (*idx_scan).indexqual = iq_list;
    (*idx_scan).indexqualorig = iq_list; // simplified: same as indexqual

    // heap recheck qual
    let hq_list = build_qual_list(heap_quals, col_map)?;

    set_plan_fields(
        &mut (*idx_scan).scan.plan,
        target_list, hq_list,
        std::ptr::null_mut(), std::ptr::null_mut(),
        cost, rows, width,
    );
    Ok(&mut (*idx_scan).scan.plan as *mut pg_sys::Plan)
}

// ── HashJoin ──────────────────────────────────────────────────────────────────

pub unsafe fn build_hash_join(
    join_type: JoinType,
    outer_plan: *mut pg_sys::Plan,
    inner_plan: *mut pg_sys::Plan,
    hash_clauses: &[(ScalarExpr, ScalarExpr)],
    join_qual: &[ScalarExpr],
    target_list: *mut pg_sys::List,
    col_map: &ColumnMapping,
    rows: f64,
    cost: &Cost,
    width: i32,
) -> Result<*mut pg_sys::Plan, OutboundError> {
    // Build Hash node wrapping inner plan
    let hash_node = build_hash_node(inner_plan, rows)?;

    // Build hash clause list — OpExpr nodes with OUTER_VAR / INNER_VAR
    let hcl = build_hash_clause_list(hash_clauses, col_map, outer_plan, inner_plan)?;

    let join_qual_list = build_join_qual_list(join_qual, col_map, outer_plan, inner_plan)?;

    let hj = palloc_node::<pg_sys::HashJoin>(pg_sys::NodeTag::T_HashJoin);
    (*hj).join.jointype = pg_join_type(join_type);
    (*hj).hashclauses = hcl;
    // Build hashoperators and hashcollations OID lists
    let mut hash_ops_list: *mut pg_sys::List = std::ptr::null_mut();
    let mut hash_colls_list: *mut pg_sys::List = std::ptr::null_mut();
    for (left_expr, _) in hash_clauses {
        if let ScalarExpr::ColumnRef(cid) = left_expr {
            let type_oid = col_map.get_column_ref(*cid)
                .map(|cr| pg_sys::Oid::from(cr.pg_vartype))
                .unwrap_or(pg_sys::Oid::from(0u32));
            hash_ops_list = pg_sys::lappend_oid(hash_ops_list, get_eq_operator(type_oid));
        } else {
            hash_ops_list = pg_sys::lappend_oid(hash_ops_list, pg_sys::Oid::from(0u32));
        }
        hash_colls_list = pg_sys::lappend_oid(hash_colls_list, pg_sys::Oid::from(0u32));
    }
    (*hj).hashoperators = hash_ops_list;
    (*hj).hashcollations = hash_colls_list;
    (*hj).join.joinqual = join_qual_list;
    (*hj).join.inner_unique = false;

    set_plan_fields(
        &mut (*hj).join.plan,
        target_list, std::ptr::null_mut(),
        outer_plan, hash_node as *mut pg_sys::Plan,
        cost, rows, width,
    );
    Ok(&mut (*hj).join.plan as *mut pg_sys::Plan)
}

unsafe fn build_hash_node(
    inner_plan: *mut pg_sys::Plan,
    rows: f64,
) -> Result<*mut pg_sys::Hash, OutboundError> {
    let hash = palloc_node::<pg_sys::Hash>(pg_sys::NodeTag::T_Hash);
    (*hash).plan.targetlist = (*inner_plan).targetlist;
    (*hash).plan.lefttree = inner_plan;
    (*hash).plan.righttree = std::ptr::null_mut();
    (*hash).plan.startup_cost = (*inner_plan).startup_cost;
    (*hash).plan.total_cost = (*inner_plan).total_cost;
    (*hash).plan.plan_rows = rows;
    (*hash).plan.plan_width = (*inner_plan).plan_width;
    (*hash).skewTable = pg_sys::Oid::from(0u32);
    (*hash).skewColumn = 0;
    (*hash).skewInherit = false;
    (*hash).rows_total = 0.0;
    Ok(hash)
}

unsafe fn build_hash_clause_list(
    hash_clauses: &[(ScalarExpr, ScalarExpr)],
    col_map: &ColumnMapping,
    outer_plan: *mut pg_sys::Plan,
    inner_plan: *mut pg_sys::Plan,
) -> Result<*mut pg_sys::List, OutboundError> {
    let mut list: *mut pg_sys::List = std::ptr::null_mut();
    for (left_expr, right_expr) in hash_clauses {
        let left = build_scalar_expr_for_join(left_expr, col_map, outer_plan, true)?;
        let right = build_scalar_expr_for_join(right_expr, col_map, inner_plan, false)?;

        // Build an OpExpr for the equality — look up proper eq operator for the type
        let left_type = if !left.is_null() && (*left).type_ == pg_sys::NodeTag::T_Var {
            (*(left as *const pg_sys::Var)).vartype
        } else {
            pg_sys::Oid::from(0u32)
        };
        let eq_op = get_eq_operator(left_type);
        let op_node = palloc_node::<pg_sys::OpExpr>(pg_sys::NodeTag::T_OpExpr);
        (*op_node).opno = eq_op;
        (*op_node).opfuncid = pg_sys::get_opcode(eq_op);
        (*op_node).opresulttype = pg_sys::Oid::from(16u32); // bool
        (*op_node).opretset = false;
        (*op_node).opcollid = pg_sys::Oid::from(0u32);
        (*op_node).inputcollid = pg_sys::Oid::from(0u32);
        (*op_node).location = -1;

        let mut arg_list: *mut pg_sys::List = std::ptr::null_mut();
        arg_list = pg_sys::lappend(arg_list, left as *mut std::ffi::c_void);
        arg_list = pg_sys::lappend(arg_list, right as *mut std::ffi::c_void);
        (*op_node).args = arg_list;

        list = pg_sys::lappend(list, op_node as *mut std::ffi::c_void);
    }
    Ok(list)
}

/// Build a join qual list, remapping Vars to OUTER_VAR/INNER_VAR.
pub unsafe fn build_join_qual_list(
    predicates: &[ScalarExpr],
    col_map: &ColumnMapping,
    outer_plan: *mut pg_sys::Plan,
    inner_plan: *mut pg_sys::Plan,
) -> Result<*mut pg_sys::List, OutboundError> {
    let mut list: *mut pg_sys::List = std::ptr::null_mut();
    for pred in predicates {
        let expr = build_scalar_expr_for_join_tree(pred, col_map, outer_plan, inner_plan)?;
        list = pg_sys::lappend(list, expr as *mut std::ffi::c_void);
    }
    Ok(list)
}

/// Recursively build a scalar expression, remapping ColumnRef Vars to OUTER_VAR or INNER_VAR
/// based on which child plan contains the column.
unsafe fn build_scalar_expr_for_join_tree(
    expr: &ScalarExpr,
    col_map: &ColumnMapping,
    outer_plan: *mut pg_sys::Plan,
    inner_plan: *mut pg_sys::Plan,
) -> Result<*mut pg_sys::Expr, OutboundError> {
    use super::expr_builders::{OUTER_VAR, INNER_VAR};

    match expr {
        ScalarExpr::ColumnRef(col_id) => {
            let col_ref = col_map.get_column_ref(*col_id)
                .ok_or_else(|| OutboundError::VarMappingError(
                    format!("join col {:?} not found", col_id)
                ))?;

            // Check if it's in the outer plan
            if let Some(attno) = find_col_position_in_plan(col_ref.pg_varno, col_ref.pg_varattno, outer_plan) {
                use super::expr_builders::build_var_with_varno;
                return Ok(build_var_with_varno(col_ref, OUTER_VAR, attno) as *mut pg_sys::Expr);
            }
            // Check inner plan
            if let Some(attno) = find_col_position_in_plan(col_ref.pg_varno, col_ref.pg_varattno, inner_plan) {
                use super::expr_builders::build_var_with_varno;
                return Ok(build_var_with_varno(col_ref, INNER_VAR, attno) as *mut pg_sys::Expr);
            }
            // Fallback: use original
            use super::expr_builders::build_var;
            Ok(build_var(col_ref) as *mut pg_sys::Expr)
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
                let arg_expr = build_scalar_expr_for_join_tree(arg, col_map, outer_plan, inner_plan)?;
                arg_list = pg_sys::lappend(arg_list, arg_expr as *mut std::ffi::c_void);
            }
            (*op_node).args = arg_list;
            Ok(op_node as *mut pg_sys::Expr)
        }
        ScalarExpr::BoolExpr { bool_type, args } => {
            use optimizer_core::ir::scalar::BoolExprType;
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
                let arg_expr = build_scalar_expr_for_join_tree(arg, col_map, outer_plan, inner_plan)?;
                arg_list = pg_sys::lappend(arg_list, arg_expr as *mut std::ffi::c_void);
            }
            (*bool_node).args = arg_list;
            Ok(bool_node as *mut pg_sys::Expr)
        }
        // Delegate other types to the standard builder
        other => build_scalar_expr(other, col_map),
    }
}

/// Build a scalar expr remapping ColumnRef Vars to OUTER_VAR (is_outer=true) or INNER_VAR.
unsafe fn build_scalar_expr_for_join(
    expr: &ScalarExpr,
    col_map: &ColumnMapping,
    plan: *mut pg_sys::Plan,
    is_outer: bool,
) -> Result<*mut pg_sys::Expr, OutboundError> {
    use super::expr_builders::{OUTER_VAR, INNER_VAR};

    match expr {
        ScalarExpr::ColumnRef(col_id) => {
            let col_ref = col_map.get_column_ref(*col_id)
                .ok_or_else(|| OutboundError::VarMappingError(
                    format!("col {:?} not found", col_id)
                ))?;

            // Find which position this column has in the plan's target list
            let varno = if is_outer { OUTER_VAR } else { INNER_VAR };
            let attno = find_col_position_in_plan(col_ref.pg_varno, col_ref.pg_varattno, plan)
                .unwrap_or(col_ref.pg_varattno);

            use super::expr_builders::build_var_with_varno;
            Ok(build_var_with_varno(col_ref, varno, attno) as *mut pg_sys::Expr)
        }
        other => build_scalar_expr(other, col_map),
    }
}

/// Find the 1-based position of (orig_varno, orig_varattno) in a plan's target list.
unsafe fn find_col_position_in_plan(
    varno: u32,
    varattno: i16,
    plan: *mut pg_sys::Plan,
) -> Option<i16> {
    use crate::utils::pg_list::list_iter;
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

// ── NestLoop ─────────────────────────────────────────────────────────────────

pub unsafe fn build_nestloop(
    join_type: JoinType,
    outer_plan: *mut pg_sys::Plan,
    inner_plan: *mut pg_sys::Plan,
    join_qual: &[ScalarExpr],
    target_list: *mut pg_sys::List,
    col_map: &ColumnMapping,
    rows: f64,
    cost: &Cost,
    width: i32,
) -> Result<*mut pg_sys::Plan, OutboundError> {
    let join_qual_list = build_join_qual_list(join_qual, col_map, outer_plan, inner_plan)?;

    let nl = palloc_node::<pg_sys::NestLoop>(pg_sys::NodeTag::T_NestLoop);
    (*nl).join.jointype = pg_join_type(join_type);
    (*nl).join.joinqual = join_qual_list;
    (*nl).join.inner_unique = false;
    (*nl).nestParams = std::ptr::null_mut();

    set_plan_fields(
        &mut (*nl).join.plan,
        target_list, std::ptr::null_mut(),
        outer_plan, inner_plan,
        cost, rows, width,
    );
    Ok(&mut (*nl).join.plan as *mut pg_sys::Plan)
}

// ── MergeJoin ─────────────────────────────────────────────────────────────────

pub unsafe fn build_merge_join(
    join_type: JoinType,
    outer_plan: *mut pg_sys::Plan,
    inner_plan: *mut pg_sys::Plan,
    merge_clauses: &[MergeClauseInfo],
    join_qual: &[ScalarExpr],
    target_list: *mut pg_sys::List,
    col_map: &ColumnMapping,
    rows: f64,
    cost: &Cost,
    width: i32,
) -> Result<*mut pg_sys::Plan, OutboundError> {
    let join_qual_list = build_join_qual_list(join_qual, col_map, outer_plan, inner_plan)?;
    let mc_list = build_merge_clause_list(merge_clauses, col_map, outer_plan, inner_plan)?;

    let n = merge_clauses.len();
    let mj = palloc_node::<pg_sys::MergeJoin>(pg_sys::NodeTag::T_MergeJoin);
    (*mj).join.jointype = pg_join_type(join_type);
    (*mj).join.joinqual = join_qual_list;
    (*mj).join.inner_unique = false;
    (*mj).mergeclauses = mc_list;
    (*mj).skip_mark_restore = false;

    // Allocate per-clause arrays
    let merge_families = pg_sys::palloc0(n * std::mem::size_of::<pg_sys::Oid>()) as *mut pg_sys::Oid;
    let merge_collations = pg_sys::palloc0(n * std::mem::size_of::<pg_sys::Oid>()) as *mut pg_sys::Oid;
    let merge_strategies = pg_sys::palloc0(n * std::mem::size_of::<i32>()) as *mut i32;
    let merge_nulls_first = pg_sys::palloc0(n * std::mem::size_of::<bool>()) as *mut bool;
    for (i, mc) in merge_clauses.iter().enumerate() {
        // Look up btree opfamily from the left key type
        let left_typid = match &mc.left_key {
            ScalarExpr::ColumnRef(cid) => col_map.get_column_ref(*cid)
                .map(|cr| pg_sys::Oid::from(cr.pg_vartype))
                .unwrap_or(pg_sys::Oid::from(23u32)), // default int4
            _ => pg_sys::Oid::from(23u32),
        };
        *merge_families.add(i) = get_btree_opfamily(left_typid);
        *merge_collations.add(i) = pg_sys::Oid::from(mc.collation);
        *merge_strategies.add(i) = 1; // BTLessStrategyNumber
        *merge_nulls_first.add(i) = mc.nulls_first;
    }
    (*mj).mergeFamilies = merge_families;
    (*mj).mergeCollations = merge_collations;
    (*mj).mergeStrategies = merge_strategies;
    (*mj).mergeNullsFirst = merge_nulls_first;

    set_plan_fields(
        &mut (*mj).join.plan,
        target_list, std::ptr::null_mut(),
        outer_plan, inner_plan,
        cost, rows, width,
    );
    Ok(&mut (*mj).join.plan as *mut pg_sys::Plan)
}

unsafe fn build_merge_clause_list(
    merge_clauses: &[MergeClauseInfo],
    col_map: &ColumnMapping,
    outer_plan: *mut pg_sys::Plan,
    inner_plan: *mut pg_sys::Plan,
) -> Result<*mut pg_sys::List, OutboundError> {
    let mut list: *mut pg_sys::List = std::ptr::null_mut();
    for mc in merge_clauses {
        let left = build_scalar_expr_for_join(&mc.left_key, col_map, outer_plan, true)?;
        let right = build_scalar_expr_for_join(&mc.right_key, col_map, inner_plan, false)?;

        let merge_opno = pg_sys::Oid::from(mc.merge_op);
        let op_node = palloc_node::<pg_sys::OpExpr>(pg_sys::NodeTag::T_OpExpr);
        (*op_node).opno = merge_opno;
        (*op_node).opfuncid = pg_sys::get_opcode(merge_opno);
        (*op_node).opresulttype = pg_sys::Oid::from(16u32);
        (*op_node).opretset = false;
        (*op_node).opcollid = pg_sys::Oid::from(mc.collation);
        (*op_node).inputcollid = pg_sys::Oid::from(mc.collation);
        (*op_node).location = -1;

        let mut arg_list: *mut pg_sys::List = std::ptr::null_mut();
        arg_list = pg_sys::lappend(arg_list, left as *mut std::ffi::c_void);
        arg_list = pg_sys::lappend(arg_list, right as *mut std::ffi::c_void);
        (*op_node).args = arg_list;

        list = pg_sys::lappend(list, op_node as *mut std::ffi::c_void);
    }
    Ok(list)
}

// ── Sort ─────────────────────────────────────────────────────────────────────

pub unsafe fn build_sort(
    keys: &[SortKey],
    child_plan: *mut pg_sys::Plan,
    target_list: *mut pg_sys::List,
    rows: f64,
    cost: &Cost,
    width: i32,
) -> Result<*mut pg_sys::Plan, OutboundError> {
    let n = keys.len();
    let sort = palloc_node::<pg_sys::Sort>(pg_sys::NodeTag::T_Sort);
    (*sort).numCols = n as i32;

    // Allocate arrays for sort keys
    let sort_col_idx = pg_sys::palloc0(n * std::mem::size_of::<pg_sys::AttrNumber>()) as *mut pg_sys::AttrNumber;
    let sort_operators = pg_sys::palloc0(n * std::mem::size_of::<pg_sys::Oid>()) as *mut pg_sys::Oid;
    let collations = pg_sys::palloc0(n * std::mem::size_of::<pg_sys::Oid>()) as *mut pg_sys::Oid;
    let nulls_first = pg_sys::palloc0(n * std::mem::size_of::<bool>()) as *mut bool;

    for (i, key) in keys.iter().enumerate() {
        // Find the attribute number in target list by position (simplified: use key position)
        *sort_col_idx.add(i) = (i + 1) as pg_sys::AttrNumber;
        *sort_operators.add(i) = pg_sys::Oid::from(key.sort_op_oid);
        *collations.add(i) = pg_sys::Oid::from(key.collation_oid);
        *nulls_first.add(i) = key.nulls_first;
    }

    (*sort).sortColIdx = sort_col_idx;
    (*sort).sortOperators = sort_operators;
    (*sort).collations = collations;
    (*sort).nullsFirst = nulls_first;

    set_plan_fields(
        &mut (*sort).plan,
        target_list, std::ptr::null_mut(),
        child_plan, std::ptr::null_mut(),
        cost, rows, width,
    );
    Ok(&mut (*sort).plan as *mut pg_sys::Plan)
}

// ── Limit ─────────────────────────────────────────────────────────────────────

pub unsafe fn build_limit(
    offset_expr: Option<&ScalarExpr>,
    count_expr: Option<&ScalarExpr>,
    child_plan: *mut pg_sys::Plan,
    target_list: *mut pg_sys::List,
    col_map: &ColumnMapping,
    rows: f64,
    cost: &Cost,
    width: i32,
) -> Result<*mut pg_sys::Plan, OutboundError> {
    let lim = palloc_node::<pg_sys::Limit>(pg_sys::NodeTag::T_Limit);

    (*lim).limitOffset = if let Some(off) = offset_expr {
        build_scalar_expr(off, col_map)? as *mut pg_sys::Node
    } else {
        std::ptr::null_mut()
    };

    (*lim).limitCount = if let Some(cnt) = count_expr {
        build_scalar_expr(cnt, col_map)? as *mut pg_sys::Node
    } else {
        std::ptr::null_mut()
    };

    (*lim).limitOption = pg_sys::LimitOption::LIMIT_OPTION_COUNT;

    set_plan_fields(
        &mut (*lim).plan,
        target_list, std::ptr::null_mut(),
        child_plan, std::ptr::null_mut(),
        cost, rows, width,
    );
    Ok(&mut (*lim).plan as *mut pg_sys::Plan)
}

// ── Agg ──────────────────────────────────────────────────────────────────────

pub unsafe fn build_agg(
    strategy: AggStrategy,
    group_by_cols: &[ColumnId],
    aggregates: &[AggExpr],
    child_plan: *mut pg_sys::Plan,
    target_list: *mut pg_sys::List,
    qual: *mut pg_sys::List,
    col_map: &ColumnMapping,
    rows: f64,
    cost: &Cost,
    width: i32,
) -> Result<*mut pg_sys::Plan, OutboundError> {
    let agg_strat = match strategy {
        AggStrategy::Plain => pg_sys::AggStrategy::AGG_PLAIN,
        AggStrategy::Sorted => pg_sys::AggStrategy::AGG_SORTED,
        AggStrategy::Hashed => pg_sys::AggStrategy::AGG_HASHED,
        AggStrategy::Mixed => pg_sys::AggStrategy::AGG_MIXED,
    };

    let n_group_cols = group_by_cols.len();
    let agg_node = palloc_node::<pg_sys::Agg>(pg_sys::NodeTag::T_Agg);
    (*agg_node).aggstrategy = agg_strat;
    (*agg_node).aggsplit = pg_sys::AggSplit::AGGSPLIT_SIMPLE;
    (*agg_node).numCols = n_group_cols as i32;
    (*agg_node).numGroups = rows as i64;
    (*agg_node).transitionSpace = 0;
    (*agg_node).aggParams = std::ptr::null_mut();
    (*agg_node).groupingSets = std::ptr::null_mut();
    (*agg_node).chain = std::ptr::null_mut();

    // Build grpColIdx array — look up proper equality operators for each column type
    if n_group_cols > 0 {
        let grp_col_idx = pg_sys::palloc0(n_group_cols * std::mem::size_of::<pg_sys::AttrNumber>())
            as *mut pg_sys::AttrNumber;
        let grp_ops = pg_sys::palloc0(n_group_cols * std::mem::size_of::<pg_sys::Oid>())
            as *mut pg_sys::Oid;
        let grp_coll = pg_sys::palloc0(n_group_cols * std::mem::size_of::<pg_sys::Oid>())
            as *mut pg_sys::Oid;
        for (i, col_id) in group_by_cols.iter().enumerate() {
            *grp_col_idx.add(i) = (i + 1) as pg_sys::AttrNumber;
            // Look up the type of this column to get the proper equality operator
            let type_oid = col_map.get_column_ref(*col_id)
                .map(|cr| pg_sys::Oid::from(cr.pg_vartype))
                .unwrap_or(pg_sys::Oid::from(0u32));
            *grp_ops.add(i) = get_eq_operator(type_oid);
            let collation = col_map.get_column_ref(*col_id)
                .map(|cr| pg_sys::Oid::from(cr.pg_varcollid))
                .unwrap_or(pg_sys::Oid::from(0u32));
            *grp_coll.add(i) = collation;
        }
        (*agg_node).grpColIdx = grp_col_idx;
        (*agg_node).grpOperators = grp_ops;
        (*agg_node).grpCollations = grp_coll;
    }

    set_plan_fields(
        &mut (*agg_node).plan,
        target_list, qual,
        child_plan, std::ptr::null_mut(),
        cost, rows, width,
    );
    Ok(&mut (*agg_node).plan as *mut pg_sys::Plan)
}

// ── IndexOnlyScan ─────────────────────────────────────────────────────────────

pub unsafe fn build_index_only_scan(
    scanrelid: u32,
    index_oid: u32,
    index_quals: &[ScalarExpr],
    target_list: *mut pg_sys::List,
    col_map: &ColumnMapping,
    rows: f64,
    cost: &Cost,
    width: i32,
) -> Result<*mut pg_sys::Plan, OutboundError> {
    let idx_scan = palloc_node::<pg_sys::IndexOnlyScan>(pg_sys::NodeTag::T_IndexOnlyScan);
    (*idx_scan).scan.scanrelid = scanrelid;
    (*idx_scan).indexid = pg_sys::Oid::from(index_oid);
    (*idx_scan).indexorderdir = pg_sys::ScanDirection::ForwardScanDirection;

    let iq_list = build_qual_list(index_quals, col_map)?;
    (*idx_scan).indexqual = iq_list;
    (*idx_scan).recheckqual = std::ptr::null_mut();

    set_plan_fields(
        &mut (*idx_scan).scan.plan,
        target_list, std::ptr::null_mut(),
        std::ptr::null_mut(), std::ptr::null_mut(),
        cost, rows, width,
    );
    Ok(&mut (*idx_scan).scan.plan as *mut pg_sys::Plan)
}

// ── BitmapHeapScan ────────────────────────────────────────────────────────────

pub unsafe fn build_bitmap_heap_scan(
    scanrelid: u32,
    index_oid: u32,
    index_quals: &[ScalarExpr],
    heap_quals: &[ScalarExpr],
    target_list: *mut pg_sys::List,
    col_map: &ColumnMapping,
    rows: f64,
    cost: &Cost,
    width: i32,
) -> Result<*mut pg_sys::Plan, OutboundError> {
    // Build the BitmapIndexScan child first
    let bis = palloc_node::<pg_sys::BitmapIndexScan>(pg_sys::NodeTag::T_BitmapIndexScan);
    (*bis).scan.scanrelid = scanrelid;
    (*bis).indexid = pg_sys::Oid::from(index_oid);
    let iq_list = build_qual_list(index_quals, col_map)?;
    (*bis).indexqual = iq_list;
    (*bis).indexqualorig = iq_list;
    // BitmapIndexScan has no output targetlist; set minimal plan fields
    (*bis).scan.plan.startup_cost = 0.0;
    (*bis).scan.plan.total_cost = cost.startup; // startup cost = BIS cost
    (*bis).scan.plan.plan_rows = rows;
    (*bis).scan.plan.plan_width = 0;

    // Build the BitmapHeapScan
    let bhs = palloc_node::<pg_sys::BitmapHeapScan>(pg_sys::NodeTag::T_BitmapHeapScan);
    (*bhs).scan.scanrelid = scanrelid;
    let hq_list = build_qual_list(heap_quals, col_map)?;
    (*bhs).bitmapqualorig = hq_list;

    set_plan_fields(
        &mut (*bhs).scan.plan,
        target_list, hq_list,
        &mut (*bis).scan.plan as *mut pg_sys::Plan, std::ptr::null_mut(),
        cost, rows, width,
    );
    Ok(&mut (*bhs).scan.plan as *mut pg_sys::Plan)
}

// ── Unique ────────────────────────────────────────────────────────────────────

pub unsafe fn build_unique(
    num_cols: usize,
    col_ids: &[ColumnId],
    child_plan: *mut pg_sys::Plan,
    target_list: *mut pg_sys::List,
    col_map: &ColumnMapping,
    rows: f64,
    cost: &Cost,
    width: i32,
) -> Result<*mut pg_sys::Plan, OutboundError> {
    let uniq = palloc_node::<pg_sys::Unique>(pg_sys::NodeTag::T_Unique);
    (*uniq).numCols = num_cols as i32;

    if num_cols > 0 {
        let col_idx = pg_sys::palloc0(num_cols * std::mem::size_of::<pg_sys::AttrNumber>())
            as *mut pg_sys::AttrNumber;
        let ops = pg_sys::palloc0(num_cols * std::mem::size_of::<pg_sys::Oid>())
            as *mut pg_sys::Oid;
        let coll = pg_sys::palloc0(num_cols * std::mem::size_of::<pg_sys::Oid>())
            as *mut pg_sys::Oid;
        for i in 0..num_cols {
            *col_idx.add(i) = (i + 1) as pg_sys::AttrNumber;
            let type_oid = col_ids.get(i)
                .and_then(|cid| col_map.get_column_ref(*cid))
                .map(|cr| pg_sys::Oid::from(cr.pg_vartype))
                .unwrap_or(pg_sys::Oid::from(0u32));
            *ops.add(i) = get_eq_operator(type_oid);
            let collation = col_ids.get(i)
                .and_then(|cid| col_map.get_column_ref(*cid))
                .map(|cr| pg_sys::Oid::from(cr.pg_varcollid))
                .unwrap_or(pg_sys::Oid::from(0u32));
            *coll.add(i) = collation;
        }
        (*uniq).uniqColIdx = col_idx;
        (*uniq).uniqOperators = ops;
        (*uniq).uniqCollations = coll;
    }

    set_plan_fields(
        &mut (*uniq).plan,
        target_list, std::ptr::null_mut(),
        child_plan, std::ptr::null_mut(),
        cost, rows, width,
    );
    Ok(&mut (*uniq).plan as *mut pg_sys::Plan)
}
