pub mod expr_builders;
pub mod plan_builders;
pub mod planned_stmt;
pub mod sanity_check;

use pgrx::pg_sys;
use optimizer_core::ir::physical::PhysicalOp;
use optimizer_core::plan::extract::PhysicalPlan;
use crate::inbound::column_mapping::ColumnMapping;

#[derive(Debug)]
pub enum OutboundError {
    PlanBuildError(String),
    VarMappingError(String),
    SanityCheckFailed(String),
    UnsupportedPhysicalOp(String),
}

impl std::fmt::Display for OutboundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PlanBuildError(s) => write!(f, "plan build: {}", s),
            Self::VarMappingError(s) => write!(f, "var mapping: {}", s),
            Self::SanityCheckFailed(s) => write!(f, "sanity check: {}", s),
            Self::UnsupportedPhysicalOp(s) => write!(f, "unsupported physical op: {}", s),
        }
    }
}

/// Convert a PhysicalPlan to a PG PlannedStmt.
pub unsafe fn generate_planned_stmt(
    plan: &PhysicalPlan,
    query: &pg_sys::Query,
    col_map: &ColumnMapping,
) -> Result<*mut pg_sys::PlannedStmt, OutboundError> {
    let pg_plan = build_plan_node(plan, col_map)?;

    // Apply projection: ensure the top-level plan's output matches the
    // query's targetList. May wrap with a Result node for pass-through plans.
    let pg_plan = apply_query_projection(pg_plan, query, col_map)?;

    let stmt = planned_stmt::build_planned_stmt(query, pg_plan, std::ptr::null_mut())?;

    sanity_check::sanity_check(stmt, query)?;

    Ok(stmt)
}

/// Returns true if the plan node is a pass-through node that doesn't
/// evaluate its targetlist (Sort, Limit, Unique, LockRows, etc.).
/// These nodes just pass child tuples through unchanged.
unsafe fn is_passthrough_node(plan: *mut pg_sys::Plan) -> bool {
    matches!(
        (*plan).type_,
        pg_sys::NodeTag::T_Sort
        | pg_sys::NodeTag::T_Limit
        | pg_sys::NodeTag::T_Unique
        | pg_sys::NodeTag::T_LockRows
    )
}

unsafe fn is_scan_node(plan: *mut pg_sys::Plan) -> bool {
    matches!(
        (*plan).type_,
        pg_sys::NodeTag::T_SeqScan
        | pg_sys::NodeTag::T_IndexScan
        | pg_sys::NodeTag::T_IndexOnlyScan
        | pg_sys::NodeTag::T_BitmapHeapScan
    )
}

unsafe fn is_join_node(plan: *mut pg_sys::Plan) -> bool {
    matches!(
        (*plan).type_,
        pg_sys::NodeTag::T_HashJoin
        | pg_sys::NodeTag::T_MergeJoin
        | pg_sys::NodeTag::T_NestLoop
    )
}

/// Ensure the top-level plan's output matches the query's targetList.
///
/// For scan nodes, we replace the targetlist directly.
/// For join nodes, we rewrite Vars to OUTER_VAR/INNER_VAR.
/// For pass-through nodes (Sort, Limit, Unique) on top of a join, we apply
/// the join projection on the join node, then propagate up through the
/// pass-through chain so their targetlists stay in sync with their child.
/// For other upper nodes (Agg, WindowAgg), we rewrite to OUTER_VAR + position.
unsafe fn apply_query_projection(
    pg_plan: *mut pg_sys::Plan,
    query: &pg_sys::Query,
    _col_map: &ColumnMapping,
) -> Result<*mut pg_sys::Plan, OutboundError> {
    if query.targetList.is_null() {
        return Ok(pg_plan);
    }

    let plan_tag = (*pg_plan).type_;
    match plan_tag {
        // Scan nodes: replace targetlist directly.
        pg_sys::NodeTag::T_SeqScan
        | pg_sys::NodeTag::T_IndexScan
        | pg_sys::NodeTag::T_IndexOnlyScan
        | pg_sys::NodeTag::T_BitmapHeapScan => {
            (*pg_plan).targetlist = query.targetList;
            Ok(pg_plan)
        }
        // Append: keep internal targetlist (built from first child).
        pg_sys::NodeTag::T_Append => Ok(pg_plan),
        // Agg: keep internal targetlist (built by build_agg_target_list with
        // OUTER_VAR group-by Vars and Aggref nodes matched to Agg internal state).
        pg_sys::NodeTag::T_Agg => Ok(pg_plan),
        // WindowAgg: rewrite Vars to reference child plan output.
        pg_sys::NodeTag::T_WindowAgg => {
            let proj_tl = rewrite_tl_for_projection(query.targetList, pg_plan);
            (*pg_plan).targetlist = proj_tl;
            Ok(pg_plan)
        }
        // Join nodes: rewrite to OUTER_VAR/INNER_VAR preserving which side.
        pg_sys::NodeTag::T_HashJoin
        | pg_sys::NodeTag::T_MergeJoin
        | pg_sys::NodeTag::T_NestLoop => {
            let proj_tl = rewrite_tl_for_join_projection(query.targetList, pg_plan);
            (*pg_plan).targetlist = proj_tl;
            Ok(pg_plan)
        }
        // Pass-through nodes on top of a join: apply join projection at
        // the join level, then propagate the updated targetlist up.
        // Pass-through nodes don't evaluate their own targetlist, so their
        // targetlist must match their child's exactly.
        _ if is_passthrough_node(pg_plan) => {
            // Walk down through pass-through chain to find the child.
            let mut bottom = pg_plan;
            while is_passthrough_node(bottom) && !(*bottom).lefttree.is_null() {
                bottom = (*bottom).lefttree;
            }
            if is_join_node(bottom) {
                // Apply join projection at the join level.
                let proj_tl = rewrite_tl_for_join_projection(query.targetList, bottom);
                (*bottom).targetlist = proj_tl;
            } else if is_scan_node(bottom) {
                // Scan node: replace with query's targetlist directly.
                (*bottom).targetlist = query.targetList;
            }
            // For Agg, Append, WindowAgg etc., keep their internal targetlist.
            // Propagate bottom's targetlist up through all pass-through nodes
            // (bottom-up so each node sees the updated child targetlist).
            let bottom_tl = (*bottom).targetlist;
            let mut cur = pg_plan;
            while !cur.is_null() {
                (*cur).targetlist = bottom_tl;
                if cur == bottom { break; }
                cur = (*cur).lefttree;
            }
            Ok(pg_plan)
        }
        // Other upper nodes (Agg, HashAggregate): rewrite to
        // OUTER_VAR + position in child's output.
        _ => {
            let proj_tl = rewrite_tl_for_projection(query.targetList, pg_plan);
            (*pg_plan).targetlist = proj_tl;
            Ok(pg_plan)
        }
    }
}

/// Rewrite a query target list for a join plan node.
///
/// For each Var in the query's targetList, find the matching entry in the join's
/// internal targetlist (by varnosyn/varattnosyn) and create a new Var that
/// references the SAME (varno, varattno) as the matched entry. This preserves
/// OUTER_VAR/INNER_VAR correctly.
///
/// Non-Var expressions (Aggref, Cast, etc.) are kept as-is.
unsafe fn rewrite_tl_for_join_projection(
    query_tl: *mut pg_sys::List,
    plan: *mut pg_sys::Plan,
) -> *mut pg_sys::List {
    use crate::utils::pg_list::list_iter;
    use expr_builders::{OUTER_VAR, INNER_VAR};

    let plan_tes = list_iter::<pg_sys::TargetEntry>((*plan).targetlist);
    let query_tes = list_iter::<pg_sys::TargetEntry>(query_tl);

    let mut new_list: *mut pg_sys::List = std::ptr::null_mut();

    for (resno_0, qte_ptr) in query_tes.iter().enumerate() {
        let qte = &**qte_ptr;
        let new_te = crate::utils::palloc::palloc_node::<pg_sys::TargetEntry>(
            pg_sys::NodeTag::T_TargetEntry,
        );
        (*new_te).resno = (resno_0 + 1) as i16;
        (*new_te).resname = qte.resname;
        (*new_te).resjunk = qte.resjunk;
        (*new_te).ressortgroupref = qte.ressortgroupref;
        (*new_te).resorigtbl = qte.resorigtbl;
        (*new_te).resorigcol = qte.resorigcol;

        if !qte.expr.is_null() && (*qte.expr).type_ == pg_sys::NodeTag::T_Var {
            let orig_var = qte.expr as *const pg_sys::Var;
            let orig_varno = (*orig_var).varno as u32;
            let orig_attno = (*orig_var).varattno;

            // Find matching entry in plan's internal targetlist
            let mut found = false;
            for (_i, pte_ptr) in plan_tes.iter().enumerate() {
                let pte = &**pte_ptr;
                if !pte.expr.is_null() && (*pte.expr).type_ == pg_sys::NodeTag::T_Var {
                    let pv = pte.expr as *const pg_sys::Var;
                    // Match by original (varnosyn, varattnosyn) or direct (varno, varattno)
                    let matches = ((*pv).varnosyn == orig_varno && (*pv).varattnosyn == orig_attno)
                        || ((*pv).varno as u32 == orig_varno && (*pv).varattno == orig_attno);
                    if matches {
                        // Create a Var with the same varno/varattno as the plan entry
                        let new_var = crate::utils::palloc::palloc_node::<pg_sys::Var>(
                            pg_sys::NodeTag::T_Var,
                        );
                        (*new_var).varno = (*pv).varno;
                        (*new_var).varattno = (*pv).varattno;
                        (*new_var).vartype = (*orig_var).vartype;
                        (*new_var).vartypmod = (*orig_var).vartypmod;
                        (*new_var).varcollid = (*orig_var).varcollid;
                        (*new_var).varnosyn = (*orig_var).varnosyn;
                        (*new_var).varattnosyn = (*orig_var).varattnosyn;
                        (*new_var).location = (*orig_var).location;
                        (*new_te).expr = new_var as *mut pg_sys::Expr;
                        found = true;
                        break;
                    }
                }
            }
            if !found {
                // Fallback: keep original expression
                (*new_te).expr = qte.expr;
            }
        } else {
            // Non-Var expression (Aggref, FuncExpr, etc.) — keep as-is
            (*new_te).expr = qte.expr;
        }

        new_list = pg_sys::lappend(new_list, new_te as *mut std::ffi::c_void);
    }

    new_list
}

/// Rewrite a query target list so Vars reference OUTER_VAR + position in child plan.
unsafe fn rewrite_tl_for_projection(
    query_tl: *mut pg_sys::List,
    child_plan: *mut pg_sys::Plan,
) -> *mut pg_sys::List {
    use crate::utils::pg_list::list_iter;
    use expr_builders::{OUTER_VAR, INNER_VAR};

    let child_tes = list_iter::<pg_sys::TargetEntry>((*child_plan).targetlist);
    let query_tes = list_iter::<pg_sys::TargetEntry>(query_tl);

    let mut new_list: *mut pg_sys::List = std::ptr::null_mut();

    for (resno_0, qte_ptr) in query_tes.iter().enumerate() {
        let qte = &**qte_ptr;
        let new_te = crate::utils::palloc::palloc_node::<pg_sys::TargetEntry>(
            pg_sys::NodeTag::T_TargetEntry,
        );
        (*new_te).resno = (resno_0 + 1) as i16;
        (*new_te).resname = qte.resname;
        (*new_te).resjunk = qte.resjunk;
        (*new_te).ressortgroupref = qte.ressortgroupref;
        (*new_te).resorigtbl = qte.resorigtbl;
        (*new_te).resorigcol = qte.resorigcol;

        // If the expression is a Var, remap it to point into the child plan's output
        if !qte.expr.is_null() && (*qte.expr).type_ == pg_sys::NodeTag::T_Var {
            let orig_var = qte.expr as *const pg_sys::Var;
            let orig_varno = (*orig_var).varno as u32;
            let orig_attno = (*orig_var).varattno;

            // Find this column's position in the child plan's target list
            let mut found_pos: Option<i16> = None;
            for (i, cte_ptr) in child_tes.iter().enumerate() {
                let cte = &**cte_ptr;
                if !cte.expr.is_null() && (*cte.expr).type_ == pg_sys::NodeTag::T_Var {
                    let cv = cte.expr as *const pg_sys::Var;
                    // Match by original varno+varattno, or by OUTER_VAR attno
                    let cv_varno = (*cv).varno;
                    let cv_attno = (*cv).varattno;
                    if (cv_varno as u32 == orig_varno && cv_attno == orig_attno)
                        || ((cv_varno == OUTER_VAR || cv_varno == INNER_VAR)
                            && (*cv).varnosyn == orig_varno
                            && (*cv).varattnosyn == orig_attno)
                    {
                        found_pos = Some((i + 1) as i16);
                        break;
                    }
                }
            }

            if let Some(pos) = found_pos {
                // Create new Var pointing to OUTER_VAR + position in child
                let new_var = crate::utils::palloc::palloc_node::<pg_sys::Var>(
                    pg_sys::NodeTag::T_Var,
                );
                (*new_var).varno = OUTER_VAR;
                (*new_var).varattno = pos;
                (*new_var).vartype = (*orig_var).vartype;
                (*new_var).vartypmod = (*orig_var).vartypmod;
                (*new_var).varcollid = (*orig_var).varcollid;
                (*new_var).varnosyn = (*orig_var).varnosyn;
                (*new_var).varattnosyn = (*orig_var).varattnosyn;
                (*new_var).location = (*orig_var).location;
                (*new_te).expr = new_var as *mut pg_sys::Expr;
            } else {
                // Fallback: use original expression (may work for scan nodes)
                (*new_te).expr = qte.expr;
            }
        } else if !qte.expr.is_null() && (*qte.expr).type_ == pg_sys::NodeTag::T_WindowFunc {
            // WindowFunc: need to remap Var nodes inside args to OUTER_VAR
            // WindowFunc: need to remap Var nodes inside args to OUTER_VAR
            // Copy the WindowFunc and remap its arg Vars
            let wf = qte.expr as *mut pg_sys::WindowFunc;
            let new_wf = crate::utils::palloc::palloc_node::<pg_sys::WindowFunc>(
                pg_sys::NodeTag::T_WindowFunc,
            );
            (*new_wf).winfnoid = (*wf).winfnoid;
            (*new_wf).wintype = (*wf).wintype;
            (*new_wf).wincollid = (*wf).wincollid;
            (*new_wf).inputcollid = (*wf).inputcollid;
            (*new_wf).winref = (*wf).winref;
            (*new_wf).winstar = (*wf).winstar;
            (*new_wf).winagg = (*wf).winagg;
            (*new_wf).location = (*wf).location;
            (*new_wf).aggfilter = (*wf).aggfilter;
            (*new_wf).runCondition = std::ptr::null_mut();
            (*new_wf).args = remap_vars_in_list((*wf).args, &child_tes);
            (*new_te).expr = new_wf as *mut pg_sys::Expr;
        } else {
            // Non-Var expression (e.g., Aggref, FuncExpr) — keep as-is
            (*new_te).expr = qte.expr;
        }

        new_list = pg_sys::lappend(new_list, new_te as *mut std::ffi::c_void);
    }

    new_list
}

/// Remap Var nodes in a List to use OUTER_VAR, matching against child target entries.
unsafe fn remap_vars_in_list(
    list: *mut pg_sys::List,
    child_tes: &[*mut pg_sys::TargetEntry],
) -> *mut pg_sys::List {
    use crate::utils::pg_list::list_iter;
    use expr_builders::{OUTER_VAR, INNER_VAR};

    if list.is_null() {
        return std::ptr::null_mut();
    }
    let items = list_iter::<pg_sys::Node>(list);
    let mut new_list: *mut pg_sys::List = std::ptr::null_mut();
    for item_ptr in &items {
        let item = *item_ptr;
        if !item.is_null() && (*item).type_ == pg_sys::NodeTag::T_TargetEntry {
            // WindowFunc args are wrapped in TargetEntry
            let te = item as *mut pg_sys::TargetEntry;
            let new_te = crate::utils::palloc::palloc_node::<pg_sys::TargetEntry>(
                pg_sys::NodeTag::T_TargetEntry,
            );
            std::ptr::copy_nonoverlapping(te, new_te, 1);
            if !(*te).expr.is_null() && (*(*te).expr).type_ == pg_sys::NodeTag::T_Var {
                (*new_te).expr = remap_single_var((*te).expr as *mut pg_sys::Var, child_tes);
            }
            new_list = pg_sys::lappend(new_list, new_te as *mut std::ffi::c_void);
        } else if !item.is_null() && (*item).type_ == pg_sys::NodeTag::T_Var {
            let new_var = remap_single_var(item as *mut pg_sys::Var, child_tes);
            new_list = pg_sys::lappend(new_list, new_var as *mut std::ffi::c_void);
        } else {
            new_list = pg_sys::lappend(new_list, item as *mut std::ffi::c_void);
        }
    }
    new_list
}

unsafe fn remap_single_var(
    var: *mut pg_sys::Var,
    child_tes: &[*mut pg_sys::TargetEntry],
) -> *mut pg_sys::Expr {
    use expr_builders::{OUTER_VAR, INNER_VAR};

    let orig_varno = (*var).varno as u32;
    let orig_attno = (*var).varattno;

    for (i, cte_ptr) in child_tes.iter().enumerate() {
        let cte = &**cte_ptr;
        if !cte.expr.is_null() && (*cte.expr).type_ == pg_sys::NodeTag::T_Var {
            let cv = cte.expr as *const pg_sys::Var;
            if ((*cv).varno as u32 == orig_varno && (*cv).varattno == orig_attno)
                || (((*cv).varno == OUTER_VAR || (*cv).varno == INNER_VAR)
                    && (*cv).varnosyn == orig_varno
                    && (*cv).varattnosyn == orig_attno)
            {
                let new_var = crate::utils::palloc::palloc_node::<pg_sys::Var>(
                    pg_sys::NodeTag::T_Var,
                );
                (*new_var).varno = OUTER_VAR;
                (*new_var).varattno = (i + 1) as i16;
                (*new_var).vartype = (*var).vartype;
                (*new_var).vartypmod = (*var).vartypmod;
                (*new_var).varcollid = (*var).varcollid;
                (*new_var).varnosyn = (*var).varnosyn;
                (*new_var).varattnosyn = (*var).varattnosyn;
                (*new_var).location = (*var).location;
                return new_var as *mut pg_sys::Expr;
            }
        }
    }
    // Not found — return original
    var as *mut pg_sys::Expr
}

unsafe fn build_plan_node(
    plan: &PhysicalPlan,
    col_map: &ColumnMapping,
) -> Result<*mut pg_sys::Plan, OutboundError> {
    use expr_builders::{build_target_list_for_scan, build_agg_target_list, build_qual_list, build_join_target_list};
    use plan_builders::*;

    match &plan.op {
        PhysicalOp::SeqScan { scanrelid } => {
            let target_list = build_target_list_for_scan(plan, col_map)?;
            let qual = build_qual_list(&plan.qual, col_map)?;
            build_seq_scan(
                *scanrelid,
                target_list,
                qual,
                plan.rows,
                &plan.cost,
                plan.width as i32,
            )
        }

        PhysicalOp::IndexScan { scanrelid, index_oid, scan_direction, index_quals, .. } => {
            let target_list = build_target_list_for_scan(plan, col_map)?;
            build_index_scan(
                *scanrelid,
                *index_oid,
                *scan_direction,
                index_quals,
                &plan.qual,
                target_list,
                col_map,
                plan.rows,
                &plan.cost,
                plan.width as i32,
            )
        }

        PhysicalOp::IndexOnlyScan { scanrelid, index_oid, index_quals } => {
            let target_list = build_target_list_for_scan(plan, col_map)?;
            build_index_only_scan(
                *scanrelid,
                *index_oid,
                index_quals,
                target_list,
                col_map,
                plan.rows,
                &plan.cost,
                plan.width as i32,
            )
        }

        PhysicalOp::BitmapHeapScan { scanrelid, index_oid, index_quals } => {
            let target_list = build_target_list_for_scan(plan, col_map)?;
            build_bitmap_heap_scan(
                *scanrelid,
                *index_oid,
                index_quals,
                &plan.qual,
                target_list,
                col_map,
                plan.rows,
                &plan.cost,
                plan.width as i32,
            )
        }

        PhysicalOp::HashJoin { join_type, hash_clauses } => {
            if plan.children.len() < 2 {
                return Err(OutboundError::PlanBuildError("HashJoin needs 2 children".into()));
            }
            let outer_plan = build_plan_node(&plan.children[0], col_map)?;
            let inner_plan = build_plan_node(&plan.children[1], col_map)?;
            let target_list = build_join_target_list(outer_plan, inner_plan);
            build_hash_join(
                *join_type,
                outer_plan,
                inner_plan,
                hash_clauses,
                &plan.qual,
                target_list,
                col_map,
                plan.rows,
                &plan.cost,
                plan.width as i32,
            )
        }

        PhysicalOp::NestLoop { join_type } => {
            if plan.children.len() < 2 {
                return Err(OutboundError::PlanBuildError("NestLoop needs 2 children".into()));
            }
            let outer_plan = build_plan_node(&plan.children[0], col_map)?;
            let inner_plan = build_plan_node(&plan.children[1], col_map)?;
            let target_list = build_join_target_list(outer_plan, inner_plan);
            build_nestloop(
                *join_type,
                outer_plan,
                inner_plan,
                &plan.qual,
                target_list,
                col_map,
                plan.rows,
                &plan.cost,
                plan.width as i32,
            )
        }

        PhysicalOp::MergeJoin { join_type, merge_clauses } => {
            if plan.children.len() < 2 {
                return Err(OutboundError::PlanBuildError("MergeJoin needs 2 children".into()));
            }
            let outer_plan = build_plan_node(&plan.children[0], col_map)?;
            let inner_plan = build_plan_node(&plan.children[1], col_map)?;
            let target_list = build_join_target_list(outer_plan, inner_plan);
            build_merge_join(
                *join_type,
                outer_plan,
                inner_plan,
                merge_clauses,
                &plan.qual,
                target_list,
                col_map,
                plan.rows,
                &plan.cost,
                plan.width as i32,
            )
        }

        PhysicalOp::Sort { keys } => {
            if plan.children.is_empty() {
                return Err(OutboundError::PlanBuildError("Sort needs a child".into()));
            }
            let child_plan = build_plan_node(&plan.children[0], col_map)?;
            let target_list = (*child_plan).targetlist; // inherit from child
            build_sort(
                keys,
                child_plan,
                target_list,
                plan.rows,
                &plan.cost,
                plan.width as i32,
            )
        }

        PhysicalOp::Limit { offset, count } => {
            if plan.children.is_empty() {
                return Err(OutboundError::PlanBuildError("Limit needs a child".into()));
            }
            let child_plan = build_plan_node(&plan.children[0], col_map)?;
            let target_list = (*child_plan).targetlist;
            build_limit(
                offset.as_ref(),
                count.as_ref(),
                child_plan,
                target_list,
                col_map,
                plan.rows,
                &plan.cost,
                plan.width as i32,
            )
        }

        PhysicalOp::Agg { strategy, group_by, aggregates } => {
            if plan.children.is_empty() {
                return Err(OutboundError::PlanBuildError("Agg needs a child".into()));
            }
            let child_plan = build_plan_node(&plan.children[0], col_map)?;
            // Agg target list: group-by Vars (OUTER_VAR) + Aggref nodes
            let target_list = build_agg_target_list(plan, group_by, aggregates, child_plan, col_map)?;
            let qual = build_qual_list(&plan.qual, col_map)?;
            build_agg(
                *strategy,
                group_by,
                aggregates,
                child_plan,
                target_list,
                qual,
                col_map,
                plan.rows,
                &plan.cost,
                plan.width as i32,
            )
        }

        PhysicalOp::Unique { num_cols } => {
            if plan.children.is_empty() {
                return Err(OutboundError::PlanBuildError("Unique needs a child".into()));
            }
            let child_plan = build_plan_node(&plan.children[0], col_map)?;
            let target_list = (*child_plan).targetlist;
            build_unique(
                *num_cols,
                &plan.output_columns,
                child_plan,
                target_list,
                col_map,
                plan.rows,
                &plan.cost,
                plan.width as i32,
            )
        }

        PhysicalOp::WindowAgg { clauses } => {
            if plan.children.is_empty() {
                return Err(OutboundError::PlanBuildError("WindowAgg needs a child".into()));
            }
            let child_plan = build_plan_node(&plan.children[0], col_map)?;
            let target_list = (*child_plan).targetlist;
            build_window_agg(clauses, child_plan, target_list, col_map, plan.rows, &plan.cost, plan.width as i32)
        }

        PhysicalOp::Append => {
            if plan.children.is_empty() {
                return Err(OutboundError::PlanBuildError("Append needs children".into()));
            }
            let mut child_plans = Vec::new();
            for child in &plan.children {
                let child_plan = build_plan_node(child, col_map)?;
                child_plans.push(child_plan);
            }
            // Use the first child's target list as a template
            let target_list = if !child_plans.is_empty() {
                (*child_plans[0]).targetlist
            } else {
                std::ptr::null_mut()
            };
            plan_builders::build_append(
                child_plans,
                target_list,
                plan.rows,
                &plan.cost,
                plan.width as i32,
            )
        }

        other => Err(OutboundError::UnsupportedPhysicalOp(format!("{:?}", other))),
    }
}
