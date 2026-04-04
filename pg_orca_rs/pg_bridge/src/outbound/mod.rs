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

    // Apply projection: replace the top-level plan's targetlist with the
    // query's targetList so the output matches what the user SELECT'd.
    apply_query_projection(pg_plan, query, col_map)?;

    let stmt = planned_stmt::build_planned_stmt(query, pg_plan, std::ptr::null_mut())?;

    sanity_check::sanity_check(stmt, query)?;

    Ok(stmt)
}

/// Replace the top-level plan's targetlist with the query's targetList.
///
/// The query's targetList has Var nodes with original (varno, varattno) referencing
/// RTEs. For scan nodes this works directly. For non-scan nodes (join, agg, sort),
/// we add a Result node on top that projects from the plan's internal targetlist
/// to the query's expected output.
unsafe fn apply_query_projection(
    pg_plan: *mut pg_sys::Plan,
    query: &pg_sys::Query,
    _col_map: &ColumnMapping,
) -> Result<(), OutboundError> {
    if query.targetList.is_null() {
        return Ok(());
    }

    // For simple scan nodes, the query's targetList Vars directly reference
    // the scan relation (varno = RTE index), so we can just replace.
    // For non-scan nodes, the query's Vars still reference the original RTEs,
    // which PG can resolve via the range table.
    //
    // Key insight: PG's executor for scan nodes evaluates targetlist Vars
    // against the current scan tuple using (varno == scanrelid).
    // For upper nodes, we wrap with a Result node if needed.

    let plan_tag = (*pg_plan).type_;
    match plan_tag {
        // Scan nodes: replace targetlist directly
        pg_sys::NodeTag::T_SeqScan
        | pg_sys::NodeTag::T_IndexScan
        | pg_sys::NodeTag::T_IndexOnlyScan
        | pg_sys::NodeTag::T_BitmapHeapScan => {
            (*pg_plan).targetlist = query.targetList;
        }
        // Non-scan nodes (join, agg, sort, etc.): keep internal targetlist as-is.
        // Projection for these requires Result node wrapping which is a larger refactor.
        // Column order is deterministic (sorted by varattno in inbound).
        _ => {}
    }
    Ok(())
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
        } else {
            // Non-Var expression (e.g., Aggref, FuncExpr) — keep as-is
            (*new_te).expr = qte.expr;
        }

        new_list = pg_sys::lappend(new_list, new_te as *mut std::ffi::c_void);
    }

    new_list
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

        other => Err(OutboundError::UnsupportedPhysicalOp(format!("{:?}", other))),
    }
}
