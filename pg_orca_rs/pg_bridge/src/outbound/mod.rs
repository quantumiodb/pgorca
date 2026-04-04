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

    let stmt = planned_stmt::build_planned_stmt(query, pg_plan, std::ptr::null_mut())?;

    sanity_check::sanity_check(stmt, query)?;

    Ok(stmt)
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
