use std::collections::HashMap;
use std::ffi::CStr;

use pgrx::pg_sys;

use optimizer_core::cost::stats::*;
use optimizer_core::ir::logical::{LogicalExpr, LogicalOp};
use optimizer_core::ir::scalar::ScalarExpr;
use optimizer_core::ir::types::{TableId, IndexAmType, SortKey, ColumnId, AggExpr, WindowClause};

use super::InboundError;
use super::column_mapping::ColumnMapping;
use super::query_check::is_supported_query;
use super::scalar_convert::convert_scalar;
use crate::utils::pg_list::{list_iter, list_iter_oid, list_length};

/// Result of Phase 1 conversion.
pub struct ConvertResult {
    pub logical_expr: LogicalExpr,
    pub catalog: CatalogSnapshot,
    pub col_map: ColumnMapping,
}

/// Convert a PG Query into our IR.
pub unsafe fn convert_query(query: &pg_sys::Query) -> Result<ConvertResult, InboundError> {
    // 1. Whitelist check
    is_supported_query(query)?;

    // 2. First pass: build ColumnMapping and CatalogSnapshot from all RTEs
    let mut col_map = ColumnMapping::new();
    let mut tables: HashMap<TableId, TableStats> = HashMap::new();
    let mut rte_to_table: HashMap<u32, TableId> = HashMap::new();
    let mut table_id_counter = 0u32;
    // Tracks partitioned parent rte_index → list of child rte_indexes
    let mut partition_children: HashMap<u32, Vec<u32>> = HashMap::new();

    let rtes = list_iter::<pg_sys::RangeTblEntry>(query.rtable);
    for (rte_index_0, rte_ptr) in rtes.iter().enumerate() {
        let rte = &**rte_ptr;
        let rte_index = (rte_index_0 + 1) as u32; // 1-based

        if rte.rtekind != pg_sys::RTEKind::RTE_RELATION {
            continue;
        }

        // Check if this is a partitioned table
        let relkind = rte.relkind as u8;
        if relkind == pg_sys::RELKIND_PARTITIONED_TABLE {
            // Expand partitioned table: get child partition OIDs
            let parent_rel = pg_sys::RelationIdGetRelation(rte.relid);
            if parent_rel.is_null() {
                return Err(InboundError::CatalogAccessError(
                    format!("cannot open partitioned relation OID {}", rte.relid.to_u32())
                ));
            }
            let pdesc = pg_sys::RelationGetPartitionDesc(parent_rel, false);
            if pdesc.is_null() || (*pdesc).nparts == 0 {
                pg_sys::RelationClose(parent_rel);
                return Err(InboundError::UnsupportedFeature(
                    "partitioned table with no partitions".into()
                ));
            }
            let nparts = (*pdesc).nparts as usize;
            let child_oids: Vec<pg_sys::Oid> = (0..nparts)
                .map(|i| *(*pdesc).oids.add(i))
                .collect();

            // Register columns for the parent so Vars referencing it can resolve.
            // The parent's columns will be aliased to each child partition below.
            table_id_counter += 1;
            let parent_table_id = TableId(table_id_counter);
            rte_to_table.insert(rte_index, parent_table_id);
            let parent_col_ids = register_relation_columns(
                parent_rel, rte.relid, rte_index, parent_table_id,
                &mut col_map, &mut tables,
            )?;
            pg_sys::RelationClose(parent_rel);

            let mut child_rte_indexes = Vec::new();
            for child_oid in &child_oids {
                // Acquire AccessShareLock on child partition so executor can open it with NoLock.
                // PG asserts that the caller holds a lock before table_open(NoLock) in ExecGetRangeTableRelation.
                pg_sys::LockRelationOid(*child_oid, pg_sys::AccessShareLock as i32);

                // Add a new RTE for each child partition into query.rtable
                let child_rte_index = add_child_rte(query as *const pg_sys::Query, *child_oid)?;

                table_id_counter += 1;
                let child_table_id = TableId(table_id_counter);
                rte_to_table.insert(child_rte_index, child_table_id);

                let child_rel = pg_sys::RelationIdGetRelation(*child_oid);
                if child_rel.is_null() {
                    return Err(InboundError::CatalogAccessError(
                        format!("cannot open child partition OID {}", child_oid.to_u32())
                    ));
                }
                register_relation_columns(
                    child_rel, *child_oid, child_rte_index, child_table_id,
                    &mut col_map, &mut tables,
                )?;
                // Register aliases: parent (rte_index, attno) → child column IDs
                // so that Vars in the query's targetList/quals referencing the parent
                // can also be resolved to child columns.
                let child_cols: Vec<ColumnId> = col_map.all_columns()
                    .filter(|cr| cr.pg_varno == child_rte_index)
                    .map(|cr| (cr.pg_varattno, cr.id))
                    .collect::<Vec<_>>()
                    .into_iter()
                    .map(|(_, id)| id)
                    .collect();
                // Map parent attno → child column id (columns match by position)
                for (parent_cid, child_cid) in parent_col_ids.iter().zip(child_cols.iter()) {
                    let parent_cr = col_map.get_column_ref(*parent_cid).unwrap();
                    let parent_attno = parent_cr.pg_varattno;
                    col_map.register_alias(child_rte_index, parent_attno, *child_cid);
                }

                pg_sys::RelationClose(child_rel);
                child_rte_indexes.push(child_rte_index);
            }
            partition_children.insert(rte_index, child_rte_indexes);
            continue;
        }

        // Regular (non-partitioned) table
        table_id_counter += 1;
        let table_id = TableId(table_id_counter);
        rte_to_table.insert(rte_index, table_id);

        let rel = pg_sys::RelationIdGetRelation(rte.relid);
        if rel.is_null() {
            return Err(InboundError::CatalogAccessError(
                format!("cannot open relation OID {}", rte.relid.to_u32())
            ));
        }
        register_relation_columns(
            rel, rte.relid, rte_index, table_id,
            &mut col_map, &mut tables,
        )?;
        pg_sys::RelationClose(rel);
    }

    // 2b. PG18: Register RTE_GROUP column aliases so Vars referencing the
    //     GROUP RTE resolve to the underlying relation columns.
    #[cfg(feature = "pg18")]
    for (idx, rte_ptr) in rtes.iter().enumerate() {
        let rte = &**rte_ptr;
        if rte.rtekind != pg_sys::RTEKind::RTE_GROUP { continue; }
        let group_varno = (idx + 1) as u32;
        let group_exprs = list_iter::<pg_sys::Node>(rte.groupexprs);
        for (attno_0, expr_ptr) in group_exprs.iter().enumerate() {
            let expr = *expr_ptr;
            if (*expr).type_ == pg_sys::NodeTag::T_Var {
                let var = expr as *mut pg_sys::Var;
                let orig_varno = (*var).varno as u32;
                let orig_attno = (*var).varattno;
                if let Some(cid) = col_map.lookup_var(orig_varno, orig_attno) {
                    col_map.register_alias(group_varno, (attno_0 + 1) as i16, cid);
                }
            }
        }
    }

    // 3. Read PG cost GUCs
    let cost_model = CostModel {
        seq_page_cost: pg_sys::seq_page_cost,
        random_page_cost: pg_sys::random_page_cost,
        cpu_tuple_cost: pg_sys::cpu_tuple_cost,
        cpu_index_tuple_cost: pg_sys::cpu_index_tuple_cost,
        cpu_operator_cost: pg_sys::cpu_operator_cost,
        effective_cache_size: pg_sys::effective_cache_size as f64,
        work_mem: pg_sys::work_mem as usize * 1024,
        damping_factor_filter: crate::ORCA_DAMPING_FILTER.get(),
        damping_factor_join: crate::ORCA_DAMPING_JOIN.get(),
        damping_factor_groupby: crate::ORCA_DAMPING_GROUPBY.get(),
    };

    let mut catalog = CatalogSnapshot { tables, rte_to_table, cost_model };

    // 4. Build logical expression tree
    let mut logical_expr = if !query.setOperations.is_null() {
        // UNION / UNION ALL: walk the SetOperationStmt tree
        translate_set_operations(
            query.setOperations as *mut pg_sys::Node,
            query,
            &mut col_map,
            &mut catalog,
        )?
    } else {
        // Normal query: build from jointree
        translate_from_expr(
            query.jointree, query, &col_map, &partition_children, &catalog,
        )?
    };

    // 5. Wrap with aggregation if needed (GROUP BY / aggregates)
    if query.hasAggs || list_length(query.groupClause) > 0 {
        logical_expr = translate_aggregate(query, logical_expr, &col_map)?;
    }

    // 6. Wrap with window function if present
    if query.hasWindowFuncs && list_length(query.windowClause) > 0 {
        logical_expr = translate_window(query, logical_expr, &col_map)?;
    }

    // 7. Wrap with sort if ORDER BY
    if list_length(query.sortClause) > 0 {
        logical_expr = translate_sort(query, logical_expr, &col_map)?;
    }

    // 7. Wrap with DISTINCT if needed
    if list_length(query.distinctClause) > 0 && !query.hasDistinctOn {
        logical_expr = translate_distinct(query, logical_expr, &col_map)?;
    }

    // 8. Wrap with LIMIT / OFFSET if present
    if !query.limitCount.is_null() || !query.limitOffset.is_null() {
        logical_expr = translate_limit(query, logical_expr, &col_map)?;
    }

    Ok(ConvertResult { logical_expr, catalog, col_map })
}

// ── UNION / set operation translation ────────────────────────────────────────

/// Recursively translate a SetOperationStmt tree into LogicalExpr.
///
/// PG represents UNION as a binary tree of SetOperationStmt nodes.
/// Leaf nodes are RangeTblRef pointing to RTE_SUBQUERY entries.
/// Each sub-query is converted recursively via convert_subquery.
unsafe fn translate_set_operations(
    node: *mut pg_sys::Node,
    parent_query: &pg_sys::Query,
    col_map: &mut ColumnMapping,
    catalog: &mut CatalogSnapshot,
) -> Result<LogicalExpr, InboundError> {
    if node.is_null() {
        return Err(InboundError::TranslationError("null set operation node".into()));
    }
    let tag = (*node).type_;
    match tag {
        pg_sys::NodeTag::T_RangeTblRef => {
            let rtref = node as *mut pg_sys::RangeTblRef;
            let rte_index = (*rtref).rtindex as u32;
            convert_union_arm(rte_index, parent_query, col_map, catalog)
        }
        pg_sys::NodeTag::T_SetOperationStmt => {
            let stmt = node as *mut pg_sys::SetOperationStmt;
            let left = translate_set_operations((*stmt).larg, parent_query, col_map, catalog)?;
            let right = translate_set_operations((*stmt).rarg, parent_query, col_map, catalog)?;

            let append = LogicalExpr {
                op: LogicalOp::Append,
                children: vec![left, right],
            };

            if (*stmt).all {
                // UNION ALL — just Append
                Ok(append)
            } else {
                // UNION (distinct) — Append + Sort + Distinct
                // Build sort keys and distinct columns from the output column types.
                // The UNION output has N columns matching colTypes list.
                let col_types = list_iter_oid((*stmt).colTypes);
                let num_cols = col_types.len();

                // We need synthetic column IDs for the UNION output.
                // Use the first arm's column IDs as the reference for sort/distinct.
                let first_arm_rte_index = get_first_leaf_rte_index((*stmt).larg);
                let mut union_col_ids = Vec::new();
                for attno in 1..=(num_cols as i16) {
                    if let Some(cid) = col_map.lookup_var(first_arm_rte_index, attno) {
                        union_col_ids.push(cid);
                    }
                }

                // Build sort keys for each column
                let mut sort_keys = Vec::new();
                for (i, &col_type_oid) in col_types.iter().enumerate() {
                    let sort_op = pg_sys::get_ordering_op_for_equality_op(
                        get_eq_operator_for_type(col_type_oid),
                        false, // nulls_first = false
                    );
                    if let Some(&cid) = union_col_ids.get(i) {
                        sort_keys.push(SortKey {
                            column: cid,
                            ascending: true,
                            nulls_first: false,
                            sort_op_oid: sort_op.to_u32(),
                            collation_oid: 0,
                        });
                    }
                }

                let sorted = LogicalExpr {
                    op: LogicalOp::Sort { keys: sort_keys },
                    children: vec![append],
                };

                Ok(LogicalExpr {
                    op: LogicalOp::Distinct { columns: union_col_ids },
                    children: vec![sorted],
                })
            }
        }
        _ => Err(InboundError::UnsupportedFeature(
            format!("unexpected set operation node {:?}", tag),
        )),
    }
}

/// Convert a single UNION arm (an RTE_SUBQUERY) into a LogicalExpr.
///
/// The sub-query's relation RTEs are copied into the parent query's rtable
/// so the executor can find them. The sub-query is then converted using these
/// new rte_indexes.
unsafe fn convert_union_arm(
    rte_index: u32,
    parent_query: &pg_sys::Query,
    col_map: &mut ColumnMapping,
    catalog: &mut CatalogSnapshot,
) -> Result<LogicalExpr, InboundError> {
    // Get the RTE for this sub-query
    let rtes = list_iter::<pg_sys::RangeTblEntry>(parent_query.rtable);
    let rte_ptr = rtes.get((rte_index - 1) as usize)
        .ok_or_else(|| InboundError::TranslationError(
            format!("RTE index {} out of range", rte_index)
        ))?;
    let rte = &**rte_ptr;

    if rte.rtekind != pg_sys::RTEKind::RTE_SUBQUERY {
        return Err(InboundError::TranslationError(
            format!("UNION arm RTE {} is not a subquery", rte_index)
        ));
    }

    let subquery = rte.subquery;
    if subquery.is_null() {
        return Err(InboundError::TranslationError(
            format!("UNION arm RTE {} has null subquery", rte_index)
        ));
    }

    // The sub-query has its own rtable. We need to copy each RTE_RELATION
    // from the sub-query into the parent's rtable, so the executor can find them.
    // Build a mapping: sub-query rte_index → parent rte_index.
    let sub_rtes = list_iter::<pg_sys::RangeTblEntry>((*subquery).rtable);
    let mut rte_remap: HashMap<u32, u32> = HashMap::new();

    for (sub_idx_0, sub_rte_ptr) in sub_rtes.iter().enumerate() {
        let sub_rte = &**sub_rte_ptr;
        let sub_rte_index = (sub_idx_0 + 1) as u32;

        if sub_rte.rtekind == pg_sys::RTEKind::RTE_RELATION {
            // Acquire lock so executor can open with NoLock
            pg_sys::LockRelationOid(sub_rte.relid, pg_sys::AccessShareLock as i32);

            // Reset perminfoindex: the sub-query's index is invalid in the
            // parent's rteperminfos. Zero means "skip permission check".
            let sub_rte_mut = *sub_rte_ptr as *mut pg_sys::RangeTblEntry;
            (*sub_rte_mut).perminfoindex = 0;

            // Copy this RTE into the parent query's rtable
            let query_mut = parent_query as *const pg_sys::Query as *mut pg_sys::Query;
            (*query_mut).rtable = pg_sys::lappend(
                (*query_mut).rtable,
                sub_rte_mut as *mut std::ffi::c_void,
            );
            let new_index = pg_sys::list_length((*query_mut).rtable) as u32;
            rte_remap.insert(sub_rte_index, new_index);
        }
    }

    // Now process each relation RTE in the sub-query using the new parent rte_indexes
    let mut table_id_counter = catalog.tables.len() as u32 + 1000 * rte_index;
    let mut children = Vec::new();

    for (sub_idx_0, sub_rte_ptr) in sub_rtes.iter().enumerate() {
        let sub_rte = &**sub_rte_ptr;
        let sub_rte_index = (sub_idx_0 + 1) as u32;

        if sub_rte.rtekind != pg_sys::RTEKind::RTE_RELATION {
            continue;
        }

        let parent_rte_index = *rte_remap.get(&sub_rte_index).unwrap();
        table_id_counter += 1;
        let table_id = TableId(table_id_counter);
        catalog.rte_to_table.insert(parent_rte_index, table_id);

        let rel = pg_sys::RelationIdGetRelation(sub_rte.relid);
        if rel.is_null() {
            return Err(InboundError::CatalogAccessError(
                format!("cannot open relation OID {}", sub_rte.relid.to_u32())
            ));
        }
        let col_ids = register_relation_columns(
            rel, sub_rte.relid, parent_rte_index, table_id,
            col_map, &mut catalog.tables,
        )?;
        pg_sys::RelationClose(rel);

        // Build a Get node for this relation
        children.push((parent_rte_index, table_id, col_ids));
    }

    // Build the logical expression from the sub-query
    // For simple UNION arms (single table, possibly with WHERE), we build
    // a scan + optional filter directly.
    if children.len() == 1 {
        let (parent_rte_index, table_id, col_ids) = &children[0];
        let mut expr = LogicalExpr {
            op: LogicalOp::Get {
                table_id: *table_id,
                columns: col_ids.clone(),
                rte_index: *parent_rte_index,
            },
            children: vec![],
        };

        // Apply the sub-query's WHERE clause if any
        if !(*subquery).jointree.is_null() && !(*(*subquery).jointree).quals.is_null() {
            // Remap Vars in the qual from sub-query rte_indexes to parent rte_indexes
            let qual_node = (*(*subquery).jointree).quals as *mut pg_sys::Node;
            remap_var_indexes(qual_node, &rte_remap);
            let predicate = convert_scalar(qual_node, col_map)?;
            expr = LogicalExpr {
                op: LogicalOp::Select { predicate },
                children: vec![expr],
            };
        }

        // Register aliases so parent query's Vars referencing this UNION arm
        // (rte_index, attno) resolve to the actual columns.
        let sub_tl = list_iter::<pg_sys::TargetEntry>((*subquery).targetList);
        for (attno_0, te_ptr) in sub_tl.iter().enumerate() {
            let te = &**te_ptr;
            if te.resjunk { continue; }
            let attno = (attno_0 + 1) as i16;

            if !te.expr.is_null() && (*te.expr).type_ == pg_sys::NodeTag::T_Var {
                let var = te.expr as *const pg_sys::Var;
                let orig_varno = (*var).varno as u32;
                let orig_attno = (*var).varattno;
                // Remap the sub-query var to parent rte_index
                let remapped_varno = rte_remap.get(&orig_varno).copied().unwrap_or(orig_varno);
                if let Some(cid) = col_map.lookup_var(remapped_varno, orig_attno) {
                    col_map.register_alias(rte_index, attno, cid);
                }
            }
        }

        Ok(expr)
    } else {
        // Multi-table sub-query (JOIN in UNION arm) — not yet supported
        Err(InboundError::UnsupportedFeature(
            "UNION arm with JOIN (multi-table)".into()
        ))
    }
}

/// Recursively remap Var.varno in a Node tree using the given mapping.
unsafe fn remap_var_indexes(node: *mut pg_sys::Node, remap: &HashMap<u32, u32>) {
    if node.is_null() { return; }
    match (*node).type_ {
        pg_sys::NodeTag::T_Var => {
            let var = node as *mut pg_sys::Var;
            let old_varno = (*var).varno as u32;
            if let Some(&new_varno) = remap.get(&old_varno) {
                (*var).varno = new_varno as i32;
                (*var).varnosyn = new_varno;
            }
        }
        pg_sys::NodeTag::T_OpExpr => {
            let op = node as *mut pg_sys::OpExpr;
            let args = list_iter::<pg_sys::Node>((*op).args);
            for arg in &args {
                remap_var_indexes(*arg, remap);
            }
        }
        pg_sys::NodeTag::T_BoolExpr => {
            let bool_expr = node as *mut pg_sys::BoolExpr;
            let args = list_iter::<pg_sys::Node>((*bool_expr).args);
            for arg in &args {
                remap_var_indexes(*arg, remap);
            }
        }
        pg_sys::NodeTag::T_FuncExpr => {
            let func = node as *mut pg_sys::FuncExpr;
            let args = list_iter::<pg_sys::Node>((*func).args);
            for arg in &args {
                remap_var_indexes(*arg, remap);
            }
        }
        _ => {} // Constants, etc. don't need remapping
    }
}

/// Get the RTE index of the leftmost leaf in a SetOperationStmt tree.
unsafe fn get_first_leaf_rte_index(node: *mut pg_sys::Node) -> u32 {
    if node.is_null() { return 0; }
    match (*node).type_ {
        pg_sys::NodeTag::T_RangeTblRef => {
            let rtref = node as *mut pg_sys::RangeTblRef;
            (*rtref).rtindex as u32
        }
        pg_sys::NodeTag::T_SetOperationStmt => {
            let stmt = node as *mut pg_sys::SetOperationStmt;
            get_first_leaf_rte_index((*stmt).larg)
        }
        _ => 0,
    }
}

/// Look up the equality operator for a type OID.
unsafe fn get_eq_operator_for_type(type_oid: pg_sys::Oid) -> pg_sys::Oid {
    let cache = pg_sys::lookup_type_cache(type_oid, pg_sys::TYPECACHE_EQ_OPR as i32);
    if cache.is_null() { pg_sys::Oid::from(0u32) } else { (*cache).eq_opr }
}

// ── FromExpr / JoinExpr translation ─────────────────────────────────────────

unsafe fn translate_from_expr(
    from_expr: *mut pg_sys::FromExpr,
    query: &pg_sys::Query,
    col_map: &ColumnMapping,
    partition_children: &HashMap<u32, Vec<u32>>,
    catalog: &CatalogSnapshot,
) -> Result<LogicalExpr, InboundError> {
    if from_expr.is_null() {
        return Err(InboundError::TranslationError("null FromExpr".into()));
    }
    let fromlist = (*from_expr).fromlist;
    let quals = (*from_expr).quals;

    // Build the join tree from the fromlist
    let items = list_iter::<pg_sys::Node>(fromlist);
    if items.is_empty() {
        return Err(InboundError::TranslationError("empty FROM clause".into()));
    }

    let mut expr = translate_from_item(items[0], query, col_map, partition_children, catalog)?;
    for item in &items[1..] {
        let right = translate_from_item(*item, query, col_map, partition_children, catalog)?;
        // Cross join — condition will be added from quals below
        expr = LogicalExpr {
            op: LogicalOp::Join {
                join_type: optimizer_core::ir::types::JoinType::Inner,
                predicate: ScalarExpr::BoolExpr {
                    bool_type: optimizer_core::ir::scalar::BoolExprType::And,
                    args: vec![],
                },
            },
            children: vec![expr, right],
        };
    }

    // Apply WHERE clause (jointree quals)
    if !quals.is_null() {
        let predicate = convert_scalar(quals as *mut pg_sys::Node, col_map)?;
        // If there's a join, merge qual into join; otherwise wrap with Select
        expr = match &expr.op {
            LogicalOp::Join { join_type, predicate: existing_pred } => {
                let merged = merge_predicates(existing_pred.clone(), predicate);
                LogicalExpr {
                    op: LogicalOp::Join {
                        join_type: *join_type,
                        predicate: merged,
                    },
                    children: expr.children,
                }
            }
            _ => LogicalExpr {
                op: LogicalOp::Select { predicate },
                children: vec![expr],
            },
        };
    }

    Ok(expr)
}

unsafe fn translate_from_item(
    node: *mut pg_sys::Node,
    query: &pg_sys::Query,
    col_map: &ColumnMapping,
    partition_children: &HashMap<u32, Vec<u32>>,
    catalog: &CatalogSnapshot,
) -> Result<LogicalExpr, InboundError> {
    if node.is_null() {
        return Err(InboundError::TranslationError("null from item".into()));
    }
    let tag = (*node).type_;
    match tag {
        pg_sys::NodeTag::T_RangeTblRef => {
            let rtref = node as *mut pg_sys::RangeTblRef;
            let rte_index = (*rtref).rtindex as u32;

            // If this RTE is a partitioned table, expand into Append(Get, Get, ...)
            if let Some(child_rte_indexes) = partition_children.get(&rte_index) {
                let mut children = Vec::new();
                for &child_rte in child_rte_indexes {
                    let mut cols: Vec<&optimizer_core::ir::types::ColumnRef> = col_map.all_columns()
                        .filter(|cr| cr.pg_varno == child_rte)
                        .collect();
                    cols.sort_by_key(|cr| cr.pg_varattno);
                    let col_ids: Vec<ColumnId> = cols.iter().map(|cr| cr.id).collect();
                    let child_table_id = col_map.all_columns()
                        .find(|cr| cr.pg_varno == child_rte)
                        .map(|cr| cr.table_id)
                        .ok_or_else(|| InboundError::TranslationError(
                            format!("no columns for child rte_index {}", child_rte)
                        ))?;
                    children.push(LogicalExpr {
                        op: LogicalOp::Get {
                            table_id: child_table_id,
                            columns: col_ids,
                            rte_index: child_rte,
                        },
                        children: vec![],
                    });
                }
                return Ok(LogicalExpr {
                    op: LogicalOp::Append,
                    children,
                });
            }

            // Regular table — build a Get node
            let mut cols: Vec<&optimizer_core::ir::types::ColumnRef> = col_map.all_columns()
                .filter(|cr| cr.pg_varno == rte_index)
                .collect();
            cols.sort_by_key(|cr| cr.pg_varattno);
            let cols: Vec<optimizer_core::ir::types::ColumnId> = cols.iter()
                .map(|cr| cr.id)
                .collect();
            let table_id = col_map.all_columns()
                .find(|cr| cr.pg_varno == rte_index)
                .map(|cr| cr.table_id)
                .ok_or_else(|| InboundError::TranslationError(
                    format!("no columns for rte_index {}", rte_index)
                ))?;
            Ok(LogicalExpr {
                op: LogicalOp::Get { table_id, columns: cols, rte_index },
                children: vec![],
            })
        }
        pg_sys::NodeTag::T_JoinExpr => {
            let join_expr = node as *mut pg_sys::JoinExpr;
            translate_join_expr(join_expr, query, col_map, partition_children, catalog)
        }
        _ => Err(InboundError::UnsupportedFeature(
            format!("from item type {:?}", tag)
        )),
    }
}

unsafe fn translate_join_expr(
    join_expr: *mut pg_sys::JoinExpr,
    query: &pg_sys::Query,
    col_map: &ColumnMapping,
    partition_children: &HashMap<u32, Vec<u32>>,
    catalog: &CatalogSnapshot,
) -> Result<LogicalExpr, InboundError> {
    use optimizer_core::ir::types::JoinType;

    let join_type = match (*join_expr).jointype {
        pg_sys::JoinType::JOIN_INNER => JoinType::Inner,
        pg_sys::JoinType::JOIN_LEFT => JoinType::Left,
        pg_sys::JoinType::JOIN_FULL => JoinType::Full,
        pg_sys::JoinType::JOIN_RIGHT => JoinType::Right,
        pg_sys::JoinType::JOIN_SEMI => JoinType::Semi,
        pg_sys::JoinType::JOIN_ANTI => JoinType::AntiSemi,
        _ => return Err(InboundError::UnsupportedFeature("join type".into())),
    };

    let left = translate_from_item((*join_expr).larg as *mut pg_sys::Node, query, col_map, partition_children, catalog)?;
    let right = translate_from_item((*join_expr).rarg as *mut pg_sys::Node, query, col_map, partition_children, catalog)?;

    let predicate = if !(*join_expr).quals.is_null() {
        convert_scalar((*join_expr).quals as *mut pg_sys::Node, col_map)?
    } else {
        // No explicit join condition — use a dummy TRUE
        ScalarExpr::Const {
            type_oid: 16,
            typmod: -1,
            collation: 0,
            value: optimizer_core::ir::scalar::ConstValue::Bool(true),
            is_null: false,
        }
    };

    Ok(LogicalExpr {
        op: LogicalOp::Join { join_type, predicate },
        children: vec![left, right],
    })
}

fn merge_predicates(existing: ScalarExpr, new: ScalarExpr) -> ScalarExpr {
    use optimizer_core::ir::scalar::{ScalarExpr, BoolExprType};
    match existing {
        ScalarExpr::BoolExpr { bool_type: BoolExprType::And, mut args } if args.is_empty() => new,
        ScalarExpr::BoolExpr { bool_type: BoolExprType::And, mut args } => {
            args.push(new);
            ScalarExpr::BoolExpr { bool_type: BoolExprType::And, args }
        }
        other => ScalarExpr::BoolExpr {
            bool_type: BoolExprType::And,
            args: vec![other, new],
        },
    }
}

// ── Aggregation translation ──────────────────────────────────────────────────

unsafe fn translate_aggregate(
    query: &pg_sys::Query,
    child: LogicalExpr,
    col_map: &ColumnMapping,
) -> Result<LogicalExpr, InboundError> {
    // Build group_by list from sortClause in groupClause
    let group_by = translate_group_by_cols(query, col_map)?;

    // Build aggregate expressions from targetList
    let aggregates = collect_aggregates(query, col_map)?;

    // If HAVING clause, wrap inner with Select first
    let inner = if !query.havingQual.is_null() {
        let pred = convert_scalar(query.havingQual as *mut pg_sys::Node, col_map)?;
        LogicalExpr {
            op: LogicalOp::Select { predicate: pred },
            children: vec![child],
        }
    } else {
        child
    };

    Ok(LogicalExpr {
        op: LogicalOp::Aggregate { group_by, aggregates },
        children: vec![inner],
    })
}

unsafe fn translate_group_by_cols(
    query: &pg_sys::Query,
    col_map: &ColumnMapping,
) -> Result<Vec<ColumnId>, InboundError> {
    let mut cols = Vec::new();
    let gc_items = list_iter::<pg_sys::SortGroupClause>(query.groupClause);
    for sgc_ptr in &gc_items {
        let sgc = &**sgc_ptr;
        // Find the target entry with matching ressortgroupref
        let tle = find_target_entry_by_sortgroup(query, sgc.tleSortGroupRef);
        if let Some(te) = tle {
            if let Ok(ScalarExpr::ColumnRef(cid)) =
                convert_scalar((*te).expr as *mut pg_sys::Node, col_map)
            {
                cols.push(cid);
            }
        }
    }
    Ok(cols)
}

unsafe fn collect_aggregates(
    query: &pg_sys::Query,
    col_map: &ColumnMapping,
) -> Result<Vec<AggExpr>, InboundError> {
    let mut aggs = Vec::new();
    let tes = list_iter::<pg_sys::TargetEntry>(query.targetList);
    for te_ptr in &tes {
        let te = &**te_ptr;
        if !te.expr.is_null() {
            collect_agg_from_expr(te.expr as *mut pg_sys::Node, col_map, &mut aggs)?;
        }
    }
    Ok(aggs)
}

unsafe fn collect_agg_from_expr(
    node: *mut pg_sys::Node,
    col_map: &ColumnMapping,
    aggs: &mut Vec<AggExpr>,
) -> Result<(), InboundError> {
    if node.is_null() { return Ok(()); }
    match (*node).type_ {
        pg_sys::NodeTag::T_Aggref => {
            let agg_ref = node as *mut pg_sys::Aggref;
            let agg_func_oid = (*agg_ref).aggfnoid.to_u32();
            let result_type = (*agg_ref).aggtype.to_u32();
            let distinct = (*agg_ref).aggdistinct != std::ptr::null_mut();

            let arg_ptrs = list_iter::<pg_sys::TargetEntry>((*agg_ref).args);
            let mut args = Vec::new();
            for te_ptr in &arg_ptrs {
                let te = *te_ptr;
                if let Ok(expr) = convert_scalar((*te).expr as *mut pg_sys::Node, col_map) {
                    args.push(expr);
                }
            }

            let filter = if !(*agg_ref).aggfilter.is_null() {
                convert_scalar((*agg_ref).aggfilter as *mut pg_sys::Node, col_map)
                    .ok()
                    .map(Box::new)
            } else {
                None
            };

            aggs.push(AggExpr { agg_func_oid, args, distinct, filter, result_type });
        }
        _ => {}
    }
    Ok(())
}

unsafe fn find_target_entry_by_sortgroup(
    query: &pg_sys::Query,
    sortgroupref: u32,
) -> Option<*mut pg_sys::TargetEntry> {
    let tes = list_iter::<pg_sys::TargetEntry>(query.targetList);
    for te_ptr in &tes {
        let te = &**te_ptr;
        if te.ressortgroupref == sortgroupref {
            return Some(*te_ptr);
        }
    }
    None
}

// ── Sort translation ─────────────────────────────────────────────────────────

unsafe fn translate_sort(
    query: &pg_sys::Query,
    child: LogicalExpr,
    col_map: &ColumnMapping,
) -> Result<LogicalExpr, InboundError> {
    let mut keys = Vec::new();
    let sc_items = list_iter::<pg_sys::SortGroupClause>(query.sortClause);
    for sgc_ptr in &sc_items {
        let sgc = &**sgc_ptr;
        // Find the target entry for this sort key
        if let Some(te) = find_target_entry_by_sortgroup(query, sgc.tleSortGroupRef) {
            if let Ok(ScalarExpr::ColumnRef(cid)) =
                convert_scalar((*te).expr as *mut pg_sys::Node, col_map)
            {
                keys.push(SortKey {
                    column: cid,
                    ascending: true, // direction encoded in sort_op_oid; default to ascending
                    nulls_first: sgc.nulls_first,
                    sort_op_oid: sgc.sortop.to_u32(),
                    collation_oid: 0,
                });
            }
        }
    }
    Ok(LogicalExpr {
        op: LogicalOp::Sort { keys },
        children: vec![child],
    })
}

// ── Distinct translation ─────────────────────────────────────────────────────

unsafe fn translate_distinct(
    query: &pg_sys::Query,
    child: LogicalExpr,
    col_map: &ColumnMapping,
) -> Result<LogicalExpr, InboundError> {
    let mut columns = Vec::new();
    let dc_items = list_iter::<pg_sys::SortGroupClause>(query.distinctClause);
    for sgc_ptr in &dc_items {
        let sgc = &**sgc_ptr;
        if let Some(te) = find_target_entry_by_sortgroup(query, sgc.tleSortGroupRef) {
            if let Ok(ScalarExpr::ColumnRef(cid)) =
                convert_scalar((*te).expr as *mut pg_sys::Node, col_map)
            {
                columns.push(cid);
            }
        }
    }
    Ok(LogicalExpr {
        op: LogicalOp::Distinct { columns },
        children: vec![child],
    })
}

// ── Limit translation ────────────────────────────────────────────────────────

unsafe fn translate_limit(
    query: &pg_sys::Query,
    child: LogicalExpr,
    col_map: &ColumnMapping,
) -> Result<LogicalExpr, InboundError> {
    let offset = if !query.limitOffset.is_null() {
        convert_scalar(query.limitOffset as *mut pg_sys::Node, col_map).ok()
    } else {
        None
    };
    let count = if !query.limitCount.is_null() {
        convert_scalar(query.limitCount as *mut pg_sys::Node, col_map).ok()
    } else {
        None
    };
    Ok(LogicalExpr {
        op: LogicalOp::Limit { offset, count },
        children: vec![child],
    })
}

// ── Window translation ──────────────────────────────────────────────────────

unsafe fn translate_window(
    query: &pg_sys::Query,
    child: LogicalExpr,
    col_map: &ColumnMapping,
) -> Result<LogicalExpr, InboundError> {
    use optimizer_core::ir::types::WindowClause;

    let wc_items = list_iter::<pg_sys::WindowClause>(query.windowClause);
    let mut clauses = Vec::new();

    for wc_ptr in &wc_items {
        let wc = &**wc_ptr;

        // Partition by columns
        let mut partition_by = Vec::new();
        let pk_items = list_iter::<pg_sys::SortGroupClause>(wc.partitionClause);
        for sgc_ptr in &pk_items {
            let sgc = &**sgc_ptr;
            if let Some(te) = find_target_entry_by_sortgroup(query, sgc.tleSortGroupRef) {
                if let Ok(ScalarExpr::ColumnRef(cid)) =
                    convert_scalar((*te).expr as *mut pg_sys::Node, col_map)
                {
                    partition_by.push(cid);
                }
            }
        }

        // Order by keys
        let mut order_by = Vec::new();
        let ok_items = list_iter::<pg_sys::SortGroupClause>(wc.orderClause);
        for sgc_ptr in &ok_items {
            let sgc = &**sgc_ptr;
            if let Some(te) = find_target_entry_by_sortgroup(query, sgc.tleSortGroupRef) {
                if let Ok(ScalarExpr::ColumnRef(cid)) =
                    convert_scalar((*te).expr as *mut pg_sys::Node, col_map)
                {
                    order_by.push(SortKey {
                        column: cid,
                        ascending: true,
                        nulls_first: sgc.nulls_first,
                        sort_op_oid: sgc.sortop.to_u32(),
                        collation_oid: 0,
                    });
                }
            }
        }

        clauses.push(WindowClause {
            partition_by,
            order_by,
            frame_options: wc.frameOptions,
            winref: wc.winref,
        });
    }

    Ok(LogicalExpr {
        op: LogicalOp::Window { clauses },
        children: vec![child],
    })
}

// ── Partition expansion helpers ──────────────────────────────────────────────

/// Register all columns of a relation into ColumnMapping and TableStats.
/// Returns the list of ColumnIds registered (in attnum order).
unsafe fn register_relation_columns(
    rel: pg_sys::Relation,
    relid: pg_sys::Oid,
    rte_index: u32,
    table_id: TableId,
    col_map: &mut ColumnMapping,
    tables: &mut HashMap<TableId, TableStats>,
) -> Result<Vec<ColumnId>, InboundError> {
    let rd_rel = (*rel).rd_rel;
    let row_count = (*rd_rel).reltuples as f64;
    let page_count = (*rd_rel).relpages as u64;
    let rel_name = CStr::from_ptr((*rd_rel).relname.data.as_ptr())
        .to_string_lossy().to_string();

    let tupdesc = (*rel).rd_att;
    let natts = (*tupdesc).natts as usize;
    let mut col_ids = Vec::new();
    let mut col_stats = Vec::new();
    let mut col_id_to_attnum: HashMap<ColumnId, i16> = HashMap::new();

    for i in 0..natts {
        let att = tupdesc_get_attr(tupdesc, natts, i);
        if att.attisdropped { continue; }
        let attnum = att.attnum;
        let attname = CStr::from_ptr(att.attname.data.as_ptr())
            .to_string_lossy().to_string();

        let cid = col_map.register_column(
            table_id,
            &attname,
            rte_index,
            attnum,
            att.atttypid.to_u32(),
            att.atttypmod,
            att.attcollation.to_u32(),
        );
        col_ids.push(cid);
        col_id_to_attnum.insert(cid, attnum);

        let avg_width = if att.attlen > 0 { att.attlen as i32 } else { 32 };
        let cs = read_column_stats(relid, attnum, att.atttypid, avg_width);
        col_stats.push(ColumnStats {
            attnum,
            name: attname,
            ..cs
        });
    }

    let indexes = read_table_indexes(rel, &col_id_to_attnum);

    tables.insert(table_id, TableStats {
        oid: relid.to_u32(),
        name: rel_name,
        row_count: if row_count < 1.0 { 1000.0 } else { row_count },
        page_count,
        columns: col_stats,
        indexes,
        col_id_to_attnum,
    });

    Ok(col_ids)
}

/// Add a new RTE for a child partition to the query's range table.
/// Returns the 1-based rte_index of the new entry.
///
/// Safety: mutates query.rtable via raw pointer (PG convention during planning).
unsafe fn add_child_rte(
    query: *const pg_sys::Query,
    child_oid: pg_sys::Oid,
) -> Result<u32, InboundError> {
    use crate::utils::palloc::palloc_node;

    let child_rel = pg_sys::RelationIdGetRelation(child_oid);
    if child_rel.is_null() {
        return Err(InboundError::CatalogAccessError(
            format!("cannot open child relation OID {}", child_oid.to_u32())
        ));
    }

    let rte = palloc_node::<pg_sys::RangeTblEntry>(pg_sys::NodeTag::T_RangeTblEntry);
    (*rte).rtekind = pg_sys::RTEKind::RTE_RELATION;
    (*rte).relid = child_oid;
    (*rte).relkind = (*(*child_rel).rd_rel).relkind;
    (*rte).rellockmode = 1; // AccessShareLock
    (*rte).inh = false;
    (*rte).lateral = false;
    (*rte).inFromCl = true;

    // Build eref alias with column names from the child relation
    let tupdesc = (*child_rel).rd_att;
    let natts = (*tupdesc).natts as usize;
    let rel_name = CStr::from_ptr((*(*child_rel).rd_rel).relname.data.as_ptr());
    let alias = palloc_node::<pg_sys::Alias>(pg_sys::NodeTag::T_Alias);

    // Copy relation name to palloc'd memory
    let name_bytes = rel_name.to_bytes_with_nul();
    let dst = pg_sys::palloc(name_bytes.len()) as *mut i8;
    std::ptr::copy_nonoverlapping(name_bytes.as_ptr() as *const i8, dst, name_bytes.len());
    (*alias).aliasname = dst;

    // Build colnames list
    let mut colnames: *mut pg_sys::List = std::ptr::null_mut();
    for i in 0..natts {
        let att = tupdesc_get_attr(tupdesc, natts, i);
        if att.attisdropped {
            // Add empty string for dropped columns to maintain position
            let empty = pg_sys::makeString(b"\0".as_ptr() as *mut i8);
            colnames = pg_sys::lappend(colnames, empty as *mut std::ffi::c_void);
        } else {
            let attname = CStr::from_ptr(att.attname.data.as_ptr());
            let name_bytes = attname.to_bytes_with_nul();
            let name_ptr = pg_sys::palloc(name_bytes.len()) as *mut i8;
            std::ptr::copy_nonoverlapping(name_bytes.as_ptr() as *const i8, name_ptr, name_bytes.len());
            let str_val = pg_sys::makeString(name_ptr);
            colnames = pg_sys::lappend(colnames, str_val as *mut std::ffi::c_void);
        }
    }
    (*alias).colnames = colnames;
    (*rte).eref = alias;

    pg_sys::RelationClose(child_rel);

    // Append to the query's range table (mutating it via raw pointer)
    let query_mut = query as *mut pg_sys::Query;
    (*query_mut).rtable = pg_sys::lappend((*query_mut).rtable, rte as *mut std::ffi::c_void);

    let new_index = pg_sys::list_length((*query_mut).rtable) as u32;
    Ok(new_index)
}

// ── Catalog reading ──────────────────────────────────────────────────────────

/// Read full column statistics from pg_statistic: ndistinct, null_fraction,
/// correlation, MCV (most common values + frequencies), and histogram bounds.
unsafe fn read_column_stats(
    relid: pg_sys::Oid,
    attnum: i16,
    type_oid: pg_sys::Oid,
    avg_width: i32,
) -> ColumnStats {
    let cache_id = pg_sys::SysCacheIdentifier::STATRELATTINH as i32;

    let tup = pg_sys::SearchSysCache3(
        cache_id,
        relid.into(),
        (attnum as i32).into(),
        pg_sys::Datum::from(0i32), // stainherit = false
    );
    if tup.is_null() {
        return ColumnStats {
            attnum,
            ndistinct: -1.0,
            avg_width,
            ..Default::default()
        };
    }

    let mut isnull = false;

    // stanullfrac
    let null_frac_datum = pg_sys::SysCacheGetAttr(
        cache_id, tup,
        pg_sys::Anum_pg_statistic_stanullfrac as i16,
        &mut isnull,
    );
    let null_fraction = if isnull { 0.0 } else {
        f32::from_bits(null_frac_datum.value() as u32) as f64
    };

    // stadistinct
    let ndist_datum = pg_sys::SysCacheGetAttr(
        cache_id, tup,
        pg_sys::Anum_pg_statistic_stadistinct as i16,
        &mut isnull,
    );
    let ndistinct = if isnull { -1.0 } else {
        f32::from_bits(ndist_datum.value() as u32) as f64
    };

    // Use PG's get_attstatsslot API to read extended statistics.
    // It handles slot scanning, detoasting, and array deconstruction properly.

    // Read correlation
    let correlation = read_stats_slot(tup,
            pg_sys::STATISTIC_KIND_CORRELATION as i32,
            type_oid,
            pg_sys::ATTSTATSSLOT_NUMBERS)
        .and_then(|(_, nums)| nums.into_iter().next())
        .map(|v| v as f64)
        .unwrap_or(0.0);

    // Read MCV: values + frequencies
    let most_common_vals = read_stats_slot(tup,
            pg_sys::STATISTIC_KIND_MCV as i32,
            type_oid,
            pg_sys::ATTSTATSSLOT_VALUES | pg_sys::ATTSTATSSLOT_NUMBERS)
        .and_then(|(values, freqs)| {
            if values.len() != freqs.len() || values.is_empty() { return None; }
            Some(values.into_iter().zip(freqs.into_iter())
                .map(|(value, frequency)| McvEntry { value, frequency })
                .collect())
        });

    // Read histogram bounds
    let histogram_bounds = read_stats_slot(tup,
            pg_sys::STATISTIC_KIND_HISTOGRAM as i32,
            type_oid,
            pg_sys::ATTSTATSSLOT_VALUES)
        .and_then(|(values, _)| {
            if values.len() < 2 { return None; }
            Some(values.into_iter()
                .map(|value| HistogramBound { value })
                .collect())
        });

    pg_sys::ReleaseSysCache(tup);

    ColumnStats {
        attnum,
        name: String::new(), // filled by caller
        ndistinct,
        null_fraction,
        avg_width,
        correlation,
        histogram_bounds,
        most_common_vals,
    }
}

/// Read a stats slot using PG's get_attstatsslot API.
/// Returns (values as ConstValue, numbers as f32) for the given stakind.
unsafe fn read_stats_slot(
    tup: pg_sys::HeapTuple,
    reqkind: i32,
    type_oid: pg_sys::Oid,
    flags: u32,
) -> Option<(Vec<optimizer_core::ir::scalar::ConstValue>, Vec<f32>)> {
    use optimizer_core::ir::scalar::ConstValue;

    let mut sslot = pg_sys::AttStatsSlot::default();
    let ok = pg_sys::get_attstatsslot(
        &mut sslot,
        tup,
        reqkind,
        pg_sys::InvalidOid, // reqop: any operator
        flags as i32,
    );
    if !ok {
        return None;
    }

    // Extract values
    let values: Vec<ConstValue> = if !sslot.values.is_null() && sslot.nvalues > 0 {
        (0..sslot.nvalues as usize)
            .map(|i| datum_to_const_value(*sslot.values.add(i), type_oid))
            .collect()
    } else {
        vec![]
    };

    // Extract numbers (float4 array)
    let numbers: Vec<f32> = if !sslot.numbers.is_null() && sslot.nnumbers > 0 {
        (0..sslot.nnumbers as usize)
            .map(|i| *sslot.numbers.add(i))
            .collect()
    } else {
        vec![]
    };

    pg_sys::free_attstatsslot(&mut sslot);
    Some((values, numbers))
}

/// Convert a single Datum to ConstValue based on PG type OID.
unsafe fn datum_to_const_value(
    datum: pg_sys::Datum,
    type_oid: pg_sys::Oid,
) -> optimizer_core::ir::scalar::ConstValue {
    use optimizer_core::ir::scalar::ConstValue;

    match type_oid {
        pg_sys::INT2OID => ConstValue::Int16(pg_sys::DatumGetInt16(datum)),
        pg_sys::INT4OID => ConstValue::Int32(pg_sys::DatumGetInt32(datum)),
        pg_sys::INT8OID => ConstValue::Int64(pg_sys::DatumGetInt64(datum)),
        pg_sys::FLOAT4OID => ConstValue::Float32(pg_sys::DatumGetFloat4(datum)),
        pg_sys::FLOAT8OID => ConstValue::Float64(pg_sys::DatumGetFloat8(datum)),
        pg_sys::BOOLOID => ConstValue::Bool(datum.value() != 0),
        pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID | pg_sys::NAMEOID => {
            let cstr = pg_sys::text_to_cstring(datum.value() as *const pg_sys::text);
            let s = CStr::from_ptr(cstr).to_string_lossy().to_string();
            pg_sys::pfree(cstr as *mut std::ffi::c_void);
            ConstValue::Text(s)
        }
        _ => {
            // Unsupported type: convert via output function to text as fallback
            // For now, store as Int64 to preserve ordering for numeric-like types
            ConstValue::Int64(datum.value() as i64)
        }
    }
}

/// Read index metadata for a relation.
unsafe fn read_table_indexes(
    rel: pg_sys::Relation,
    col_id_to_attnum: &HashMap<ColumnId, i16>,
) -> Vec<IndexStats> {
    let mut indexes = Vec::new();

    let index_list = pg_sys::RelationGetIndexList(rel);
    if index_list.is_null() {
        return indexes;
    }

    let idx_oids = list_iter_oid(index_list);
    for idx_oid in &idx_oids {
        let idx_rel = pg_sys::RelationIdGetRelation(*idx_oid);
        if idx_rel.is_null() { continue; }

        let rd_rel = (*idx_rel).rd_rel;
        let rd_idx = (*idx_rel).rd_index;

        if rd_idx.is_null() {
            pg_sys::RelationClose(idx_rel);
            continue;
        }

        let am_oid = (*rd_rel).relam.to_u32();
        let am_type = match am_oid {
            403 => IndexAmType::BTree,
            405 => IndexAmType::Hash,
            783 => IndexAmType::GiST,
            2742 => IndexAmType::GIN,
            4000 => IndexAmType::BRIN,
            4783 => IndexAmType::SPGiST,
            _ => {
                pg_sys::RelationClose(idx_rel);
                continue;
            }
        };

        let pages = (*rd_rel).relpages as u64;
        let unique = (*rd_idx).indisunique;
        let natts = (*rd_idx).indnkeyatts as usize;

        // Read key column attnums from indkey
        let col_attnums: Vec<i16> = {
            let ptr = (*rd_idx).indkey.values.as_ptr();
            (0..natts)
                .map(|i| *ptr.add(i))
                .filter(|&a| a > 0)
                .collect()
        };

        let name = CStr::from_ptr((*rd_rel).relname.data.as_ptr())
            .to_string_lossy()
            .to_string();

        indexes.push(IndexStats {
            oid: idx_oid.to_u32(),
            name,
            columns: col_attnums,
            unique,
            am_type,
            pages,
            tree_height: 0,
            predicate: None,
            include_columns: vec![],
        });

        pg_sys::RelationClose(idx_rel);
    }

    pg_sys::list_free(index_list);
    indexes
}

/// Access the i'th FormData_pg_attribute from a TupleDesc.
/// In PG18 the `attrs` flexible array was replaced by `compact_attrs`,
/// with FormData_pg_attribute stored immediately after the compact array.
#[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16", feature = "pg17"))]
unsafe fn tupdesc_get_attr(
    tupdesc: *mut pg_sys::TupleDescData,
    _natts: usize,
    i: usize,
) -> pg_sys::FormData_pg_attribute {
    *(*tupdesc).attrs.as_ptr().add(i)
}

#[cfg(feature = "pg18")]
unsafe fn tupdesc_get_attr(
    tupdesc: *mut pg_sys::TupleDescData,
    natts: usize,
    i: usize,
) -> pg_sys::FormData_pg_attribute {
    let att_pointer = (*tupdesc)
        .compact_attrs
        .as_ptr()
        .add(natts)
        .cast::<pg_sys::FormData_pg_attribute>();
    *att_pointer.add(i)
}
