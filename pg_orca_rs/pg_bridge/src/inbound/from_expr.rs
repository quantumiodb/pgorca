use std::collections::HashMap;
use std::ffi::CStr;

use pgrx::pg_sys;

use optimizer_core::cost::stats::*;
use optimizer_core::ir::logical::{LogicalExpr, LogicalOp};
use optimizer_core::ir::scalar::ScalarExpr;
use optimizer_core::ir::types::{TableId, IndexAmType, SortKey, ColumnId, AggExpr};

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

    let rtes = list_iter::<pg_sys::RangeTblEntry>(query.rtable);
    for (rte_index_0, rte_ptr) in rtes.iter().enumerate() {
        let rte = &**rte_ptr;
        let rte_index = (rte_index_0 + 1) as u32; // 1-based

        if rte.rtekind != pg_sys::RTEKind::RTE_RELATION {
            continue;
        }

        table_id_counter += 1;
        let table_id = TableId(table_id_counter);
        rte_to_table.insert(rte_index, table_id);

        let rel = pg_sys::RelationIdGetRelation(rte.relid);
        if rel.is_null() {
            return Err(InboundError::CatalogAccessError(
                format!("cannot open relation OID {}", rte.relid.to_u32())
            ));
        }

        let rd_rel = (*rel).rd_rel;
        let row_count = (*rd_rel).reltuples as f64;
        let page_count = (*rd_rel).relpages as u64;
        let rel_name = CStr::from_ptr((*rd_rel).relname.data.as_ptr())
            .to_string_lossy().to_string();

        // Read attributes and build column mapping + stats
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

            // Read basic stats from pg_statistic if available
            let (ndistinct, null_fraction) = read_column_stats(rte.relid, attnum);

            col_stats.push(ColumnStats {
                attnum,
                name: attname,
                ndistinct,
                null_fraction,
                avg_width: if att.attlen > 0 { att.attlen as i32 } else { 32 },
                correlation: 0.0,
            });
        }

        // Read indexes
        let indexes = read_table_indexes(rel, &col_id_to_attnum);

        pg_sys::RelationClose(rel);

        tables.insert(table_id, TableStats {
            oid: rte.relid.to_u32(),
            name: rel_name,
            row_count: if row_count < 1.0 { 1000.0 } else { row_count },
            page_count,
            columns: col_stats,
            indexes,
            col_id_to_attnum,
        });
    }

    // 3. Read PG cost GUCs
    let cost_params = CostParams {
        seq_page_cost: pg_sys::seq_page_cost,
        random_page_cost: pg_sys::random_page_cost,
        cpu_tuple_cost: pg_sys::cpu_tuple_cost,
        cpu_index_tuple_cost: pg_sys::cpu_index_tuple_cost,
        cpu_operator_cost: pg_sys::cpu_operator_cost,
        effective_cache_size: pg_sys::effective_cache_size as f64,
        work_mem: pg_sys::work_mem as usize * 1024,
    };

    let catalog = CatalogSnapshot { tables, rte_to_table, cost_params };

    // 4. Build logical expression tree from jointree
    let mut logical_expr = translate_from_expr(query.jointree, query, &col_map)?;

    // 5. Wrap with aggregation if needed (GROUP BY / aggregates)
    if query.hasAggs || list_length(query.groupClause) > 0 {
        logical_expr = translate_aggregate(query, logical_expr, &col_map)?;
    }

    // 6. Wrap with sort if ORDER BY present
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

// ── FromExpr / JoinExpr translation ─────────────────────────────────────────

unsafe fn translate_from_expr(
    from_expr: *mut pg_sys::FromExpr,
    query: &pg_sys::Query,
    col_map: &ColumnMapping,
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

    let mut expr = translate_from_item(items[0], query, col_map)?;
    for item in &items[1..] {
        let right = translate_from_item(*item, query, col_map)?;
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
) -> Result<LogicalExpr, InboundError> {
    if node.is_null() {
        return Err(InboundError::TranslationError("null from item".into()));
    }
    let tag = (*node).type_;
    match tag {
        pg_sys::NodeTag::T_RangeTblRef => {
            let rtref = node as *mut pg_sys::RangeTblRef;
            let rte_index = (*rtref).rtindex as u32;
            // Find matching table_id from col_map
            // We get all column refs for this rte_index
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
            translate_join_expr(join_expr, query, col_map)
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

    let left = translate_from_item((*join_expr).larg as *mut pg_sys::Node, query, col_map)?;
    let right = translate_from_item((*join_expr).rarg as *mut pg_sys::Node, query, col_map)?;

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

// ── Catalog reading ──────────────────────────────────────────────────────────

/// Read ndistinct and null_fraction from pg_statistic for one column.
unsafe fn read_column_stats(relid: pg_sys::Oid, attnum: i16) -> (f64, f64) {
    // Use SysCache to look up pg_statistic (3-key cache in PG17: relid, attnum, stainherit)
    let tup = pg_sys::SearchSysCache3(
        pg_sys::SysCacheIdentifier::STATRELATTINH as i32,
        relid.into(),
        (attnum as i32).into(),
        pg_sys::Datum::from(0i32), // stainherit = false
    );
    if tup.is_null() {
        return (-1.0, 0.0); // no stats
    }

    let mut isnull = false;

    // stanullfrac (attr 5 in pg_statistic)
    let null_frac_datum = pg_sys::SysCacheGetAttr(
        pg_sys::SysCacheIdentifier::STATRELATTINH as i32,
        tup,
        pg_sys::Anum_pg_statistic_stanullfrac as i16,
        &mut isnull,
    );
    let null_fraction = if isnull { 0.0 } else {
        f32::from_bits(null_frac_datum.value() as u32) as f64
    };

    // stadistinct (attr 7)
    let ndist_datum = pg_sys::SysCacheGetAttr(
        pg_sys::SysCacheIdentifier::STATRELATTINH as i32,
        tup,
        pg_sys::Anum_pg_statistic_stadistinct as i16,
        &mut isnull,
    );
    let ndistinct = if isnull { -1.0 } else {
        f32::from_bits(ndist_datum.value() as u32) as f64
    };

    pg_sys::ReleaseSysCache(tup);
    (ndistinct, null_fraction)
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
