use std::collections::HashMap;
use std::ffi::CStr;

use pgrx::pg_sys;

use optimizer_core::cost::stats::*;
use optimizer_core::ir::logical::{LogicalExpr, LogicalOp};
use optimizer_core::ir::types::TableId;

use super::InboundError;
use super::column_mapping::ColumnMapping;
use super::query_check::is_supported_query;
use crate::utils::pg_list;

/// Result of Phase 1 conversion.
pub struct ConvertResult {
    pub logical_expr: LogicalExpr,
    pub catalog: CatalogSnapshot,
    pub col_map: ColumnMapping,
}

/// Convert a PG Query into our IR (M1: single table, no WHERE).
pub unsafe fn convert_query(query: &pg_sys::Query) -> Result<ConvertResult, InboundError> {
    // 1. Whitelist check
    is_supported_query(query)?;

    // 2. Walk range table — expect exactly 1 RTE_RELATION
    let rtes = pg_list::list_iter::<pg_sys::RangeTblEntry>(query.rtable);
    if rtes.is_empty() {
        return Err(InboundError::TranslationError("empty range table".into()));
    }

    let mut col_map = ColumnMapping::new();
    let mut tables = HashMap::new();
    let mut table_id_counter = 0u32;
    let mut logical_expr: Option<LogicalExpr> = None;

    for (rte_index_0based, rte_ptr) in rtes.iter().enumerate() {
        let rte = &**rte_ptr;
        let rte_index = (rte_index_0based + 1) as u32; // 1-based

        if rte.rtekind != pg_sys::RTEKind::RTE_RELATION {
            continue;
        }

        table_id_counter += 1;
        let table_id = TableId(table_id_counter);

        // Open relation to read metadata
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

        // Read attributes
        let tupdesc = (*rel).rd_att;
        let natts = (*tupdesc).natts as usize;
        let mut col_ids = Vec::new();
        let mut col_stats = Vec::new();

        for i in 0..natts {
            let att = *(*tupdesc).attrs.as_ptr().add(i);
            if att.attisdropped {
                continue;
            }
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

            col_stats.push(ColumnStats {
                attnum,
                name: attname,
                ndistinct: -1.0,
                null_fraction: 0.0,
                avg_width: att.attlen as i32,
                correlation: 0.0,
            });
        }

        pg_sys::RelationClose(rel);

        // Build table stats
        tables.insert(table_id, TableStats {
            oid: rte.relid.to_u32(),
            name: rel_name,
            row_count: if row_count < 0.0 { 1000.0 } else { row_count },
            page_count,
            columns: col_stats,
            indexes: vec![],
        });

        // Build logical expression for this table
        logical_expr = Some(LogicalExpr {
            op: LogicalOp::Get {
                table_id,
                columns: col_ids,
                rte_index,
            },
            children: vec![],
        });
    }

    let expr = logical_expr.ok_or_else(|| {
        InboundError::TranslationError("no base table found".into())
    })?;

    // Read PG cost GUCs
    let cost_params = CostParams {
        seq_page_cost: pg_sys::seq_page_cost,
        random_page_cost: pg_sys::random_page_cost,
        cpu_tuple_cost: pg_sys::cpu_tuple_cost,
        cpu_index_tuple_cost: pg_sys::cpu_index_tuple_cost,
        cpu_operator_cost: pg_sys::cpu_operator_cost,
        effective_cache_size: pg_sys::effective_cache_size as f64,
        work_mem: pg_sys::work_mem as usize * 1024, // work_mem is in KB
    };

    let catalog = CatalogSnapshot { tables, cost_params };

    Ok(ConvertResult { logical_expr: expr, catalog, col_map })
}
