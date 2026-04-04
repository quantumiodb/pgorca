use crate::ir::types::Cost;
use crate::ir::physical::PhysicalOp;
use crate::properties::logical::LogicalProperties;

use super::stats::CostParams;

/// Compute cost for a physical operator.
pub fn cost_physical_op(
    op: &PhysicalOp,
    logical_props: &LogicalProperties,
    params: &CostParams,
    _children_costs: &[Cost],
    _children_rows: &[f64],
    page_count: u64,
) -> Cost {
    match op {
        PhysicalOp::SeqScan { .. } => {
            let rows = logical_props.row_count;
            let pages = page_count as f64;
            let startup = 0.0;
            let total = params.seq_page_cost * pages + params.cpu_tuple_cost * rows;
            Cost { startup, total }
        }
        // Future operators will be added here
        _ => Cost { startup: 0.0, total: f64::MAX / 2.0 },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::types::*;

    #[test]
    fn test_seq_scan_cost() {
        let params = CostParams::default();
        let props = LogicalProperties {
            output_columns: vec![ColumnId(1)],
            row_count: 1000.0,
            table_ids: vec![TableId(1)],
            not_null_columns: vec![],
            unique_keys: vec![],
            avg_width: 32.0,
        };
        let cost = cost_physical_op(
            &PhysicalOp::SeqScan { scanrelid: 1 },
            &props,
            &params,
            &[],
            &[],
            10, // 10 pages
        );
        // total = 1.0 * 10 + 0.01 * 1000 = 10 + 10 = 20
        assert_eq!(cost.startup, 0.0);
        assert!((cost.total - 20.0).abs() < 0.001);
    }
}
