use crate::ir::types::Cost;
use crate::ir::physical::PhysicalOp;
use crate::ir::physical::AggStrategy;
use crate::properties::logical::LogicalProperties;

use super::stats::CostModel;

/// Compute cost for a physical operator.
pub fn cost_physical_op(
    op: &PhysicalOp,
    logical_props: &LogicalProperties,
    params: &CostModel,
    children_costs: &[Cost],
    children_rows: &[f64],
    page_count: u64,
) -> Cost {
    let rows = logical_props.row_count.max(1.0);

    match op {
        PhysicalOp::SeqScan { .. } => {
            let pages = page_count as f64;
            Cost {
                startup: 0.0,
                total: params.seq_page_cost * pages + params.cpu_tuple_cost * rows,
            }
        }

        PhysicalOp::IndexScan { .. } => {
            // index traversal + heap fetches
            let index_pages = page_count as f64;
            let startup = params.random_page_cost; // tree root traversal
            let total = startup
                + params.random_page_cost * index_pages
                + params.cpu_index_tuple_cost * rows
                + params.cpu_tuple_cost * rows;
            Cost { startup, total }
        }

        PhysicalOp::IndexOnlyScan { .. } => {
            // Similar to IndexScan but no heap fetch if visibility map is set
            let index_pages = page_count as f64;
            let startup = params.random_page_cost;
            let total = startup
                + params.random_page_cost * index_pages * 0.5 // fewer heap fetches
                + params.cpu_index_tuple_cost * rows;
            Cost { startup, total }
        }

        PhysicalOp::BitmapHeapScan { .. } => {
            // Bitmap scans: lower cost per heap page due to sequential re-read pattern
            let index_pages = page_count as f64;
            let startup = params.random_page_cost + params.cpu_index_tuple_cost * rows;
            let total = startup
                + params.seq_page_cost * index_pages // sequential re-scan
                + params.cpu_tuple_cost * rows;
            Cost { startup, total }
        }

        PhysicalOp::NestLoop { .. } => {
            let outer = children_costs.first().copied().unwrap_or(Cost::zero());
            let inner = children_costs.get(1).copied().unwrap_or(Cost::zero());
            let outer_rows = children_rows.first().copied().unwrap_or(1.0).max(1.0);
            Cost {
                startup: outer.startup + inner.startup,
                total: outer.total
                    + outer_rows * inner.total
                    + params.cpu_operator_cost * rows,
            }
        }

        PhysicalOp::HashJoin { .. } => {
            let outer = children_costs.first().copied().unwrap_or(Cost::zero());
            let inner = children_costs.get(1).copied().unwrap_or(Cost::zero());
            let inner_rows = children_rows.get(1).copied().unwrap_or(1.0).max(1.0);
            let outer_rows = children_rows.first().copied().unwrap_or(1.0).max(1.0);
            // build hash table on inner, probe with outer
            let build = inner.total + params.cpu_operator_cost * inner_rows;
            let probe = outer.total + params.cpu_operator_cost * outer_rows;
            Cost {
                startup: build,
                total: build + probe + params.cpu_tuple_cost * rows,
            }
        }

        PhysicalOp::MergeJoin { .. } => {
            let outer = children_costs.first().copied().unwrap_or(Cost::zero());
            let inner = children_costs.get(1).copied().unwrap_or(Cost::zero());
            Cost {
                startup: outer.startup + inner.startup,
                total: outer.total + inner.total + params.cpu_operator_cost * rows,
            }
        }

        PhysicalOp::Sort { .. } => {
            let child = children_costs.first().copied().unwrap_or(Cost::zero());
            let child_rows = children_rows.first().copied().unwrap_or(1.0).max(1.0);
            let sort_cpu = if child_rows > 1.0 {
                params.cpu_operator_cost * child_rows * child_rows.log2()
            } else {
                0.0
            };
            Cost {
                startup: child.total + sort_cpu,
                total: child.total + sort_cpu + params.cpu_tuple_cost * rows,
            }
        }

        PhysicalOp::Agg { strategy, .. } => {
            let child = children_costs.first().copied().unwrap_or(Cost::zero());
            let child_rows = children_rows.first().copied().unwrap_or(1.0).max(1.0);
            match strategy {
                AggStrategy::Hashed => Cost {
                    startup: child.total + params.cpu_operator_cost * child_rows,
                    total: child.total + params.cpu_operator_cost * child_rows
                        + params.cpu_tuple_cost * rows,
                },
                _ => Cost {
                    startup: child.startup,
                    total: child.total
                        + params.cpu_operator_cost * child_rows
                        + params.cpu_tuple_cost * rows,
                },
            }
        }

        PhysicalOp::Limit { .. } => {
            let child = children_costs.first().copied().unwrap_or(Cost::zero());
            let child_rows = children_rows.first().copied().unwrap_or(1.0).max(1.0);
            let frac = (rows / child_rows).min(1.0);
            Cost {
                startup: child.startup,
                total: child.startup + (child.total - child.startup) * frac,
            }
        }

        PhysicalOp::Unique { .. } => {
            let child = children_costs.first().copied().unwrap_or(Cost::zero());
            let child_rows = children_rows.first().copied().unwrap_or(1.0).max(1.0);
            Cost {
                startup: child.startup,
                total: child.total + params.cpu_operator_cost * child_rows,
            }
        }

        PhysicalOp::WindowAgg { .. } => {
            let child = children_costs.first().copied().unwrap_or(Cost::zero());
            let child_rows = children_rows.first().copied().unwrap_or(1.0).max(1.0);
            // WindowAgg: sort + per-row eval
            let sort_cpu = if child_rows > 1.0 {
                params.cpu_operator_cost * child_rows * child_rows.log2()
            } else {
                0.0
            };
            Cost {
                startup: child.total + sort_cpu,
                total: child.total + sort_cpu + params.cpu_tuple_cost * rows,
            }
        }

        PhysicalOp::Append => {
            // Sum of all children costs + small per-tuple overhead
            let total: f64 = children_costs.iter().map(|c| c.total).sum::<f64>()
                + params.cpu_tuple_cost * rows;
            let startup = children_costs.first().map(|c| c.startup).unwrap_or(0.0);
            Cost { startup, total }
        }

        // Future operators
        _ => Cost { startup: 0.0, total: f64::MAX / 2.0 },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::types::TableId;

    #[test]
    fn test_seq_scan_cost() {
        let params = CostModel::default();
        let props = LogicalProperties {
            output_columns: vec![],
            row_count: 1000.0,
            table_ids: vec![],
            not_null_columns: vec![],
            unique_keys: vec![],
            fd_keys: vec![],
            equivalence_classes: vec![],
            avg_width: 32.0,
        };
        let cost = cost_physical_op(
            &PhysicalOp::SeqScan { scanrelid: 1 },
            &props,
            &params,
            &[],
            &[],
            10,
        );
        // total = 1.0 * 10 + 0.01 * 1000 = 20
        assert_eq!(cost.startup, 0.0);
        assert!((cost.total - 20.0).abs() < 0.001);
    }
}
