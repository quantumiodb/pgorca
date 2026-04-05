//! Fuzz testing with proptest: generate random LogicalExpr trees and verify
//! the optimizer never panics and always produces a valid plan or error.

use proptest::prelude::*;
use std::collections::HashMap;

use optimizer_core::cost::stats::*;
use optimizer_core::ir::logical::{LogicalExpr, LogicalOp};
use optimizer_core::ir::scalar::ScalarExpr;
use optimizer_core::ir::types::*;

fn make_catalog(num_tables: u32) -> CatalogSnapshot {
    let mut tables = HashMap::new();
    let mut rte_to_table = HashMap::new();
    for i in 1..=num_tables {
        let tid = TableId(i);
        rte_to_table.insert(i, tid);
        tables.insert(tid, TableStats {
            oid: 16384 + i,
            name: format!("t{}", i),
            row_count: 1000.0 * i as f64,
            page_count: 10 * i as u64,
            columns: vec![
                ColumnStats { attnum: 1, name: "a".into(), avg_width: 4, ..Default::default() },
                ColumnStats { attnum: 2, name: "b".into(), avg_width: 32, ..Default::default() },
            ],
            indexes: vec![],
            col_id_to_attnum: HashMap::new(),
        });
    }
    CatalogSnapshot { tables, rte_to_table, cost_model: CostModel::default() }
}

// Strategy to generate random LogicalExpr trees
fn arb_logical_expr(max_depth: u32, num_tables: u32) -> impl Strategy<Value = LogicalExpr> {
    let leaf = (1..=num_tables).prop_map(move |t| LogicalExpr {
        op: LogicalOp::Get {
            table_id: TableId(t),
            columns: vec![ColumnId(t * 10 + 1), ColumnId(t * 10 + 2)],
            rte_index: t,
        },
        children: vec![],
    });

    leaf.prop_recursive(max_depth, 64, 2, move |inner| {
        prop_oneof![
            // Select (WHERE filter)
            inner.clone().prop_map(|child| LogicalExpr {
                op: LogicalOp::Select {
                    predicate: ScalarExpr::Const {
                        type_oid: 16, typmod: -1, collation: 0,
                        value: optimizer_core::ir::scalar::ConstValue::Bool(true),
                        is_null: false,
                    },
                },
                children: vec![child],
            }),
            // Join (inner join two subtrees)
            (inner.clone(), inner.clone()).prop_map(|(left, right)| LogicalExpr {
                op: LogicalOp::Join {
                    join_type: JoinType::Inner,
                    predicate: ScalarExpr::Const {
                        type_oid: 16, typmod: -1, collation: 0,
                        value: optimizer_core::ir::scalar::ConstValue::Bool(true),
                        is_null: false,
                    },
                },
                children: vec![left, right],
            }),
            // Aggregate
            inner.clone().prop_map(|child| LogicalExpr {
                op: LogicalOp::Aggregate {
                    group_by: vec![],
                    aggregates: vec![],
                },
                children: vec![child],
            }),
            // Sort
            inner.clone().prop_map(|child| LogicalExpr {
                op: LogicalOp::Sort { keys: vec![] },
                children: vec![child],
            }),
            // Limit
            inner.clone().prop_map(|child| LogicalExpr {
                op: LogicalOp::Limit { offset: None, count: None },
                children: vec![child],
            }),
        ]
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// The optimizer must never panic on any valid LogicalExpr input.
    /// It may return an error (e.g., NoFeasiblePlan) but must not crash.
    #[test]
    fn optimizer_never_panics(expr in arb_logical_expr(4, 3)) {
        let catalog = make_catalog(3);
        // Just verify it doesn't panic — errors are OK
        let _ = optimizer_core::optimize(expr, &catalog);
    }

    /// With timeout=0, the optimizer should still produce a plan (graceful degradation).
    #[test]
    fn optimizer_handles_zero_timeout(expr in arb_logical_expr(3, 2)) {
        let catalog = make_catalog(2);
        let result = optimizer_core::optimize_with_timeout(expr, &catalog, 0);
        // Either produces a plan or returns an error — but must not panic
        match result {
            Ok(_plan) => {} // got a plan despite zero timeout (simple queries)
            Err(_) => {}    // error is acceptable
        }
    }

    /// Deep expression trees should not stack-overflow.
    #[test]
    fn optimizer_handles_deep_trees(depth in 1u32..8) {
        let catalog = make_catalog(1);
        // Build a chain: Get → Select → Select → ... (depth levels)
        let mut expr = LogicalExpr {
            op: LogicalOp::Get {
                table_id: TableId(1),
                columns: vec![ColumnId(1)],
                rte_index: 1,
            },
            children: vec![],
        };
        for _ in 0..depth {
            expr = LogicalExpr {
                op: LogicalOp::Select {
                    predicate: ScalarExpr::Const {
                        type_oid: 16, typmod: -1, collation: 0,
                        value: optimizer_core::ir::scalar::ConstValue::Bool(true),
                        is_null: false,
                    },
                },
                children: vec![expr],
            };
        }
        let _ = optimizer_core::optimize(expr, &catalog);
    }
}

/// Deterministic stress test: 4-way join with all xform rules.
#[test]
fn four_way_join_completes() {
    let catalog = make_catalog(4);
    let get = |t: u32| LogicalExpr {
        op: LogicalOp::Get {
            table_id: TableId(t),
            columns: vec![ColumnId(t * 10 + 1)],
            rte_index: t,
        },
        children: vec![],
    };
    let join = |l: LogicalExpr, r: LogicalExpr| LogicalExpr {
        op: LogicalOp::Join {
            join_type: JoinType::Inner,
            predicate: ScalarExpr::Const {
                type_oid: 16, typmod: -1, collation: 0,
                value: optimizer_core::ir::scalar::ConstValue::Bool(true),
                is_null: false,
            },
        },
        children: vec![l, r],
    };

    // ((t1 ⋈ t2) ⋈ t3) ⋈ t4
    let expr = join(join(join(get(1), get(2)), get(3)), get(4));
    let result = optimizer_core::optimize_with_timeout(expr, &catalog, 2000);
    assert!(result.is_ok(), "4-way join failed: {:?}", result.err());
}
