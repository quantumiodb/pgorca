# pg_orca_rs — Implementation Plan

## Overview

pg_orca_rs is a Rust Cascades query optimizer for PostgreSQL, implemented as a pgrx extension. It replaces PG's standard planner via `planner_hook` for supported queries, with transparent fallback.

- **Architecture**: `optimizer_core` (pure safe Rust) + `pg_bridge` (pgrx extension)
- **Design spec**: [DESIGN.md](../DESIGN.md) (2670 lines)
- **Target**: PostgreSQL 17.4, pgrx 0.17.0, Rust 1.92+

---

## Milestone 1+2: Cascades Core + `SELECT * FROM t` (DONE)

**Goal**: End-to-end path where `SELECT * FROM t` goes through our Cascades engine.

### Work Packages (completed)

| WP | Name | Files | Lines | Status |
|----|------|-------|-------|--------|
| WP0 | Workspace scaffolding | 4 | ~90 | Done |
| WP1 | IR types (LogicalOp, PhysicalOp, ScalarExpr) | 6 | ~400 | Done |
| WP2 | Memo (arena, UnionFind, fingerprint dedup, OnceLock props) | 8 | ~480 | Done |
| WP3 | Cost model (CatalogSnapshot, SeqScan formula) | 3 | ~185 | Done |
| WP4 | Rule system + Get2SeqScan | 3 | ~115 | Done |
| WP5 | Search engine (optimize_group recursive) + plan extraction | 5 | ~365 | Done |
| WP6 | Phase 1 inbound (Query → LogicalExpr) | 8 | ~380 | Done |
| WP7 | Phase 3 outbound (PhysicalPlan → PlannedStmt) + hook | 8 | ~465 | Done |
| **Total** | | **44** | **~2,480** | **Done** |

### Dependency graph

```
WP0 → WP1 → WP2 → WP4 → WP5 → WP7
           → WP3 ────────↗       ↗
           → WP6 ────────────────↗
```

### Verified results

```sql
-- orca handles simple SELECT
EXPLAIN SELECT * FROM orders;
--  Seq Scan on orders  (cost=0.00..2.00 rows=100 width=4)
--  Optimizer: pg_orca

-- WHERE clause falls back to standard planner
EXPLAIN SELECT * FROM orders WHERE id > 50;
--  Seq Scan on orders  (cost=0.00..2.25 rows=51 width=4)
--    Filter: (id > 50)
--  Optimizer: Postgres
```

### Key technical decisions

1. pgrx 0.17.0 + PG 17.4, `extern "C-unwind"` for hooks
2. `shared_preload_libraries = 'pg_bridge'` for hook registration
3. `ExplainOneQuery_hook` displays `Optimizer: pg_orca` / `Postgres`
4. `thread_local! ORCA_PLANNED` flag tracks per-query optimizer usage
5. PlannedStmt.rtable shares Query.rtable (no copy)
6. All palloc in Phase 3 runs in planner's MemoryContext

### Unit tests (7 passing)

- `utility::union_find::tests::test_basic`
- `memo::memo::tests::test_insert_and_dedup`
- `memo::memo::tests::test_separate_groups`
- `memo::memo::tests::test_add_to_existing_group`
- `cost::model::tests::test_seq_scan_cost`
- `rules::impl_rules::scan::tests::test_get2seqscan`
- `search::engine::tests::test_end_to_end_single_table`

---

## Milestone 3: Filter + IndexScan

**Goal**: `SELECT * FROM t WHERE a > 10` handled by orca.

### Tasks

- [ ] Scalar expression translation: PG `Expr` → `ScalarExpr` (OpExpr, BoolExpr, Const, Var, FuncExpr, NullTest)
- [ ] `LogicalOp::Select` with predicate → qual on SeqScan
- [ ] CatalogSnapshot: read pg_statistic (ndistinct, null_fraction, histogram)
- [ ] Selectivity estimation for basic predicates (=, <, >, IS NULL)
- [ ] `Get2IndexScan` implementation rule (match predicate to index leading columns)
- [ ] Phase 3: `build_index_scan()` plan builder
- [ ] Phase 3: `build_op_expr()`, `build_const()`, `build_func_expr()` expression builders
- [ ] Remove WHERE fallback in query_check.rs
- [ ] Integration tests: single-table with WHERE, index selection

---

## Milestone 4: Two-table Join

**Goal**: `SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id` handled by orca.

### Tasks

- [ ] Phase 1: translate JoinExpr → `LogicalOp::Join`
- [ ] Phase 1: support multi-table range tables
- [ ] `Join2HashJoin`, `Join2NestLoop`, `Join2MergeJoin` implementation rules
- [ ] Phase 3: `build_hash_join()` with auto T_Hash wrapping
- [ ] Phase 3: `build_nestloop()`, `build_merge_join()`
- [ ] Phase 3: TranslateContext propagation (child output → parent Var reference)
- [ ] Var reference correctness after join (OUTER_VAR / INNER_VAR)
- [ ] Cost formulas for HashJoin, NestLoop, MergeJoin
- [ ] Integration tests: two-table join with all three methods

---

## Milestone 5: Join Reorder

**Goal**: 3+ table joins benefit from Cascades search.

### Tasks

- [ ] `JoinCommutativity` transformation rule (A ⋈ B → B ⋈ A)
- [ ] `JoinAssociativity` transformation rule ((A ⋈ B) ⋈ C → A ⋈ (B ⋈ C))
- [ ] `orca.join_order_threshold` GUC enforcement
- [ ] Branch-and-bound pruning verification with 3+ tables
- [ ] Comparison with PG `join_collapse_limit` behavior
- [ ] Integration tests: TPC-H Q3 style (3-table join)

---

## Milestone 6: Aggregation

**Goal**: `GROUP BY` / `HAVING` queries.

### Tasks

- [ ] Phase 1: translate groupClause → `LogicalOp::Aggregate`
- [ ] Phase 1: translate havingQual → Select above Aggregate
- [ ] `Agg2HashAgg`, `Agg2SortAgg`, `Agg2PlainAgg` implementation rules
- [ ] Phase 3: `build_agg()` plan builder
- [ ] Phase 3: `build_aggref()` expression builder
- [ ] Enforcer insertion: Sort for SortAgg
- [ ] Cost formulas for Agg variants
- [ ] Remove GROUP BY / HAVING / aggregates fallback in query_check.rs
- [ ] Integration tests: GROUP BY, HAVING, count/sum/avg

---

## Milestone 7: Sort + Limit + Distinct

**Goal**: ORDER BY, LIMIT/OFFSET, DISTINCT.

### Tasks

- [ ] `Sort2Sort` implementation rule
- [ ] `Limit2Limit` implementation rule
- [ ] `Distinct2Unique` implementation rule (Sort + Unique)
- [ ] Phase 3: `build_sort()`, `build_limit()`, `build_unique()` plan builders
- [ ] Property framework: ordering enforcement and delivery
- [ ] IncrementalSort for partially sorted input
- [ ] Remove ORDER BY / LIMIT / DISTINCT fallback in query_check.rs
- [ ] Integration tests: ORDER BY + LIMIT, DISTINCT

---

## Milestone 8: BitmapScan + IndexOnlyScan

### Tasks

- [ ] `Get2BitmapScan` implementation rule
- [ ] Phase 3: two-layer BitmapHeapScan + BitmapIndexScan generation
- [ ] `Get2IndexOnlyScan` implementation rule (covering index)
- [ ] Phase 3: `build_index_only_scan()` with indextlist
- [ ] Integration tests: bitmap scan selection, index-only scan

---

## Milestone 9: Performance & Stability

### Tasks

- [ ] Fuzz testing with proptest (random LogicalExpr trees)
- [ ] TPC-H benchmark (Q3, Q5, Q10 subset)
- [ ] Search timeout (`orca.xform_timeout_ms`) enforcement
- [ ] Damping factor tuning for selectivity estimation
- [ ] Memo group limit enforcement with graceful degradation

---

## Milestone 10: Advanced Cardinality (optd-inspired)

### Tasks

- [ ] ColumnConstraintGraph: equivalence classes + interval constraints
- [ ] Contradiction detection (A=B AND A>50 AND B<30 → sel=0)
- [ ] Kruskal MST for cyclic join key cardinality
- [ ] Better histogram/MCV utilization from pg_statistic
- [ ] Integration with existing CatalogSnapshot

---

## Milestone 11: Advanced Features

### Tasks

- [ ] Parallel search with rayon
- [ ] Subquery support (DependentJoin + decorrelation, ref: Neumann 2025)
- [ ] CTE support (producer/consumer)
- [ ] Window function support
- [ ] DML support (INSERT/UPDATE/DELETE)
