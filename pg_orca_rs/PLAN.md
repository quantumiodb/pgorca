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
| WP5 | Search engine (Task-based Scheduler v0.2) | 5 | ~365 | Done |
| WP6 | Phase 1 inbound (Query → LogicalExpr) | 8 | ~380 | Done |
| WP7 | Phase 3 outbound (PhysicalPlan → PlannedStmt) + hook | 8 | ~465 | Done |
| **Total** | | **44** | **~2,480** | **Done** |

---

## Milestone 3: Filter + IndexScan (DONE)

**Goal**: `SELECT * FROM t WHERE a > 10` handled by orca.

- [x] Scalar expression translation: PG `Expr` → `ScalarExpr`
- [x] `LogicalOp::Select` implementation
- [x] CatalogSnapshot: read pg_statistic (ndistinct, null_fraction, histogram)
- [x] Selectivity estimation for basic predicates
- [x] `Get2IndexScan` implementation rule
- [x] Integration tests: single-table with WHERE, index selection

---

## Milestone 4: Two-table Join (DONE)

**Goal**: `SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id` handled by orca.

- [x] Phase 1: translate JoinExpr → `LogicalOp::Join`
- [x] Implementation rules: HashJoin, NestLoop, MergeJoin
- [x] Phase 3: build_hash_join, build_nestloop, build_merge_join
- [x] Var reference correctness (OUTER_VAR / INNER_VAR)
- [x] Integration tests passing

---

## Milestone 5: Join Reorder (DONE)

**Goal**: 3+ table joins benefit from Cascades search.

- [x] `JoinCommutativity` transformation rule (A ⋈ B → B ⋈ A)
- [x] `JoinAssociativity` transformation rule ((A ⋈ B) ⋈ C → A ⋈ (B ⋈ C))
- [x] `orca.join_order_threshold` GUC enforcement
- [x] Branch-and-bound pruning verification with 3+ tables

---

## Milestone 6: Aggregation (DONE)

- [x] Phase 1: translate groupClause → `LogicalOp::Aggregate`
- [x] rules: Agg2HashAgg, Agg2SortAgg, Agg2PlainAgg
- [x] Phase 3: build_agg plan builder
- [x] Integration tests passing

---

## Milestone 7: Sort + Limit + Distinct (DONE)

- [x] ORDER BY → Sort
- [x] LIMIT/OFFSET → Limit
- [x] DISTINCT → Sort + Unique
- [x] Property framework foundations (v0.3)
- [x] Integration tests passing

---

## Milestone 8: BitmapScan + IndexOnlyScan (DONE)

- [x] `Get2BitmapScan` implementation rule
- [x] Phase 3: BitmapHeapScan + BitmapIndexScan generation
- [x] `Get2IndexOnlyScan` implementation rule (covering index)

---

## Milestone 9: Performance & Stability (DONE)

- [x] Fuzz testing with proptest (random LogicalExpr trees)
- [ ] TPC-H benchmark (Q3, Q5, Q10 subset)
- [x] Search timeout (`orca.xform_timeout_ms`) enforcement
- [x] Damping factor tuning for selectivity estimation
- [x] Memo group limit enforcement with graceful degradation

---

## Milestone 10: Advanced Cardinality (DONE)

- [x] ColumnConstraintGraph: equivalence classes + interval constraints
- [x] Contradiction detection (A=B AND A>10 AND B<5 → sel=0)
- [ ] Kruskal MST for cyclic join key cardinality (TODO)
- [x] Better histogram/MCV utilization enhancement

---

## Milestone 11: Advanced Features

- [ ] Parallel search with rayon (v0.4 path)
- [ ] Parallel Query support (Parallel SeqScan, Gather/GatherMerge)
- [ ] CTE support (producer/consumer)
- [ ] Window function support
- [ ] DML support (INSERT/UPDATE/DELETE)

---

## Milestone 12: Partitioning & Pruning

**Goal**: Handle large-scale partitioned tables efficiently.

- [ ] Static Partition Pruning (exclude child relations during optimization)
- [ ] Dynamic Partition Elimination (runtime pruning based on Join results)
- [ ] `PartitionSelector` operator implementation (ref: GPORCA)
- [ ] Parallel Append support for multi-worker partition scans

---

## Milestone 13: Advanced Indexing & Scalar Match

- [ ] GIN, GiST, BRIN index support and cost modeling
- [ ] Functional Index matching (e.g. `WHERE upper(name) = '...'`)
- [ ] Expression Index support
- [ ] Visibility Map awareness for Index-Only Scan costing

---

## Milestone 14: Subquery Unnesting & Decorrelation

**Goal**: Complex subquery transformation to Join.

- [ ] Pull-up Subqueries (converting SubLinks to Joins)
- [ ] Semi-Join / Anti-Join transformation rules
- [ ] Decorrelation via `DependentJoin` (ref: Neumann 2025)
- [ ] Lateral Join support

---

## Milestone 15: Extended Stats & Disk Spill Costing

- [ ] Support for Multi-column statistics (`pg_statistic_ext`)
- [ ] Dependency-aware selectivity (e.g. city/zipcode correlation)
- [ ] Memory-aware costing: Disk spill overhead for HashJoin/Sort when exceeding `work_mem`

---

## Milestone 16: Plan Stability & Management

- [ ] Rule-level GUC toggles (e.g. `SET orca.enable_mergejoin = off`)
- [ ] Plan Baseline / SPM support
- [ ] LLM-integrated Explain Advisor (Interactive diagnostics)

---

## Milestone 17: Comprehensive PostgreSQL Type Support

**Goal**: Support industrial-strength data types and safe Varlena handling.

- [ ] Refactor `scalar_convert.rs` to use `pgrx` Datum API for safe De-toast/De-compress.
- [ ] Support complex types: `Numeric`, `Date`, `Timestamp/tz`, `Interval`, `Jsonb`.
- [ ] Collection support: `Array` constants and `ScalarArrayOp` optimization.
- [ ] Implement missing scalar expressions: `CaseExpr`, `Coalesce`, `BooleanTest`.
- [ ] Type-aware selectivity: Linear mapping for date/time in `const_to_f64`.
