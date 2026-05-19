# pg_orca TODO

## Status snapshot (as of 2026-05-19, commit `29fc891`)

| Workload | ORCA vs PG (par=0) | ORCA vs PG (par=2) | Coverage edge | Source |
|---|---|---|---|---|
| TPC-H sf=10 | 1.12× geomean (ORCA total 636.7 s vs PG 657.9 s) | **0.49× geomean** (ORCA 750.4 s vs PG 328.2 s) | — | [`test/bench/results/tpch_sf10_three_way_summary.md`](test/bench/results/tpch_sf10_three_way_summary.md) |
| TPC-H sf=5  | 1.07× geomean | — | — | par=0 baseline |
| TPC-DS sf=5 | sum: ORCA 1458 s vs PG 1492 s (common 90) | — | **9 / 99 queries ORCA completes that PG times out (≥120 s)** | `test/bench/results/tpcds_tpcds_sf5_compare.csv` |
| TPC-DS sf=1 | ORCA 627 s vs PG 513 s (common 94) — ORCA 1.22× slower in total | — | **5 / 99 queries ORCA completes that PG times out (≥120 s)** | `test/bench/results/run_tpcds_sf1_20260509_1112.log` |

## Strengths — keep leveraging

1. **Correlated subquery decorrelation — ORCA's signature capability.**
   - ORCA rewrites `Apply` algebraically into a regular `Join` via the `CXformApply2Join` family (`libgpopt/src/xforms/CXformInnerApply2InnerJoin.cpp`, `CXformLeftSemiApply2LeftSemiJoin.cpp`, `CXformLeftOuterApply2LeftOuterJoin.cpp`, and ~10 sibling xforms). The result is a single optimizable plan; PG often falls back to a `SubPlan` / `InitPlan` executed once per outer row.
   - PG handles `EXISTS` / `NOT EXISTS` adequately; **everything else** (`= (SELECT ...)`, `IN (SELECT ...)`, nested correlation, subquery with aggregate) is where ORCA opens large gaps.
   - Wins (sample, par=0):

      | Query | Pattern | Speedup |
      |---|---|---:|
      | TPC-H Q17 | `l_quantity < 0.2 * AVG(...) WHERE p_partkey = ...` | **20.7×** (par=0) / 20.4× (par=2) |
      | TPC-DS Q41 | correlated IN + EXISTS | **156×** at sf=5 |
      | TPC-DS Q21 | correlated min/max with HAVING | 7.3× |
      | TPC-DS Q17 | correlated aggregate | 7.3× |
   - **Drives the TPC-DS coverage edge**: 7 of 9 PG-timeout queries at sf=5 (Q1, Q6, Q11, Q14, Q30, Q74, Q81 — Q4 and Q95 mix in window/roll-up) are correlated-subquery dominated.
   - **Orthogonal to parallelism** — Q17 still wins 20× at par=2. Structural plan-shape decision, not hardware throughput.

2. **Cost-based exhaustive join-order enumeration (DPv2).**
   - `libgpopt/src/xforms/CJoinOrderDPv2.cpp` + the commutativity / associativity xforms enumerate the full join space under a cost model that uses ORCA's cardinality estimates (not PG's). Result: shape selection (left-deep vs bushy vs star) is cardinality-driven, not heuristic.
   - PG comparison: `geqo_threshold = 12` switches to genetic algorithm above 12 relations; below it uses greedy DP. Neither path uses bushy plans aggressively, and both are sensitive to PG's row estimates.
   - Wins (sample, par=0):

      | Query | Pattern | Speedup |
      |---|---|---:|
      | TPC-H Q4  | 2-way + correlated EXISTS, anti-semi shape | 2.55× |
      | TPC-H Q10 | 4-way + filter + ORDER BY LIMIT | 1.71× |
      | TPC-H Q21 | 4-way + NOT EXISTS, anti-semi tree | 1.26× |
      | TPC-DS Q25 | 6-way roll-up join | 9.0× |
      | TPC-DS Q29 | 6-way roll-up join | 7.0× |
   - Controlled by `optimizer_join_order_threshold = 10` (in `gpopt/utils/COptTasks.cpp:402` — currently not exposed as a `pg_orca.*` GUC; raising it widens the search at the cost of more planning time).
   - **Orthogonal to parallelism** — join-tree shape decisions survive intact at par=2.

3. **Statistics propagation through GROUP BY / DISTINCT in CTE / subquery is correct.**
   - `CGroupByStatsProcessor::CalcGroupByStats` preserves the full input histogram (incl. NDV) on grouping columns; CTE consumer inherits stats via colid mapping.
   - On TPC-DS Q31 this gives **3.28× vs PG** at sf=5 (PG: 37.4 s vs ORCA: 11.4 s) — same root cause as upstream Richard Guo's 2026-04 patch (`stadistinct` propagation through GROUP BY in CTEs).
   - Once that patch lands in PG, this particular gap will narrow. Hunt for the next family of stats-propagation wins before then (multi-column NDV, cross-column correlation, correlated-subquery decorrelation).

4. **TPC-DS coverage edge — ORCA solves queries PG cannot complete.**
   - At `statement_timeout=120 s`, sf=5: ORCA finishes **9 / 99** queries that PG times out on (Q1, Q4, Q6, Q11, Q14, Q30, Q74, Q81, Q95). Same setting at sf=1: **5 / 99** (Q1, Q4, Q6, Q11, Q74). These are not "ORCA marginally faster" — they are **complex multi-join / CTE / window queries where PG's planner picks a plan so bad it never returns**.
   - **Full 99-query total once PG timeouts are counted (floor: 120 s each)**:

      | Suite | ORCA total (all 99) | PG total (lower bound) | ORCA edge |
      |---|---:|---:|---:|
      | TPC-DS sf=5 | 1,836 s (1,458 s common + 378 s on the 9 PG-timeouts) | ≥ 2,572 s (1,492 s + 9 × 120 s) | **≥ 1.40×** |
      | TPC-DS sf=1 | 627 s | ≥ 1,113 s (513 s + 5 × 120 s) | **≥ 1.77×** |

      `120 s` is the timeout floor; PG's true runtime on those queries is open-ended (could be many minutes — see Richard Guo's 25.6 min on Q31 in upstream master). The real ORCA edge is strictly larger than the numbers above.
   - Within the head-to-head subset at sf=5, ORCA's distribution is bimodal: many small losses (0.4–0.9×) but a few massive wins. Top wins (sf=5): **Q41 156×, Q25 9.0×, Q21 7.3×, Q17 7.3×, Q29 7.0×, Q31 5.8×, Q39 5.8×, Q55 4.9×**. Sum-of-times still nets in ORCA's favor on the common 90 (1458 s vs 1492 s).
   - These wins cluster around: multi-CTE self-joins (Q31/Q39), correlated subqueries (Q17/Q41), and roll-up aggregations (Q21/Q29). Same family as TPC-H Q17 — **structural plan-shape wins**.
   - Worst losses (sf=5): Q61 (0.001×, ORCA 1270 ms vs PG 0.6 ms — pure planning overhead on a trivial query, see weakness #1), Q82/85/37/91/92 (0.06–0.18×). These are short queries where ORCA's planning cost dominates, or specific plan-shape regressions worth case-by-case investigation.

5. **Single-thread baseline is the right benchmark for optimizer-quality work.** par=2 numbers reflect hardware throughput, not optimizer changes.

## Weaknesses — actively hurting us

### 1. Planning time ⚠️ kills short queries

**Impact**: ORCA's CBO is consistently heavier than PG planner.

| Workload | ORCA planning | PG planning |
|---|---:|---:|
| Simple point lookup (1-row return) | 13.7 ms | 0.25 ms |
| Simple aggregate over 1 table | 3.8 ms | 0.14 ms |
| TPC-H Q4 (Inner Join + EXISTS + GroupBy) | 169 ms | 5.5 ms |
| TPC-DS Q31 (2 CTEs, 6-way self-join) | 192 ms | 3.6 ms |

For OLTP / short-running queries (< 50 ms exec), the planning overhead **dominates total latency**. A point lookup under ORCA takes ~14 ms wall-clock where PG takes < 1 ms — even if the executed plan is identical.

**Why this matters**:
- Disqualifies pg_orca from any latency-sensitive workload (web request paths, transactional code).
- TPC-H/TPC-DS benchmarks are exec-bound so the overhead is amortized — production mixed workloads are not.

**Possible mitigation directions** (no commitment yet):
- [ ] **Query plan cache / shape-keyed memoization** — cache the optimized DXL plan keyed by a normalized query shape + statistics generation number. Most OLTP traffic is small numbers of distinct query shapes; reusing planning would cut amortized cost dramatically. Requires invalidation on stats / DDL changes (hook into PG's `invalidate_*_callbacks`).
- [ ] **Heuristic fast-path bypass** — let `pg_orca` short-circuit to PG planner for queries below a complexity threshold (single table, no joins, no aggregates, equality predicates on indexed columns). Add GUC `pg_orca.fast_path_threshold` (e.g. join_count). Cheapest win.
- [ ] **Profile ORCA's search loop**. The 169 ms / 192 ms numbers suggest the cost is in Memo group exploration + transformation rules. Identify the dominant xforms with a flamegraph; some may be disable-able for simple shapes via existing `optimizer.*` GUCs.
- [ ] **`optimizer_join_order_threshold` / `optimizer_search_limit`** — already exist in ORCA but not exposed as PG-level GUCs in pg_orca. Wire them through and measure.

### 2. Existing items

- [ ] Partitioned tables — translation layer for `PartitionedTable` scans / dynamic partition pruning (DXL has `PartitionSelector` but stubs in `compat/cdb/`).

## References

- TPC-H three-way analysis: `test/bench/results/tpch_sf10_three_way_summary.md`
- par=2 gap + engineering path: `test/bench/results/tpch_sf10_par2_summary.md`
- par=0 baseline: `test/bench/results/tpch_sf10_summary.md`
- TPC-DS Q31 stats propagation win: documented inline in 2026-05-19 conversation transcript
