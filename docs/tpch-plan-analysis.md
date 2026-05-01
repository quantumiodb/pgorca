# TPC-H Plan Analysis: ORCA vs PostgreSQL Native Planner

SF=1, parallelism disabled. ORCA baseline (risk threshold=3).

## Summary Table

| Q | PG (ms) | ORCA (ms) | Ratio | Category |
|---|---------|-----------|-------|----------|
| Q1  | 10,394 | 10,781 | 1.04x | ≈ |
| Q2  |    517 |    868 | 1.68x | regression |
| Q3  |  2,953 |  3,120 | 1.06x | ≈ |
| Q4  |  1,184 |  4,330 | 3.66x | **severe** |
| Q5  |  1,220 |  2,785 | 2.28x | regression |
| Q6  |  2,238 |  2,209 | 0.99x | ≈ |
| Q7  |  2,014 |  2,946 | 1.46x | regression |
| Q8  |    906 |    940 | 1.04x | ≈ |
| Q9  |  8,650 |  4,355 | 0.50x | **ORCA wins 2x** |
| Q10 |  2,933 |  2,920 | 1.00x | ≈ |
| Q11 |    548 |    672 | 1.23x | minor |
| Q12 |  3,616 |  3,421 | 0.95x | ≈ |
| Q13 |  1,696 |  1,766 | 1.04x | ≈ |
| Q14 |  2,235 |  2,180 | 0.98x | ≈ |
| Q15 |  2,232 |  2,219 | 0.99x | ≈ |
| Q16 |  1,137 |    626 | 0.55x | **ORCA wins 1.8x** |
| Q17 |  1,843 |  2,168 | 1.18x | ORCA robust (no index: PG ~1h vs ORCA ~2s); index path suppressed by risk penalty |
| Q18 |  8,228 |  9,211 | 1.12x | minor |
| Q19 |    151 |    158 | 1.05x | ≈ |
| Q20 |    761 |  3,454 | 4.54x | **severe** |
| Q21 |  3,829 |  9,670 | 2.53x | **severe** |
| Q22 |    649 |  1,064 | 1.64x | regression |
| **Total** | **59,934** | **71,863** | **1.20x** | |

---

## Near-Identical (≈): Q1, Q3, Q6, Q8, Q10, Q12, Q13, Q14, Q15, Q19

Both planners choose structurally identical plans. Differences are within measurement noise.

Q12 and Q14: ORCA is marginally faster (0.95x, 0.98x) — slightly better join ordering.

---

## ORCA Wins

### Q9 — ORCA 2x faster (8,650 ms → 4,355 ms)

**Query:** 6-table star join (part, lineitem, partsupp, supplier, nation, orders) with `p_name LIKE '%green%'`.

| | PG | ORCA |
|--|--|--|
| Strategy | 3-level Nested Loop | Linear Hash Join chain |
| lineitem access | Repeated NL rebinds | Single SeqScan |
| part filter | Applied early | Applied early |

PG uses a deep nested loop stack, re-reading lineitem segments per outer row. ORCA builds a single Hash Join pipeline that processes lineitem once. For this star-join with 6 large tables, ORCA's Hash Join ordering avoids redundant I/O.

---

### Q16 — ORCA 1.8x faster (1,137 ms → 626 ms)

**Query:** Count distinct (p_brand, p_type, p_size) combinations from partsupp, filtering out suppliers in a `NOT IN` blacklist.

| | PG | ORCA |
|--|--|--|
| NOT IN handling | SubPlan (Bitmap Heap Scan × 6,088) | Hash Anti Join + NL Left Join (204 lookups) |
| subquery executions | 6,088 | 204 |

PG re-executes the `NOT IN` subplan 6,088 times. ORCA decorrelates it into a Hash Anti Join driven by 204 matching parts, reducing subquery work by 30x.

---

## Severe Regressions

### Q20 — ORCA 4.54x slower (761 ms → 3,454 ms)

**Query:** Find suppliers whose `ps_availqty > 0.5 * SUM(l_quantity)` for a specific year and part color.

```sql
ps_availqty > (SELECT 0.5 * sum(l_quantity) FROM lineitem
               WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
               AND l_shipdate BETWEEN '1994-01-01' AND '1995-01-01')
```

| | PG (lazy SubPlan) | ORCA (eager decorrelation) |
|--|--|--|
| lineitem strategy | 8,508 × Bitmap Index Scan via `lineitem_l_partkey_idx` | Full SeqScan → HashAggregate |
| lineitem rows processed | ~9,400 | **909,455** |
| aggregate groups | 8,508 executions | **543,210 groups** |
| disk spill | none | Batches=5, 11,872 KB |

ORCA eagerly decorrelates the correlated scalar subquery into a full `HashAggregate(l_partkey, l_suppkey)` over the entire year's lineitem data, then joins. PG evaluates the subquery lazily, using the index to fetch only the ~1.1 rows relevant to each of the 8,508 qualifying `(ps_partkey, ps_suppkey)` pairs.

ORCA processes **64x more lineitem rows** than PG.

**Root cause:** The subquery unnesting transform does not compare the cost of the decorrelated plan against the lazy correlated SubPlan. When `outer_qualifying_rows << decorrelated_result_rows`, keeping the SubPlan wins.

---

### Q4 — ORCA 3.66x slower (1,184 ms → 4,330 ms)

**Query:** Count orders by priority where at least one lineitem has `l_commitdate < l_receiptdate`.

```sql
EXISTS (SELECT * FROM lineitem
        WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)
```

| | PG | ORCA |
|--|--|--|
| EXISTS strategy | NL Semi Join + `lineitem_pkey` index (57,218 lookups) | Full SeqScan + HashAggregate dedup |
| lineitem rows accessed | ~114K (57K × ~2 via index) | **3,793,296** |
| deduplication | none (semi-join stops at first match) | HashAggregate, 1.375M groups, 88 MB |
| index used | `lineitem_pkey` leading col `l_orderkey` ✓ | none |

ORCA unnests EXISTS into a full scan + dedup then Hash Join. PG uses a Nested Loop Semi Join, probing `lineitem_pkey` per outer order row and stopping at the first match.

ORCA scans **33x more lineitem data**.

**Root cause:** ORCA has no `EopPhysicalLeftSemiIndexNLJoin` operator. It cannot generate an index-accelerated semi join. The only physical alternatives for a semi join are `CPhysicalLeftSemiHashJoin` (full scan) and `CPhysicalLeftSemiNLJoin` (full inner scan per outer row) — both worse than an indexed probe.

---

### Q21 — ORCA 2.53x slower (3,829 ms → 9,670 ms)

**Query:** Find suppliers with waiting shipments — involves EXISTS and NOT EXISTS on `lineitem` with `l_orderkey` correlation, plus join to `orders`.

| | PG | ORCA |
|--|--|--|
| l3 (NOT EXISTS) | NL Anti Join, `lineitem_pkey` 156,739 lookups × 0.9 rows | Hash Anti Join, full SeqScan 3,793,296 rows + disk spill |
| l2 (EXISTS)     | NL Semi Join, `lineitem_pkey` 13,859 lookups × 0.6 rows | Hash Semi Join, full SeqScan 6,001,215 rows + disk spill |
| orders join     | NL + `orders_pkey` index (8,357 lookups) | Hash Join, full SeqScan 729,413 rows |

Three separate problems compound:

1. **Missing Semi/Anti-Semi Index NL Join operators** (same as Q4): no `EopPhysicalLeftSemiIndexNLJoin` / `EopPhysicalLeftAntiSemiIndexNLJoin` → ORCA always materializes full lineitem for l2 and l3.

2. **Build/probe direction fixed by semantics**: in `CPhysicalLeftSemiHashJoin`, the left (outer) side must be the probe side. The large l2 (6M rows) is always the build side because the SQL semantics require `Anti-Join result` on the left. There is no "right-build semi hash join" operator in either ORCA or the PostgreSQL executor.

3. **orders join: risk penalty + cardinality overestimate**: the orders Inner Join sits at join-stack depth 5 (risk=5). `CostIndexNLJoin` penalizes by `5×` when `risk > threshold(3)`, while `CostHashJoin` has no such penalty. ORCA also overestimates the Semi Join output (38,298 estimated vs 8,357 actual — 4.6×), so the apparent Index NL Join cost is inflated by `4.6 × 5 = 23×`. The Hash Join (729K-row table, 33 MB — under 50 MB spill threshold) wins on paper.

---

## Moderate Regressions

### Q2 — ORCA 1.68x slower (517 ms → 868 ms)

**Query:** Find minimum-cost suppliers per part in a region, using a correlated MIN subquery.

```sql
ps_supplycost = (SELECT min(ps_supplycost) FROM partsupp ps2, supplier s2, nation n2, region r2
                 WHERE ps2.ps_partkey = p.p_partkey AND ...)
```

| | PG (lazy SubPlan) | ORCA (eager decorrelation) |
|--|--|--|
| MIN subquery | SubPlan, 1,207 executions × `partsupp_pkey` index (4 rows each) | Full partsupp SeqScan (800K rows) → HashAggregate (117K groups) |
| partsupp rows | ~4,800 via index | **800,000** (twice — once for agg, once for join) |

Same pattern as Q20: ORCA eagerly decorrelates the MIN subquery into a full `HashAggregate`, then Hash Right Joins the result back to part. PG runs the SubPlan 1,207 times using `partsupp_pkey(ps_partkey)` to fetch ~4 rows each. PG touches 1/166 of the partsupp data that ORCA touches.

---

### Q5 — ORCA 2.28x slower (1,220 ms → 2,785 ms)

**Query:** Revenue by nation for suppliers and customers in the ASIA region.

| | PG | ORCA |
|--|--|--|
| orders→lineitem join | NL + Index Scan `lineitem_pkey` (45,980 lookups × ~4 rows = 184K rows) | Hash Join, full Seq Scan lineitem → 6,001,064 rows probed |
| lineitem rows accessed | **184K** | **6,001,064** (33× more) |
| Intermediate before supplier join | 183,952 rows | 649,143 rows |
| Group cardinality shown in EXPLAIN | 7,358 (rows estimate) | **2,596,573** (overestimate) |
| Actual final join result | ~649K | ~649K |

ORCA and PG agree on the high-level join order: both start from `nation ⋈ region → 5 ASIA nations → customer(30K) → orders(46K)`. The difference is the final step: PG uses a Nested Loop + Index Scan on `lineitem_pkey` (leading column `l_orderkey`) to fetch only the relevant ~184K lineitem rows, while ORCA performs a full Seq Scan of 6M lineitem rows as the Hash Join probe side.

**Root cause (debugged via source-level join stats trace):**

Two compounding issues identified by instrumenting `CJoinStatsProcessor::SetResultingJoinStats`:

**1. Memo group cardinality overestimate (2,596,573)**

ORCA's memo stores one cardinality per group, set from the *first* join ordering explored for that group. For the full 6-table join, an early-explored bad split `region(1) ⋈ {customer-orders-lineitem-supplier-nation}` is evaluated first. The 5-table sub-group `{c,o,l,s,n}` gets cardinality **12,982,863** from the bad ordering:

```
customer(150K) ⋈ supplier(10K) [c_nationkey=s_nationkey]
  = 150,000 × 10,000 / 25  =  60,000,000  (no nation/region filter applied)
(customer-supplier)(60M) ⋈ orders(229K)  =  91,959,700
(customer-orders-supplier)(91M) ⋈ lineitem(6M)  =  12,982,863
```

Then `region(1) ⋈ 12,982,863` on `r_regionkey=n_regionkey` → `12,982,863 / 5 = 2,596,573`. This becomes the stored group cardinality. The correct ordering later explored (nation-region first → 649K rows) does NOT update the stored stats.

The root cause of the bad sub-group estimate: `customer ⋈ supplier` is computed *before* the nation/region filter is pushed down. With 25 nations uniformly distributed, the join formula gives `150K × 10K / 25 = 60M` — but after the ASIA filter reduces nations to 5, the actual result is only `30K × 2K / 5 = 12K`. ORCA's estimator produces a 5,000× overestimate for this particular sub-group.

**2. Hash Join preferred over Index NL Join for orders→lineitem**

At join depth 4 (nation→customer→orders→lineitem), the risk counter reaches `risk=4`. The risk penalty applies to NL/Index NL Joins (`cost × risk`) but NOT to Hash Joins. At `risk=4`, Index NL Join cost is 4× inflated, making the Hash Join appear cheaper. ORCA chooses Hash Join + Seq Scan lineitem (6M rows) over the correct Index NL Join (184K rows via `lineitem_pkey`).

The correct 2-predicate join stats for the final step (`outer=649,143 ⋈ supplier(10,000) npreds=2`) are computed correctly at **649,143** by `SetResultingJoinStats`. The 2,596,573 shown in EXPLAIN is the stale memo cardinality from issue #1 above, not the cost-comparison stats.

---

### Q7 — ORCA 1.46x slower (2,014 ms → 2,946 ms)

**Query:** Volume shipping between FRANCE and GERMANY, 1995–1996.

Same structural issue as Q5: ORCA starts with a full lineitem SeqScan (1.8M rows after date filter) as the probe side of the outer Hash Join. PG builds from nation (2 rows FRANCE/GERMANY) → supplier (small) → orders/customer → lineitem via NL.

ORCA total I/O: 127K buffer accesses. PG total: 521K buffer accesses — but PG's extra reads are small cached pages (NL inner re-reads of nation and supplier tables), while ORCA performs a single large sequential I/O. Despite lower raw I/O, ORCA is 1.46x slower because its Hash Join intermediate results are larger and the hash table probe overhead outweighs PG's NL overhead at these cardinalities.

---

### Q22 — ORCA 1.64x slower (649 ms → 1,064 ms)

**Query:** Count customers with no orders, grouped by phone country code prefix.

| | PG | ORCA |
|--|--|--|
| NOT EXISTS | Hash Right Anti Join (orders=probe, customer=build, 19K rows) | Hash Left Join + Filter `COALESCE(count(*), 0) = 0` |
| orders rows consumed | 1,500,000 (streaming probe) | 1,500,000 (streaming probe) |
| customer materialization | 19K rows in hash table | 19K rows in hash table |
| extra work | none | Count aggregation per customer, then filter |

Both scan all 1.5M orders. PG uses `Hash Right Anti Join` which terminates the match early (marks customers as "matched" and emits only unmatched ones). ORCA lacks a direct Anti Join path here — it performs a Hash Left Join then counts orders per customer via aggregation (`COALESCE(count(*), 0) = 0`), adding an unnecessary aggregation step over the matched rows.

---

## Minor Regressions

### Q17 — ORCA 1.18x slower (1,843 ms → 2,168 ms)

**Query:** Average yearly order quantity for a specific part brand/container combination.

```sql
l_quantity < (SELECT 0.2 * avg(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)
```

| | PG | ORCA (baseline) | ORCA (risk=5) |
|--|--|--|--|
| avg computation | SubPlan × 6,088, via `lineitem_partkey_idx` | HashAggregate (lineitem full scan) + second full lineitem scan | Index Scan `lineitem_partkey_idx` 204 lookups |
| lineitem passes | 1 full scan + 6,088 index reads | **2 full scans** | 204 index lookups |
| execution time | 1,843 ms | 2,168 ms | **102 ms** |

**Index dependency of each approach:**

| Scenario | PG (lazy SubPlan) | ORCA baseline (decorrelation) |
|--|--|--|
| `lineitem_partkey_idx` exists | 1,843 ms ✓ | 2,168 ms |
| **index missing** | **6,088 × 6M rows ≈ ~1 hour** ✗ | **~2 seconds (unchanged)** ✓ |

PG's SubPlan approach is fundamentally dependent on the index: without it, each of the 6,088 SubPlan executions performs a full 6M-row lineitem scan, making the query infeasible. ORCA's eager decorrelation scans lineitem once for the HashAggregate regardless of index availability — **O(lineitem)** total work, not **O(outer × lineitem)**.

This means ORCA's approach is the robust, index-independent choice. The 1.18x regression in the TPC-H benchmark (which happens to have the index) is misleading: ORCA is actually correct here; it simply cannot also exploit the index path.

At `risk=5`, ORCA uses `lineitem_partkey_idx` (204 lookups, ~30 rows each) to compute the per-part average — equivalent to PG's SubPlan approach. This produces a 21x speedup (102 ms vs 2,168 ms, or 18x faster than PG). With the index and the right risk threshold, ORCA gets the best of both worlds.

**Root cause:** the Index NL Join needed to exploit `lineitem_partkey_idx` is suppressed by the risk=4 penalty (join stack depth 4 → risk=4 > threshold=3 → cost multiplied by 4×). Raising the threshold to 5 restores the correct plan for Q17, but doing so globally causes Q9 and Q20 to regress (see risk threshold experiment). A targeted fix is needed.

**Fix direction:** When the decorrelated HashAggregate plan exists as the fallback, ORCA should additionally generate the Index NL Join alternative if an index covers the inner join key, and let the cost model choose. This is a targeted xform-level change that does not affect the global risk threshold.

---

### Q11 — ORCA 1.23x slower (548 ms → 672 ms)

Residual regression after the non-leading column index scan fix (commit `46b96a6`). The Q11 fix brought execution from 11,281 ms to 672 ms. The remaining 22% gap vs PG's 548 ms is attributable to ORCA's higher HashAggregate overhead for the value aggregation subquery.

---

### Q18 — ORCA 1.12x slower (8,228 ms → 9,211 ms)

Both plans use identical join structure (Hash Join: customer ⋈ orders ⋈ lineitem with a grouped subquery on lineitem). The difference is that ORCA's Hash Join for the large lineitem aggregate spills to disk (temp read/write = 22,399 pages each), adding ~1 second of I/O overhead. PG keeps the same join in shared buffers. Work_mem tuning would eliminate this gap.

---

## Root Cause Summary

### Pattern 1 — Eager subquery decorrelation (Q2, Q20)

ORCA eagerly unnests correlated scalar subqueries (MIN, SUM) into full-table HashAggregates before joining with the outer query. PG evaluates them lazily per outer row via indexes. When `outer_qualifying_rows ≪ full_table_rows`, the lazy approach wins decisively.

**Fix direction:** The unnesting transform should estimate and compare the cost of the decorrelated plan vs correlated execution based on `outer_rows × subplan_cost`. If `outer_rows × index_scan_cost < full_agg_cost`, keep the SubPlan.

### Pattern 2 — Missing Semi/Anti-Semi Index NL Join operators (Q4, Q21)

ORCA's physical operator set has `EopPhysicalInnerIndexNLJoin` and `EopPhysicalLeftOuterIndexNLJoin` but **no** `EopPhysicalLeftSemiIndexNLJoin` or `EopPhysicalLeftAntiSemiIndexNLJoin`. `CXformImplementIndexApply` only instantiates the inner/outer-join variants.

Without these operators, every EXISTS/NOT EXISTS subquery that becomes a semi/anti-semi join is forced to use Hash Semi/Anti Join with a full inner table scan. PG can use Nested Loop Semi/Anti Join + index probe, reading only the rows relevant to each outer tuple.

**Fix direction:** Implement `CPhysicalLeftSemiIndexNLJoin` and `CPhysicalLeftAntiSemiIndexNLJoin`, extend `CXformImplementIndexApply` to instantiate them. This is the highest-leverage single fix: resolves Q4 and Q21 simultaneously.

### Pattern 3 — Risk penalty asymmetry (Q17, Q21 orders join)

`CostIndexNLJoin` and `CostNLJoin` multiply their cost by `risk` when `risk > threshold(3)`, but `CostHashJoin` has no corresponding penalty. At deep join-stack levels (risk ≥ 4), this systematically biases ORCA toward Hash Joins regardless of actual cardinalities.

The penalty is intended to guard against NL Join cost explosions when outer cardinality is underestimated. But it fires identically when the outer cardinality is overestimated (Q17: 38K estimated vs 8K actual), inflating the Index NL Join cost without cause.


### Pattern 4 — Join order suboptimal for selective dimension chains (Q5, Q7)

For queries that join a large fact table (lineitem) through selective dimension filters (region → nation → supplier), both ORCA and PG correctly identify the small dimension chain as the driving side at the high level. The actual divergence is in the **orders→lineitem step**: PG uses a Nested Loop + Index Scan on `lineitem_pkey` (leading column `l_orderkey`), fetching only the 184K lineitem rows relevant to the 45,980 qualifying orders. ORCA uses a Hash Join with a full 6M-row Seq Scan of lineitem.

**Root cause (verified by join stats instrumentation):** Two compounding factors:

1. **Memo group cardinality overestimate**: When ORCA's DP first explores the join group containing `{customer, supplier}` without the nation/region filter, it computes `150K × 10K / 25 = 60M` rows — correct for unfiltered data but 5,000× too large for the ASIA-filtered case. This propagates to a 2,596,573-row estimate for the full 6-table join group (vs the correct 649,143), stored in the memo and not updated when better orderings are found later.

2. **Risk penalty on Index NL Join**: At join depth 4 (nation→customer→orders→lineitem), `risk=4 > threshold(3)`, so `CostIndexNLJoin` is multiplied by 4. `CostHashJoin` has no risk penalty. This inflates the Index NL Join cost by 4×, making the Hash Join + Seq Scan appear cheaper even though it processes 33× more lineitem rows.

**Fix direction (two independent paths):**
- **Cardinality**: Fix the memo to store per-ordering stats or to update group stats when a lower-cardinality ordering is found. Alternatively, prevent ORCA from joining `customer ⋈ supplier` before the nation/region filter is pushed down (filter push-through improvement).
- **Index NL Join bias**: Apply the risk penalty symmetrically to Hash Joins as well, or add a risk penalty only when the outer cardinality is *underestimated* (not when it's overestimated).
