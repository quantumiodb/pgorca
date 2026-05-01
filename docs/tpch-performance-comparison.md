# TPC-H Performance Comparison: ORCA vs PostgreSQL Native Planner

Test environment: TPC-H SF=2, PostgreSQL 18, single-node, parallelism disabled
(`max_parallel_workers_per_gather=0`), warm buffer cache.

## Execution Time

| Query | PG (ms) | ORCA (ms) | Ratio (ORCA/PG) | Result |
|-------|--------:|----------:|----------------:|--------|
| Q1    | 10,500  | 10,971    | 1.04x           | ≈      |
| Q2    |    488  |  2,243    | 4.59x           | PG     |
| Q3    |  2,812  |  3,167    | 1.13x           | ≈      |
| Q4    |  1,134  |  1,179    | 1.04x ✓ (fixed) | ≈      |
| Q5    |  1,138  |  4,214    | 3.70x           | PG     |
| Q6    |  2,196  |  2,210    | 1.01x           | ≈      |
| Q7    |  1,821  |  3,604    | 1.98x           | PG     |
| Q8    |    939  |  3,017    | 3.21x           | PG     |
| Q9    |  7,773  |  6,169    | **0.79x**       | **ORCA** |
| Q10   |  2,846  |  1,829    | **0.64x**       | **ORCA** |
| Q11   |    504  |    868    | 1.72x           | PG     |
| Q12   |  3,589  |  3,453    | 0.96x           | ≈      |
| Q13   |  1,783  |  1,919    | 1.08x           | ≈      |
| Q14   |  2,200  |  2,598    | 1.18x           | ≈      |
| Q15   |  2,233  |  2,416    | 1.08x           | ≈      |
| Q16   |  1,152  |  1,220    | 1.06x           | ≈      |
| Q17   |  1,539  |    179    | **0.12x** ✓ (fixed) | **ORCA 8.6x** |
| Q18   |  7,865  |  9,275    | 1.18x           | ≈      |
| Q19   |    142  |    329    | 2.33x           | PG     |
| Q20   |    693  |  3,787    | 5.47x           | PG     |
| Q21   |  3,585  | 11,145    | 3.11x           | PG     |
| Q22   |    608  |  1,129    | 1.86x           | PG     |

**Total (22 queries):** PG ~57,539 ms vs ORCA ~76,921 ms — ORCA is **1.34x slower** overall.

---

## Key Findings

**ORCA wins:**
- **Q17**: 8.6x faster — NL join + `lineitem_l_partkey_idx` index scan vs PG's full scan + hash join
- **Q10**: 1.6x faster — better join order for the 6-table join
- **Q9**: 1.3x faster — better join order for the 8-way join with correlated subquery

**Remaining regressions:**
- **Q20** (5.5x): subquery decorrelation gone wrong — eager full decorrelation vs PG's lazy SubPlan
- **Q2** (4.6x): join order / index usage on small-table star join
- **Q5** (3.7x): multi-table join chain, join order suboptimal
- **Q8** (3.2x): 8-table join with subquery, ORCA selects suboptimal join order
- **Q21** (3.1x): semi/anti-semi joins — ORCA uses hash join on full scans; NL index plan not chosen
- **Q7** (2.0x): cross-nation join, non-optimal join order
- **Q19** (2.3x): disjunctive predicate, ORCA misses filter pushdown opportunity

**Planning overhead:**
- ORCA planning is 50×–1150× slower than PG due to exhaustive search. On short-running
  queries this overhead is a significant fraction of total wall time.

---

## Regression Analysis

### Q11 — Fixed ✓

**Commit:** `46b96a6 cost: penalize index scan on non-leading composite key column`

**Root cause:** `partsupp_pkey` is `(ps_partkey, ps_suppkey)`. ORCA generated a Nested Loop
where the inner side was `Index Scan using partsupp_pkey` with condition
`ps_suppkey = supplier.s_suppkey` — using the *non-leading* column. Because the B-tree is
ordered by `ps_partkey` first, this required scanning almost the entire index (~2,196 pages)
per lookup. ORCA's cost model treated it identically to a leading-column scan, underestimating
the cost by ~50,000×.

**Fix:** In `CostIndexScan()` (`libgpdbcost/src/CCostModelGPDB.cpp`), when the predicate does
not reference the first key column of a composite index, scale `dIndexScanTupRandomFactor` by
`table_pages / rows_per_rebind`. This reflects that O(table_pages) index entries must be read
rather than a small range, making the seq-scan + hash-join alternative cheaper.

**Result:** Q11 execution time 11,281 ms → **672 ms** (16.8× improvement).

---

### Q4 — Fixed ✓

**Commits:** `b9be6ec feat: add semi/anti-semi index NL join support to ORCA`,
`97d6a92 feat: calibrate NL index scan costs via OLS regression`

**Root cause:** Q4 filters orders by date range and uses `EXISTS` to require at least one
late-delivery lineitem:

```sql
EXISTS (SELECT * FROM lineitem
        WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)
```

ORCA unnested `EXISTS` into a **full deduplication + hash join**, scanning all 3.8M lineitem
rows and building an 88 MB hash table. PG uses a **Nested Loop Semi Join** with
`lineitem_pkey(l_orderkey, l_linenumber)`:

| | PG (NL Semi Join + index) | ORCA before fix |
|--|---|---|
| lineitem rows accessed | ~114K (57K × ~2 via index) | **3,793,296** (full scan) |
| Deduplication | none (semi-join stops at first match) | HashAggregate, 1.375M groups, 88 MB |

**Fix (two parts):**

1. **`EopPhysicalLeftSemiIndexNLJoin` operator** — ORCA previously lacked physical operators
   for semi/anti-semi index NL joins. `CXformImplementIndexApply` was extended to produce
   `CPhysicalLeftSemiIndexNLJoin` and `CPhysicalLeftAntiSemiIndexNLJoin`, allowing ORCA to
   represent the "probe index with semi-join semantics" plan at all.

2. **Warm-cache cost calibration via OLS regression** — The default ORCA cost parameters were
   calibrated for cold-disk MPP systems and made NL index scans appear too expensive on
   single-node PG with a warm buffer cache. Calibration methodology:
   - **Experiment**: 7 synthetic tables with exactly r=1..7 lineitems per order, K=10,000
     probes via `lineitem_pkey`, selecting `l_extendedprice` (heap-fetch, non-index column)
   - **Model**: T_per_probe = α + r × W_real × β (linear in r, R²≈0.999)
   - **Fitted**: α=4.914×10⁻³ ms/probe, β=6.628×10⁻⁶ ms/(probe·row·byte), C_unit=1.154 ms/unit
   - **NL branch values** in `CostIndexScan()`:
     - `dEffectiveRandomFactor = 4.259e-3` (vs global MPP default 0.05, i.e. ÷11.7)
     - `dEffectiveScanTupCostUnit = 1.725e-6` (vs global MPP default 3.66e-6, i.e. ÷2.1)

**Result:** Q4 went from **4,330 ms → 1,179 ms** (3.7× regression eliminated, now ≈1.04× of PG).

Also resolved: **Q17** is now ORCA's biggest win at **0.12× of PG** (8.6× faster), using
`lineitem_l_partkey_idx` with the same calibrated NL cost model.

---

### Q20 — Open

**Symptom:** 3,787 ms vs PG 693 ms (5.5× slower).

**Root cause:** Q20 contains a correlated scalar subquery in a `HAVING`-style filter:

```sql
ps_availqty > (SELECT 0.5 * sum(l_quantity) FROM lineitem
               WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
               AND l_shipdate BETWEEN '1994-01-01' AND '1995-01-01')
```

ORCA **eagerly decorrelates** this into a full `HashAggregate(l_partkey, l_suppkey)` over all
of lineitem before joining with the outer query. PG keeps it as a correlated `SubPlan` and
evaluates it lazily only for the 8,508 qualifying `(ps_partkey, ps_suppkey)` pairs.

| | PG (lazy SubPlan) | ORCA (eager decorrelation) |
|--|---|---|
| lineitem rows processed | 8,508 × ~1.1 rows via Bitmap Index Scan | **909,455 rows** (full year scan) |
| Aggregate groups | 8,508 executions | **543,210 groups** (64× more) |
| ORCA cardinality estimate for Hash Join | — | `rows=1` (actual 5,833 — wildly wrong) |

The decorrelated plan processes 64× more lineitem data. ORCA's cardinality underestimate
(`rows=1`) masked the true cost.

**Fix direction:** The subquery unnesting transform should compare the expected size of the
decorrelated intermediate result vs. the cost of correlated execution (outer rows × subplan
cost). When `outer_qualifying_rows << decorrelated_result_rows`, keeping the SubPlan wins.

---

### Q21 — Open

**Symptom:** 11,145 ms vs PG 3,585 ms (3.1× slower).

**Query structure:** Q21 finds suppliers who had waiting shipments. It uses:
- `NOT EXISTS` on `l3` (same order, different supplier, also late) — Anti Join
- `EXISTS` on `l2` (same order, different supplier) — Semi Join

**ORCA plan** (even after adding Semi/Anti-Semi Index NL Join operators):
```
Hash Join l1+supplier+nation ⋈ orders
  Hash Semi Join l1 ⋈ l2 (EXISTS, full lineitem scan 6M rows, disk spill)
    Hash Anti Join l1 ⋈ l3 (NOT EXISTS, full lineitem scan 3.8M rows, disk spill)
```

**PG plan** (max_parallel_workers_per_gather=0):
```
Nested Loop → orders (Index Scan orders_pkey, 8,357 lookups)
  Nested Loop Semi Join → l2 (Index Scan lineitem_pkey, ~14K lookups)
    Nested Loop Anti Join → l3 (Index Scan lineitem_pkey, 156,739 lookups)
```

| | PG (NL + index) | ORCA (Hash + full scan) |
|--|---|---|
| l3 rows read | ~142K (156,739 × 0.91 via index) | **3,793,296** (full scan + disk spill) |
| l2 rows read | ~8,300 (13,859 × 0.60 via index) | **6,001,215** (full scan + disk spill) |

**Status of fix attempt:** Commit `b9be6ec` added `EopPhysicalLeftSemiIndexNLJoin` and
`EopPhysicalLeftAntiSemiIndexNLJoin` operators. However, for Q21 ORCA still prefers hash
join because the Anti Join outer side has **156,739 rows** — at that volume NL probes are
genuinely more expensive than a hash join on full scans per ORCA's cost model. The real
problem is that the join build order is wrong: ORCA should drive the Anti Join from the
smaller result set (after `orders` filter) rather than from `l1`'s full filtered scan.

**Fix direction:** The join ordering for Q21 is the deeper problem. Alternatively, improved
cardinality estimation for the inner semi/anti-join results would help ORCA select a smaller
driving side, making NL index probes cost-effective again.

---

### Q2, Q5, Q7, Q8 — Open

These share a common theme: ORCA selects a suboptimal join order for multi-table star or
chain joins. The cardinality estimates at intermediate join results diverge from actuals,
causing ORCA to build large hash tables on the wrong side or drive joins in the wrong order.
Improving multi-table selectivity estimation (e.g., via column correlation awareness or
better histogram propagation through joins) is the most impactful long-term fix.
