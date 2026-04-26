# TPC-H Performance Comparison: ORCA vs PostgreSQL Native Planner

Test data from `test/results/tpch_orca.out` (pg_orca) vs `test/expected/tpch_pg.out` (stock PG
planner), both run on the same TPC-H SF=1 dataset with parallelism disabled.

## Execution Time

| Query | PG Exec (ms) | ORCA Exec (ms) | Ratio (ORCA/PG) | Winner |
|-------|-------------|----------------|-----------------|--------|
| Q1    | 10,394      | 10,781         | 1.04x           | ≈      |
| Q2    | 517         | 868            | 1.68x           | PG     |
| Q3    | 2,953       | 3,120          | 1.06x           | ≈      |
| Q4    | 1,184       | 4,330          | **3.66x**       | PG     |
| Q5    | 1,220       | 2,785          | **2.28x**       | PG     |
| Q6    | 2,238       | 2,209          | 0.99x           | ≈      |
| Q7    | 2,014       | 2,946          | 1.46x           | PG     |
| Q8    | 906         | 940            | 1.04x           | ≈      |
| Q9    | 8,650       | 4,355          | **0.50x**       | **ORCA 2x** |
| Q10   | 2,933       | 2,920          | 1.00x           | ≈      |
| Q11   | 548         | 672            | 1.23x ✓ (fixed) | ≈      |
| Q12   | 3,616       | 3,421          | 0.95x           | ≈      |
| Q13   | 1,696       | 1,766          | 1.04x           | ≈      |
| Q14   | 2,235       | 2,180          | 0.98x           | ≈      |
| Q15   | 2,232       | 2,219          | 0.99x           | ≈      |
| Q16   | 1,137       | 626            | **0.55x**       | **ORCA 1.8x** |
| Q17   | 1,843       | 2,168          | 1.18x ✓ (fixed) | ≈      |
| Q18   | 8,228       | 9,211          | 1.12x           | ≈      |
| Q19   | 151         | 158            | 1.04x           | ≈      |
| Q20   | 761         | 3,454          | **4.54x**       | PG     |
| Q21   | 3,829       | 9,670          | **2.53x**       | PG     |
| Q22   | 649         | 1,064          | 1.64x           | PG     |

**Total (22 queries):** PG ~59,935 ms vs ORCA ~71,863 ms — ORCA is **1.20x slower** overall.


## Key Findings

**ORCA wins:**
- Q9: 2x faster — better join order for the 8-way join with correlated subquery
- Q16: 1.8x faster — better plan for the `NOT IN` anti-join with part/supplier

**Remaining regressions:**
- Q20 (4.5x): subquery decorrelation gone wrong — see analysis below
- Q4 (3.7x): EXISTS subquery unnesting — see analysis below
- Q5 (2.3x): multi-table join chain, join order suboptimal
- Q21 (2.5x): complex multi-join with semi/anti-join
- Q2 (1.7x): join order / index usage

**Planning overhead:**
- ORCA planning is 50x–1150x slower than PG due to exhaustive search. On short-running
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
the cost by ~50,000x.

**Fix:** In `CostIndexScan()` (`libgpdbcost/src/CCostModelGPDB.cpp`), when the predicate does
not reference the first key column of a composite index, scale `dIndexScanTupRandomFactor` by
`table_pages / rows_per_rebind`. This reflects that O(table_pages) index entries must be read
rather than a small range, making the seq-scan + hash-join alternative cheaper.

**Result:** Q11 execution time 11,281 ms → **672 ms** (16.8x improvement). The 2014 TODO
comment in `CostIndexScan` that requested exactly this logic was removed.

Also resolved as a side effect: **Q17** (previously commented out as unsupported) now runs
correctly at 2,168 ms (1.18x vs PG).

---

### Q20 — Open

**Symptom:** 3,454 ms vs PG 761 ms (4.5x slower). `work_mem` increase to 256 MB eliminates
disk spill but gives no improvement (3,618 ms) — disk spill was not the bottleneck.

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
| Aggregate groups | 8,508 executions | **543,210 groups** (64x more) |
| Disk spill | none | Batches=5, 11,872 KB (at 64 MB work_mem) |
| ORCA cardinality estimate for Hash Join | — | `rows=1` (actual 5,833 — wildly wrong) |

The decorrelated plan processes 64x more lineitem data. ORCA's cardinality estimate for the
join result (`rows=1`, actual 5,833) masked the true cost and led to selecting this path.

**Fix direction:** The subquery unnesting transform should compare the expected size of the
decorrelated intermediate result vs. the cost of correlated execution (outer rows × subplan
cost). When `outer_qualifying_rows << decorrelated_result_rows`, keeping the SubPlan wins.

---

### Q4 — Open

**Symptom:** 4,330 ms vs PG 1,184 ms (3.66x slower).

**Root cause:** Q4 filters orders by date range and uses `EXISTS` to require at least one
late-delivery lineitem:

```sql
EXISTS (SELECT * FROM lineitem
        WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)
```

ORCA unnests `EXISTS` into a **full deduplication + hash join**:

```
SeqScan lineitem (l_commitdate < l_receiptdate) → 3,793,296 rows
  HashAggregate(l_orderkey) → 1,375,365 distinct keys   (88 MB hash table)
    Hash Join with orders (57,218 rows)
```

PG uses a **Nested Loop Semi Join** with `lineitem_pkey(l_orderkey, l_linenumber)`:

```
SeqScan orders → 57,218 rows
  Index Scan lineitem_pkey (l_orderkey = o_orderkey, filter l_commitdate < l_receiptdate)
    57,218 lookups × ~0.92 rows (semi-join stops at first match)
```

| | PG (NL Semi Join + index) | ORCA (full dedup + hash join) |
|--|---|---|
| lineitem rows accessed | ~114K (57K × ~2 via index) | **3,793,296** (full scan) |
| Deduplication | none (semi-join semantics) | HashAggregate, 1.375M groups, 88 MB |
| Index used | `lineitem_pkey` leading col `l_orderkey` ✓ | none |
| ORCA row estimate | — | HashAggregate `rows=437,556` (actual 1,375,365, **3.1x off**) |

ORCA scans 33x more lineitem data. The cardinality underestimate (3.1x) hides the true hash
table size and causes ORCA to prefer this plan.

**Pattern shared with Q20 and Q21:** Q4, Q20, and Q21 all involve ORCA eagerly unnesting a
subquery into a full-scan + aggregate/dedup or Hash Semi/Anti Join before applying outer
filters, while PG's correlated or semi-join execution exploits available indexes and applies
filters early. The specific cause for Q4/Q21 is structural: ORCA lacks
`EopPhysicalLeftSemiIndexNLJoin` — see Q21 analysis for detail.

**Fix direction:** The EXISTS unnesting transform (`CXformExistSubq2Join` or equivalent) should
estimate the cost of keeping the correlated semi-join path. When the outer driving table is
small and an index covers the inner join key as a leading column, the nested-loop semi-join
is likely cheaper than full-scan + dedup.

---

### Q21 — Open

**Symptom:** 9,670 ms vs PG 3,829 ms (2.53x slower).

**Query structure:** Q21 finds suppliers who had waiting shipments. It joins `supplier`,
`lineitem l1` (late-delivery filter), `orders`, and uses:
- `NOT EXISTS` on `l3` (same order, different supplier, also late) — Anti Join
- `EXISTS` on `l2` (same order, different supplier) — Semi Join

**ORCA plan:**

```
Hash Join l1+supplier+nation ⋈ orders (729K-row hash, no spill)
  Hash Semi Join l1 ⋈ l2 (EXISTS, l_orderkey)
    Hash Anti Join l1 ⋈ l3 (NOT EXISTS, l_orderkey)
      Hash Join l1 ⋈ supplier ⋈ nation  →  156,739 rows
        Seq Scan l1 (l_receiptdate > l_commitdate)  →  3,793,296 rows
    Hash l3: Seq Scan lineitem (3,793,296 rows) — Batches=2, disk spill 6,957 pages
  Hash l2: Seq Scan lineitem (6,001,215 rows) — Batches=4, disk spill 15,376 pages
```

**PG plan:**

```
Nested Loop → orders (Index Scan orders_pkey, 8,357 lookups)
  Nested Loop Semi Join → l2 (Index Scan lineitem_pkey, 13,859 lookups × ~0.6 rows)
    Nested Loop Anti Join → l3 (Index Scan lineitem_pkey, 156,739 lookups × ~0.9 rows)
      Hash Join l1 ⋈ supplier ⋈ nation  →  156,739 rows
        Seq Scan l1 (l_receiptdate > l_commitdate)  →  3,793,296 rows
```

**Data volume comparison:**

| | PG (NL + index) | ORCA (Hash + full scan) |
|--|---|---|
| l3 rows read | ~142K (156,739 × 0.91 via index) | **3,793,296** (full scan + disk spill) |
| l2 rows read | ~8,300 (13,859 × 0.60 via index) | **6,001,215** (full scan + disk spill) |
| orders rows | 8,357 (index lookups) | 729,413 (full hash table) |
| Disk spill | none | l3: 6,957 pages; l2: 15,376 pages |

ORCA reads **26x** more l3 data and **721x** more l2 data than PG.

**Root cause: missing Semi/Anti-Semi Index NL Join operators**

ORCA's physical operator set includes:

| Operator | Exists? |
|----------|---------|
| `EopPhysicalInnerIndexNLJoin` | yes |
| `EopPhysicalLeftOuterIndexNLJoin` | yes |
| `EopPhysicalLeftSemiIndexNLJoin` | **no** |
| `EopPhysicalLeftAntiSemiIndexNLJoin` | **no** |

`CXformImplementIndexApply` (the transform that instantiates physical Index NL Join operators)
only produces `CPhysicalInnerIndexNLJoin` and `CPhysicalLeftOuterIndexNLJoin`. It has no
branches for semi or anti-semi variants.

Because these operators don't exist, ORCA **can never** use an index to probe the inner side
of an EXISTS / NOT EXISTS subquery. The only available physical alternatives are
`CPhysicalLeftSemiHashJoin` and `CPhysicalLeftAntiSemiHashJoin`, both of which require
materializing the entire inner relation into a hash table — hence the full scan of `lineitem`
(6M rows for l2, 3.8M for l3) and the resulting disk spill.

**Pattern shared with Q4:** Q4's EXISTS regression has the identical structural cause: the
correlated EXISTS subquery is unnested into a full scan + dedup rather than an indexed
semi-join, for the same reason — no `EopPhysicalLeftSemiIndexNLJoin` exists to exploit
`lineitem_pkey`.

**Fix direction:** Implement `CPhysicalLeftSemiIndexNLJoin` and
`CPhysicalLeftAntiSemiIndexNLJoin` physical operators (mirroring the inner/outer variants),
and extend `CXformImplementIndexApply` to instantiate them. This is a significant effort
requiring new physical operator classes, cost model entries, and DXL translator support, but
would address Q4, Q21, and any other query using EXISTS/NOT EXISTS with an indexed inner key.
