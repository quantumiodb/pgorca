# D2: Drop Stale Rebind Caching for Correlated Index Scans

Commit: `437f7fe cost: drop stale rebind caching for correlated index scans`

## Problem

Inner stats for a correlated subtree (e.g. `IndexGet(l2 on l_orderkey = outer.col)`)
used to set `NumRebinds = outer_rows` at stats-derive time. The outer cardinality
was baked into the group-shared inner stats. After join reorder placed the inner
under a different (smaller) outer, the cached rebinds became stale, and
`CostIndexNLJoin` multiplied per-probe cost by the original (large) outer row count.

### Concrete instance — Q21 SF=1

`SemiJoin(supplier ⋈ nation ⋈ l1, l2)` had:

- Actual outer rows (group 724): **96,010**
- Cached inner `NumRebinds` (group 56, l1_filtered): **2,400,253**
- LeftSemiIndexNLJoin cost: 31,125 — **6.5× more expensive than HashSemi (4,747)**

The optimizer therefore picked Hash Semi Join + full table scan of l2 (6M rows)
instead of NL + Index probe on `idx_lineitem_orderkey`. PG's planner picked the
NL+Index path and was 6.4× faster.

## Fix

Three coordinated changes:

1. **`CJoinStatsProcessor::DeriveStatsWithOuterRefs`** — no longer calls
   `SetRebinds(num_rows_outer)`. Inner stats keep rebinds at the default 1.
   `Rows()` represents per-probe output; the actual NL outer cardinality is
   supplied at cost time.

2. **`CostIndexNLJoin`** — extends rebind-compensation block to all IndexNL
   variants (Inner / LeftOuter / LeftSemi / LeftAntiSemi). Previously only
   `InnerIndexNLJoin` compensated; the others assumed cached inner rebinds
   equaled outer rows, which is no longer true.

3. **`CostIndexScan`** — switches NL-inner detection in the warm-cache OLS
   path and the heap-fetch term from `pci->NumRebinds() > 1` to
   `exprhdl.HasOuterRefs()`. Cached rebinds are now always 1, so the rebinds
   check would otherwise classify every correlated probe as a cold standalone
   scan and add ~100× heap-fetch cost.

## Benchmark Methodology

- Hardware: macOS 14.5, ARM64, 16 GB RAM
- PostgreSQL 18 (single-node), warm cache
- `max_parallel_workers_per_gather = 0`
- `optimizer_index_join_allowed_risk_threshold = 3` (default)
- Median of 3 runs per query (warm-up discarded)
- Fresh `ANALYZE` before each scale-factor run

## Results

| SF | Baseline (no D2) | D2 (this fix) | Δ vs baseline | PG Native | ORCA/PG |
|---|---|---|---|---|---|
| **SF=1** | 68,933 ms | **58,510 ms** | **−15.1%** | 42,429 | 1.38× |
| **SF=2** | 188,816 ms | **167,583 ms** | **−11.2%** | 246,139 | **0.68×** |
| **SF=5** | 712,810 ms | **627,078 ms** | **−12.0%** | 382,880 | 1.64× |

SF=2 PG total is inflated by PG's own poor plan choices on Q5 (62 s) and Q7 (46 s);
ORCA beats PG on those.

## Per-Query Highlights (SF=5)

### Significant improvements (>20%)

| Query | Baseline | D2 | Δ |
|---|---|---|---|
| Q19 | 5,414 ms | **653 ms** | **−87.9%** |
| Q17 | 3,211 ms | **423 ms** | **−86.8%** |
| Q3 | 41,519 ms | **15,546 ms** | **−62.6%** |
| Q2 | 10,420 ms | **4,911 ms** | **−52.9%** |
| Q22 | 8,102 ms | **5,328 ms** | **−34.2%** |
| Q11 | 2,772 ms | **1,873 ms** | **−32.4%** |
| Q21 | 173,058 ms | **130,490 ms** | **−24.6%** |
| Q7 | 16,761 ms | **12,863 ms** | **−23.3%** |

### Regressions

| Query | Baseline | D2 | Δ |
|---|---|---|---|
| Q12 | 12,641 ms | 17,778 ms | **+40.6%** |
| Q10 | 17,748 ms | 21,209 ms | +19.5% |
| Q9 | 156,425 ms | 162,178 ms | +3.7% |

Q12 is the only consistent regression across SF=2/5 (SF=1 it improves). Worth a
follow-up investigation.

## Q21 across all SF

| SF | Baseline | D2 | Improvement |
|---|---|---|---|
| SF=1 | 10,225 ms | **5,472 ms** | −46.5% |
| SF=2 | 33,866 ms | **19,494 ms** | −42.4% |
| SF=5 | 173,058 ms | **130,490 ms** | −24.6% |

## Threshold Sensitivity

Tested `optimizer_index_join_allowed_risk_threshold = 10` on SF=5 — total time
went from 641 s to 1,096 s (+71%). Q7 alone regressed from 14 s to 242 s. The
default value 3 is correct for the general case; users can opt into 10 at the
session level for plans that match Q21's shape.

## Files Changed

- `libgpdbcost/src/CCostModelGPDB.cpp` — `CostIndexNLJoin` and `CostIndexScan`
- `libnaucrates/src/statistics/CJoinStatsProcessor.cpp` — `DeriveStatsWithOuterRefs`

Total +20 lines, −10 lines.
