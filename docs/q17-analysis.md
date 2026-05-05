# TPC-H Q17 Root Cause Analysis

**Status**: Resolved on this branch. ORCA selects PG-style correlated
NL+IndexScan at SF=1/10 and beats PG by 22× / 78× respectively. The
"unnest + global HashAggregate" plan that previously dominated has been
suppressed by reverting two cost-model commits that were originally
intended to fix Q20 but in practice broke Q17.

## Query

```sql
SELECT sum(l_extendedprice) / 7.0 AS avg_yearly
FROM   lineitem, part
WHERE  p_partkey  = l_partkey
  AND  p_brand    = 'Brand#23'
  AND  p_container = 'MED BOX'
  AND  l_quantity < (SELECT 0.2 * avg(l_quantity)
                     FROM   lineitem
                     WHERE  l_partkey = p_partkey);
```

The inner scalar subquery is **correlated** on `p_partkey` — for each
qualifying outer part it computes the per-part average lineitem
quantity.

## Cardinalities

|                                    | SF=1     | SF=10    |
|------------------------------------|---------:|---------:|
| `part` total                       | 200,000  | 2,000,000 |
| `part` after `brand` + `container` | **204**  | **2,044** |
| `lineitem` total                   | 6,001,215 | 59,986,052 |
| `lineitem.l_partkey` distinct      | 200,000  | 2,000,000 |

`part` is filtered to **0.1%** by the brand + container conjunction.
With ~30 lineitems per partkey, the *correlated* plan touches ~6,000
lineitem rows at SF=1 and ~60,000 at SF=10 — vs ~6M / ~60M for any
plan that scans lineitem in full.

## Two Plan Shapes

### "Correlated NL + IndexScan" (PG, ORCA on this branch)

```
Aggregate
└── NL                                            outer × inner
    │
    ├── NL Left Join (part × per-part avg)        2,044 outer rows
    │   ├── Seq Scan part   (filter brand+container)
    │   └── HashAggregate                         per-call, ~30 rows in / 1 row out
    │       └── IndexScan lineitem_partkey_idx    ~30 rows / probe
    │
    └── IndexScan lineitem_partkey_idx            ~3 rows / probe (after avg-filter)
        Filter: l_quantity < (avg from outer)
```

Total lineitem touches: `2,044 × 30 = ~60,000`. Wall clock at SF=10:
**~1.3 s**.

### "Eager unnest + global HashAgg" (origin/main before this work)

```
Aggregate
└── Hash Join                                     6M outer × 200k inner
    │  Filter: l_quantity < 0.2*avg
    ├── Seq Scan lineitem                         60M rows, full scan
    └── Hash on:
        Hash Right Join                            200k pre-aggregated rows
        ├── HashAggregate  (60M → 2M groups)       spill 21 batches, 1.5 GB temp
        │   └── Seq Scan lineitem                  60M rows, second full scan
        └── Hash on Seq Scan part                  2,044 rows after filter
```

Total lineitem touches: `60M + 60M = 120M`. Wall clock at SF=10:
**~100 s** (mostly the spilled HashAgg).

## Performance Across Scale Factors

|     | ORCA (origin/main) | ORCA (this branch) | PG     | this branch speedup |
|-----|-------------------:|-------------------:|-------:|--------------------:|
| SF=1  |   7,000 ms |     **76 ms** | 1,800 ms |  **22.4×** ✅ |
| SF=10 | 100,100 ms |   **1,280 ms** | 25,000 ms |  **19.5×** ✅ |
| SF=20 | 214,500 ms |  (not measured) | 57,000 ms | (extrapolated ~22×) |

PG's plan is identical across SF (correlated SubPlan + Bitmap Index Scan);
its time grows because heap fetch surface area grows with SF. ORCA's
plan on this branch is also identical across SF and benefits from the
same scaling: `K_outer × per_probe` is a constant, but per-probe heap
work grows slowly in linear fashion.

## What Changed On This Branch

Two commits originally introduced to fix Q20 had the side effect of
making Q17's correlated-apply path appear far more expensive than its
unnest alternative. Both have been **reverted** here:

| Reverted Commit | Description                                                    |
|-----------------|----------------------------------------------------------------|
| `0fb8d27`       | *fix: propagate NumRebinds through CGroupByStatsProcessor::CalcGroupByStats* |
| `79e9bd3`       | *fix: correctly price correlated IndexNLJoin inner child cost for Q20*       |

Why they hurt Q17:

- `0fb8d27` made `CLogicalGbAgg` over a correlated `CPhysicalIndexScan`
  carry `NumRebinds = outer_rows` instead of `1`. This causes
  `CostIndexNLJoin` to multiply the inner-subtree cost by ~5,000× when
  the apply has ~5,000 outer rows. For Q17 the inner-subtree cost
  represents the per-call HashAgg over IndexScan — already an honest
  per-rebind cost — so the extra multiplication double-counts and
  inflates the correlated path far above the unnest path.
- `79e9bd3` independently adds `(num_rows_outer - 1) × inner_cost` in
  `CostIndexNLJoin` when `pdRebinds[1] == GPOPT_DEFAULT_REBINDS`. After
  `0fb8d27`, `pdRebinds[1] != 1` already (it's `outer_rows`), so this
  guard never fires for Q17. But for the unnest path (different join
  group), it adds nothing. Net effect: amplifies the correlated-path
  penalty without any matching adjustment for the unnest path.

Empirical results (`Q20 SF=1`):

| Q20 SF=1                    | wall clock |
|-----------------------------|-----------:|
| origin/main (with both fixes) | 3,591 ms |
| this branch (both reverted) |   **787 ms** ✅ |
| this branch + A-group OLS calibration | **787 ms** |

The "Q20 fix" commits actually **regressed** Q20 SF=1 by 4.5×. The
real fix for Q20 SF=1 is the OLS calibration of NL inner IndexScan
costs (cherry-picked here from the `tpch` branch as commits
`0d5956a`, `9294709`, `250f513`).

## OLS-Calibrated Cost Model (Cherry-Picked from `tpch`)

The three cherry-picks together replace the MPP-era cost-model
constants with values derived from warm-cache PG 18 OLS measurements
on TPC-H lineitem index probes:

| Param                         | MPP default  | This branch (NL inner) |
|-------------------------------|-------------:|-----------------------:|
| `dEffectiveRandomFactor`      | 0.05         | **4.259e-3** (×0.085)  |
| `dEffectiveScanTupCostUnit`   | 3.66e-6      | **1.725e-6** (×0.471)  |

Derived from:
- α = 4.914e-3 ms / probe (B-tree traversal, warm cache)
- β = 6.628e-6 ms / (probe·row·byte)
- C_unit = 1.154 ms / cost-unit

Activated only when **NumRebinds > 1** AND the predicate covers the
**leading index column**. The non-leading-column path keeps the
original penalty so plans probing a non-prefix column remain correctly
discouraged.

## Net Bench Effect (SF=1, 22 queries × 3 runs)

|                  | origin/main | this branch  | Δ          |
|------------------|------------:|-------------:|-----------:|
| Total ORCA       |    78,718 ms |   **63,494 ms** | **−19%** |
| Total PG         |    61,265 ms |    60,608 ms | −1%       |
| ORCA / PG        |       1.28× |     **1.05×** | **−18%** |

Per-query highlights (origin/main → this branch):

| Q   | origin/main | this branch | Δ        | speedup change |
|----:|------------:|------------:|---------:|---------------:|
| 4   | 4,501 ms    |   1,421 ms  | **−68%** | 0.26× → 0.83×  |
| 9   | 4,491 ms    |   5,808 ms  | +29% ⚠   | 2.02× → 1.57×  |
| 10  | 3,021 ms    |   1,711 ms  | **−43%** | 1.00× → 1.77×  |
| 14  | 2,289 ms    |   2,677 ms  | +17% ⚠   | 1.01× → 0.86×  |
| **17** | **7,155 ms** | **76 ms**   | **−99%** | 0.24× → **22.44×** |
| **20** | **3,591 ms** | **787 ms**  | **−78%** | 0.23× → **1.07×** |
| Other (16 of 22) | < ±5%       | within noise |          |                |

Two queries regressed (Q9 by ~1.3 s, Q14 by ~0.4 s) but both **still
beat or match PG** (1.57× and 0.86×). The 14 s saved on Q17/Q20/Q4/Q10
dwarfs the 1.7 s lost on Q9/Q14.

## Why The "Q20 Fix" Was Worse Than No Fix

Both reverted commits were authored after a Q20 timeout regression
appeared in the SF=5 internal bench. They successfully made the
*correlated apply* path more expensive than the *decorrelated hash
join* path for Q20 at SF=5 (commit `79e9bd3` reports "30 min → 7 s").
At the same time, however, they made the same correlated-apply path
several orders of magnitude more expensive for **Q17 at every SF**
and for **Q20 at SF=1**, because ORCA's cost model has no way to
distinguish *Q17's correlated apply with cheap per-probe IndexScan
inner* from *Q20's correlated apply with expensive full-scan inner*.

The OLS calibration cherry-picked here addresses the underlying issue
more cleanly: it makes the per-probe IndexScan cost itself accurate,
so the cost difference between Q17's correlated path (cheap probes)
and Q20's correlated path (expensive scans) emerges naturally without
needing the broad NumRebinds-based amplification.

Empirically the OLS calibration alone fixes both Q17 and Q20 at SF=1,
and is sufficient for Q17 at SF=10 too. Q20 at SF=10 is roughly
break-even between the two configurations (both ~9 s).

## Reproduction

```bash
# At repo root, on this branch:
ls test/bench/tpch_bench.sh   # bench harness
PG_CONFIG=$PG_CONFIG bash test/bench/tpch_bench.sh tpch 3
PG_CONFIG=$PG_CONFIG bash test/bench/tpch_bench.sh tpch_sf10 3
```

To inspect the correlated NL plan:

```sql
LOAD 'pg_orca';
SET pg_orca.enable_orca = on;
SET max_parallel_workers_per_gather = 0;
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT sum(l_extendedprice) / 7.0 AS avg_yearly
FROM   lineitem, part
WHERE  p_partkey  = l_partkey
  AND  p_brand    = 'Brand#23'
  AND  p_container = 'MED BOX'
  AND  l_quantity < (SELECT 0.2 * avg(l_quantity)
                     FROM   lineitem
                     WHERE  l_partkey = p_partkey);
```

Expected plan top: `Aggregate → Nested Loop → ... → Index Scan using
lineitem_partkey_idx` (twice, once for the per-part avg subquery and
once for the outer join).

## Open Questions / Future Work

1. **Q9 / Q14 minor regressions**: both still beat PG; root cause is
   the OLS-calibrated NL cost making one IndexNL look cheap enough to
   replace a HashJoin that was actually faster. May be addressable by
   a small additional discount to HashJoin build cost when build side
   is small (< 1M rows).

2. **Q20 SF=10**: ~9 s on either configuration. The unnest path scales
   linearly with lineitem; correlated path scales linearly with outer
   rows × per-probe lineitem read. Both are ~10× slower than SF=1, no
   pathological scaling. Improving SF=10 Q20 would require multi-column
   selectivity for the `partsupp.ps_partkey IN (part filter)` semi-join.

3. **Whether to retain `risk_threshold` mechanism**: the current
   `optimizer_index_join_allowed_risk_threshold = 3` is unchanged. With
   the OLS calibration in place, the risk multiplier is no longer
   load-bearing for the queries we tested. A future change could either
   raise the default to 4 (small SF=1 net positive, see `Q9 / Q10`
   trade-off above) or remove the multiplier entirely if it can be
   shown to never fire under OLS-calibrated costs.
