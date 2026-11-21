--
-- A hashed Agg that is rescanned must rebuild its hash table when an outer
-- reference used inside its aggregate calls changes.
--
-- ExecReScanAgg() keeps a filled hash table when the Agg's input has no
-- changed params and none of the changed params is in Agg.aggParams.  ORCA
-- plans left aggParams NULL, so when the outer reference appeared only in an
-- aggregate argument, every rescan returned the aggregates computed for the
-- first outer row.  (ORCA does not plan aggregates with FILTER.)
--
LOAD 'pg_orca';
SET pg_orca.enable_orca = on;
SET client_min_messages = warning;

-- outer reference in an aggregate argument (from PG's aggregates test)
EXPLAIN (COSTS OFF)
SELECT s1, s2, sm FROM generate_series(1, 3) s1,
  LATERAL (SELECT s2, sum(s1 + s2) sm FROM generate_series(1, 3) s2
           GROUP BY s2) ss
ORDER BY 1, 2;
SELECT s1, s2, sm FROM generate_series(1, 3) s1,
  LATERAL (SELECT s2, sum(s1 + s2) sm FROM generate_series(1, 3) s2
           GROUP BY s2) ss
ORDER BY 1, 2;

-- the same inside a correlated SubPlan
EXPLAIN (COSTS OFF)
SELECT s1, (SELECT max(c) FROM (SELECT s2 % 2 g, sum(s1 + s2) c
                                FROM generate_series(1, 4) s2
                                GROUP BY s2 % 2) x)
FROM generate_series(1, 3) s1 ORDER BY 1;
SELECT s1, (SELECT max(c) FROM (SELECT s2 % 2 g, sum(s1 + s2) c
                                FROM generate_series(1, 4) s2
                                GROUP BY s2 % 2) x)
FROM generate_series(1, 3) s1 ORDER BY 1;

-- outer reference only outside the aggregates: the hash table may be kept
EXPLAIN (COSTS OFF)
SELECT s1, k, c FROM generate_series(1, 3) s1,
  LATERAL (SELECT s2 + s1 k, count(*) c FROM generate_series(1, 3) s2
           GROUP BY s2) ss
ORDER BY 1, 2;
SELECT s1, k, c FROM generate_series(1, 3) s1,
  LATERAL (SELECT s2 + s1 k, count(*) c FROM generate_series(1, 3) s2
           GROUP BY s2) ss
ORDER BY 1, 2;
