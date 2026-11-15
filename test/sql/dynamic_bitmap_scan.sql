-- pg_orca DynamicBitmapScan: ORCA's DynamicBitmapTableScan on a partitioned
-- table, run per partition by the DynamicBitmapScan CustomScan
-- (compat/executor/dyn_scan.c).

LOAD 'pg_orca';
SET pg_orca.enable_orca = on;
SET client_min_messages = warning;

DROP TABLE IF EXISTS dbs_het, dbs_dim CASCADE;

-- dbs_het_p2 has its own column order and a dropped column, so the
-- per-partition copy of the scan must remap the root's attribute numbers.
CREATE TABLE dbs_het (a int, b int, c text) PARTITION BY RANGE (a);
CREATE TABLE dbs_het_p1 PARTITION OF dbs_het FOR VALUES FROM (0) TO (1000);
CREATE TABLE dbs_het_p2 (c text, junk int, b int, a int);
ALTER TABLE dbs_het_p2 DROP COLUMN junk;
ALTER TABLE dbs_het ATTACH PARTITION dbs_het_p2 FOR VALUES FROM (1000) TO (2000);
CREATE TABLE dbs_het_p3 PARTITION OF dbs_het FOR VALUES FROM (2000) TO (3000);
INSERT INTO dbs_het SELECT g, g % 97, 'v' || g FROM generate_series(0, 2999) g;
CREATE INDEX dbs_het_b ON dbs_het (b);
CREATE INDEX dbs_het_c ON dbs_het (c);

CREATE TABLE dbs_dim (k int, a int);
INSERT INTO dbs_dim SELECT g, g * 10 FROM generate_series(1, 20) g;

ANALYZE dbs_het;
ANALYZE dbs_dim;

-- ===================================================================
-- One partitioned index, every partition
-- ===================================================================
EXPLAIN (COSTS OFF)
SELECT count(*), sum(a), min(c), max(c) FROM dbs_het WHERE b = 5;
SELECT count(*), sum(a), min(c), max(c) FROM dbs_het WHERE b = 5;

-- Static pruning down to the partition with the other column layout
EXPLAIN (COSTS OFF)
SELECT a, b, c FROM dbs_het WHERE b = 5 AND a BETWEEN 1000 AND 1500 ORDER BY a;
SELECT a, b, c FROM dbs_het WHERE b = 5 AND a BETWEEN 1000 AND 1500 ORDER BY a;

-- BitmapOr of two partitioned indexes
EXPLAIN (COSTS OFF)
SELECT count(*), sum(a) FROM dbs_het WHERE b = 5 OR c = 'v1234';
SELECT count(*), sum(a) FROM dbs_het WHERE b = 5 OR c = 'v1234';

-- ===================================================================
-- Outer side of a Hash Join.  The scan returns its partitions' heap slots,
-- which the join must deform (this used to crash in heap_compute_data_size).
-- VERBOSE output names the root table's columns with its alias.
-- ===================================================================
EXPLAIN (VERBOSE, COSTS OFF)
SELECT d.k, h.a, h.c FROM dbs_dim d JOIN dbs_het h ON h.b = d.k WHERE d.k < 4;
SELECT d.k, count(*), sum(h.a), max(h.c)
FROM dbs_dim d JOIN dbs_het h ON h.b = d.k WHERE d.k < 4
GROUP BY d.k ORDER BY d.k;

-- ===================================================================
-- Inner side of a Nested Loop: rescanned once per outer row with a new
-- parameter value, which EXPLAIN shows as the outer column (d.k).
-- ===================================================================
SET enable_hashjoin = off;
SET enable_mergejoin = off;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT count(*), sum(h.a) FROM dbs_dim d JOIN dbs_het h ON h.b = d.k WHERE d.k < 4;
SELECT count(*), sum(h.a) FROM dbs_dim d JOIN dbs_het h ON h.b = d.k WHERE d.k < 4;

-- A SubPlan correlated to both sides of the join, above the rescanned scan
SELECT count(*), sum(h.a)
FROM dbs_dim d JOIN dbs_het h ON h.b = d.k
WHERE d.k < 4
  AND (h.c = 'zz' OR EXISTS (SELECT 1 FROM dbs_dim d2
                             WHERE d2.k = h.a % 7 AND d2.a > d.a));
RESET enable_hashjoin;
RESET enable_mergejoin;

DROP TABLE dbs_het, dbs_dim;
