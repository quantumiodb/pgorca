-- pg_orca DPE (Dynamic Partition Elimination) regression tests
-- Tests HashJoin DPE via DynamicTableScanCS + PartitionSelectorCS CustomScan nodes.

LOAD 'pg_orca';
SET pg_orca.enable_orca TO on;
SET max_parallel_workers_per_gather TO 0;
SET enable_mergejoin TO off;

-- ------------------------------------------------------------
-- Setup: partitioned tables
-- ------------------------------------------------------------

CREATE TABLE p (a int, b text)
    PARTITION BY RANGE (a);

CREATE TABLE p_1 PARTITION OF p FOR VALUES FROM (1)    TO (501);
CREATE TABLE p_2 PARTITION OF p FOR VALUES FROM (501)  TO (1001);
CREATE TABLE p_3 PARTITION OF p FOR VALUES FROM (1001) TO (1501);
CREATE TABLE p_4 PARTITION OF p FOR VALUES FROM (1501) TO (2001);

INSERT INTO p SELECT i, 'val' || i FROM generate_series(1, 2000) i;
ANALYZE p;

-- Non-partitioned driving table
CREATE TABLE t (a int, b text);
INSERT INTO t VALUES (10, 'ten'), (505, 'five05'), (1050, 'kilo'), (1750, 'bigone');
ANALYZE t;

-- Second partitioned table for partition-to-partition join tests
CREATE TABLE q (a int, c text)
    PARTITION BY RANGE (a);

CREATE TABLE q_1 PARTITION OF q FOR VALUES FROM (1)    TO (501);
CREATE TABLE q_2 PARTITION OF q FOR VALUES FROM (501)  TO (1001);
CREATE TABLE q_3 PARTITION OF q FOR VALUES FROM (1001) TO (1501);
CREATE TABLE q_4 PARTITION OF q FOR VALUES FROM (1501) TO (2001);

INSERT INTO q SELECT i, 'qval' || i FROM generate_series(1, 2000) i;
ANALYZE q;

-- ------------------------------------------------------------
-- 1. Basic DPE: non-partitioned driving table joins partitioned table
--    ORCA should emit DynamicTableScan (CustomScan) instead of Append.
--    MergeJoin does not produce a PartitionSelector (that is HashJoin-only);
--    DynamicTableScan here provides static partition pruning.
-- ------------------------------------------------------------

EXPLAIN (costs off)
SELECT t.a, p.b
FROM   t JOIN p ON t.a = p.a
ORDER BY t.a;

-- Verify correctness
SELECT t.a, p.b
FROM   t JOIN p ON t.a = p.a
ORDER BY t.a;

-- ------------------------------------------------------------
-- 2. DPE with static pruning: qual on partition key narrows partitions further
-- ------------------------------------------------------------

EXPLAIN (costs off)
SELECT t.a, p.b
FROM   t JOIN p ON t.a = p.a
WHERE  p.a BETWEEN 501 AND 1000
ORDER BY t.a;

SELECT t.a, p.b
FROM   t JOIN p ON t.a = p.a
WHERE  p.a BETWEEN 501 AND 1000
ORDER BY t.a;

-- ------------------------------------------------------------
-- 3. Partition-to-partition self-join (p JOIN p)
--    Both sides are partitioned; both get DynamicTableScan with
--    static pruning from the BETWEEN predicate (Partitions Selected: 1).
-- ------------------------------------------------------------

EXPLAIN (costs off)
SELECT p1.a, p1.b
FROM   p p1 JOIN p p2 ON p1.a = p2.a
WHERE  p1.a BETWEEN 1001 AND 1500
ORDER BY p1.a
LIMIT  5;

SELECT p1.a, p1.b
FROM   p p1 JOIN p p2 ON p1.a = p2.a
WHERE  p1.a BETWEEN 1001 AND 1500
ORDER BY p1.a
LIMIT  5;

-- ------------------------------------------------------------
-- 4. DPE across two different partitioned tables (p JOIN q)
-- ------------------------------------------------------------

EXPLAIN (costs off)
SELECT p.a, p.b, q.c
FROM   p JOIN q ON p.a = q.a
WHERE  p.a BETWEEN 1 AND 500
ORDER BY p.a
LIMIT  5;

SELECT p.a, p.b, q.c
FROM   p JOIN q ON p.a = q.a
WHERE  p.a BETWEEN 1 AND 500
ORDER BY p.a
LIMIT  5;

-- ------------------------------------------------------------
-- 5. DPE correctness: no false rows, exact match
-- ------------------------------------------------------------

-- Only rows in t should appear; rows 505 and 1050 land in different partitions.
SELECT count(*)
FROM   t JOIN p ON t.a = p.a;

-- Each t row matches exactly one p row
SELECT t.a, count(p.a)
FROM   t JOIN p ON t.a = p.a
GROUP BY t.a
ORDER BY t.a;

-- ------------------------------------------------------------
-- 6. Partition attached with non-default column order
--    Tests the tuple-remapping path in dts_exec.
-- ------------------------------------------------------------

CREATE TABLE r_base (x int, y text, z int);
INSERT INTO r_base VALUES (5, 'five', 100), (15, 'fifteen', 200);

CREATE TABLE r (x int, z int, y text)   -- different column order from r_base
    PARTITION BY RANGE (x);

-- Attach r_base as a partition (column order mismatch triggers remap)
ALTER TABLE r ATTACH PARTITION r_base FOR VALUES FROM (1) TO (20);

CREATE TABLE r_2 PARTITION OF r FOR VALUES FROM (20) TO (40);
INSERT INTO r_2 VALUES (25, 300, 'twentyfive');

ANALYZE r;

CREATE TABLE s (x int);
INSERT INTO s VALUES (5), (15), (25);
ANALYZE s;

EXPLAIN (costs off)
SELECT s.x, r.y, r.z
FROM   s JOIN r ON s.x = r.x
ORDER BY s.x;

SELECT s.x, r.y, r.z
FROM   s JOIN r ON s.x = r.x
ORDER BY s.x;

-- ------------------------------------------------------------
-- 7. Rescan correctness: NestLoop outer drives repeated inner DPE scans
--    Each outer row should see only its own matching inner partition rows.
-- ------------------------------------------------------------

-- Force NestLoop by disabling hash/merge joins temporarily
SET enable_hashjoin  TO off;
SET enable_mergejoin TO off;

SELECT t.a, count(p.a) AS cnt
FROM   t JOIN p ON t.a = p.a
GROUP BY t.a
ORDER BY t.a;

RESET enable_hashjoin;
RESET enable_mergejoin;

-- ------------------------------------------------------------
-- Cleanup
-- ------------------------------------------------------------

DROP TABLE IF EXISTS p, q, r, s, t CASCADE;
