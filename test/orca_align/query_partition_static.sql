-- pg_orca static partition selection regression tests
-- Ported from testrepo/partitioning/staticselection/sql/ (static_selection_1..7)
-- MPP-24709, GPSQL-2879: Static partition pruning with RANGE partitioning

LOAD 'pg_orca';
SET pg_orca.enable_orca = on;
SET client_min_messages = warning;
SET enable_mergejoin TO off;

-- ===================================================================
-- Setup: foo(a int, b int, c int) RANGE partitioned on b
-- 10 partitions: [1,11), [11,21), ..., [91,101)
-- ===================================================================

DROP TABLE IF EXISTS foo CASCADE;

CREATE TABLE foo (a int, b int, c int) PARTITION BY RANGE (b);
CREATE TABLE foo_p1  PARTITION OF foo FOR VALUES FROM (1)  TO (11);
CREATE TABLE foo_p2  PARTITION OF foo FOR VALUES FROM (11) TO (21);
CREATE TABLE foo_p3  PARTITION OF foo FOR VALUES FROM (21) TO (31);
CREATE TABLE foo_p4  PARTITION OF foo FOR VALUES FROM (31) TO (41);
CREATE TABLE foo_p5  PARTITION OF foo FOR VALUES FROM (41) TO (51);
CREATE TABLE foo_p6  PARTITION OF foo FOR VALUES FROM (51) TO (61);
CREATE TABLE foo_p7  PARTITION OF foo FOR VALUES FROM (61) TO (71);
CREATE TABLE foo_p8  PARTITION OF foo FOR VALUES FROM (71) TO (81);
CREATE TABLE foo_p9  PARTITION OF foo FOR VALUES FROM (81) TO (91);
CREATE TABLE foo_p10 PARTITION OF foo FOR VALUES FROM (91) TO (101);

INSERT INTO foo SELECT generate_series(1,5), generate_series(1,100), generate_series(1,10);
ANALYZE foo;

-- ===================================================================
-- Test 1: Full scan -- all 10 partitions selected
-- ===================================================================

EXPLAIN (costs ON) SELECT * FROM foo;

SELECT * FROM foo ORDER BY a, b, c;

-- ===================================================================
-- Test 2: Point filter b = 35 -- 1 partition (p4: [31,41))
-- ===================================================================

EXPLAIN (costs ON) SELECT * FROM foo WHERE b = 35;

SELECT * FROM foo WHERE b = 35 ORDER BY a, b, c;

-- ===================================================================
-- Test 3: Range filter b < 35 -- 4 partitions ([1,11), [11,21), [21,31), [31,41))
-- ===================================================================

EXPLAIN (costs ON) SELECT * FROM foo WHERE b < 35;

SELECT * FROM foo WHERE b < 35 ORDER BY a, b, c;

-- ===================================================================
-- Test 4: IN list b IN (5, 6, 14, 23) -- 3 partitions ([1,11), [11,21), [21,31))
-- ===================================================================

EXPLAIN (costs ON) SELECT * FROM foo WHERE b IN (5, 6, 14, 23);

SELECT * FROM foo WHERE b IN (5, 6, 14, 23) ORDER BY a, b, c;

-- ===================================================================
-- Test 5: OR range b < 15 OR b > 60 -- 6 partitions ([1,11), [11,21), [61,71), [71,81), [81,91), [91,101))
-- ===================================================================

EXPLAIN (costs ON) SELECT * FROM foo WHERE b < 15 OR b > 60;

SELECT * FROM foo WHERE b < 15 OR b > 60 ORDER BY a, b, c;

-- ===================================================================
-- Test 6: Out-of-range b = 150 -- 0 partitions (empty result)
-- ===================================================================

EXPLAIN (costs ON) SELECT * FROM foo WHERE b = 150;

SELECT * FROM foo WHERE b = 150 ORDER BY a, b, c;

-- ===================================================================
-- Test 7: Non-constant predicate b = a*5 -- cannot prune statically, all 10 partitions
-- ===================================================================

EXPLAIN (costs ON) SELECT * FROM foo WHERE b = a*5;

SELECT * FROM foo WHERE b = a*5 ORDER BY a, b, c;

-- ===================================================================
-- Cleanup
-- ===================================================================

DROP TABLE foo CASCADE;
