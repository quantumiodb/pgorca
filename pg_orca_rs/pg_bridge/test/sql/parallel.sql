-- ================================================================
-- pg_orca_rs parallel query regression tests
-- ================================================================

SET orca.enabled = on;
SET client_min_messages = 'warning';

-- Enable parallel query with deterministic settings.
SET max_parallel_workers_per_gather = 2;
SET min_parallel_table_scan_size = 0;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET enable_gathermerge = on;

-- ================================================================
-- Setup: create a "large" table whose relpages we manually set
-- to exceed the parallel scan threshold (>= 1024 pages).
-- ================================================================

DROP TABLE IF EXISTS orca_parallel CASCADE;

CREATE TABLE orca_parallel (id int, val text);
INSERT INTO orca_parallel
  SELECT i, md5(i::text) FROM generate_series(1, 10000) i;
ANALYZE orca_parallel;

-- Simulate a large table by bumping relpages.
UPDATE pg_class SET relpages = 2000 WHERE relname = 'orca_parallel';

-- ================================================================
-- Test 1: Simple parallel scan produces Gather -> Parallel Seq Scan
-- ================================================================

EXPLAIN (COSTS OFF) SELECT * FROM orca_parallel;

-- ================================================================
-- Test 2: Parallel scan with WHERE clause
-- ================================================================

EXPLAIN (COSTS OFF) SELECT * FROM orca_parallel WHERE id > 9000;

-- ================================================================
-- Test 3: Parallel scan with aggregate
-- ================================================================

EXPLAIN (COSTS OFF) SELECT count(*) FROM orca_parallel;

-- ================================================================
-- Test 4: Correctness — results must match a direct count
-- ================================================================

SELECT count(*) FROM orca_parallel;
SELECT count(*) FROM orca_parallel WHERE id > 9000;

-- ================================================================
-- Test 5: Small table must NOT get a Gather node
-- ================================================================

DROP TABLE IF EXISTS orca_small CASCADE;
CREATE TABLE orca_small (id int, val text);
INSERT INTO orca_small VALUES (1, 'a'), (2, 'b'), (3, 'c');
ANALYZE orca_small;

EXPLAIN (COSTS OFF) SELECT * FROM orca_small;

-- Cleanup
DROP TABLE IF EXISTS orca_parallel, orca_small CASCADE;
