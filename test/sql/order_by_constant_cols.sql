--
-- ORDER BY keys that the query fixes to a single value need no sort.
--
-- ORCA compared the required order with the input's order column by column,
-- so "WHERE b = 10 ORDER BY b DESC" got a Sort on b although its constraint
-- was b = 10.  CQueryContext now drops such columns (a single non-null
-- value, or IS NULL) from the top-level ORDER BY, as PG's planner does for
-- pathkeys whose equivalence class holds a constant.
--
LOAD 'pg_orca';
SET pg_orca.enable_orca = on;
SET client_min_messages = warning;

CREATE SCHEMA order_by_constant_cols;
SET search_path = order_by_constant_cols;

CREATE TABLE t (a int, b int);
INSERT INTO t SELECT g, g % 25 FROM generate_series(1, 100) g;
INSERT INTO t VALUES (101, NULL), (102, NULL);
ANALYZE t;

-- no sort at all
EXPLAIN (COSTS OFF) SELECT a, b FROM t WHERE b = 10 ORDER BY b DESC;
SELECT a, b FROM t WHERE b = 10 ORDER BY b DESC, a;
EXPLAIN (COSTS OFF) SELECT a, b FROM t WHERE b IS NULL ORDER BY b;
EXPLAIN (COSTS OFF) SELECT a, b FROM t WHERE b = 10 ORDER BY b DESC LIMIT 2;

-- only the non-constant key is sorted on
EXPLAIN (COSTS OFF) SELECT a, b FROM t WHERE b = 10 ORDER BY b DESC, a DESC;
SELECT a, b FROM t WHERE b = 10 ORDER BY b DESC, a DESC;

-- not constant: still sorted
EXPLAIN (COSTS OFF) SELECT a, b FROM t WHERE b IN (10, 11) ORDER BY b DESC, a;
SELECT a, b FROM t WHERE b IN (10, 11) ORDER BY b DESC, a;
EXPLAIN (COSTS OFF) SELECT a, b FROM t WHERE b >= 10 AND b <= 10 OR b IS NULL
ORDER BY b, a;

RESET search_path;
DROP SCHEMA order_by_constant_cols CASCADE;
