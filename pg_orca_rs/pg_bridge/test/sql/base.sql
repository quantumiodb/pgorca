-- ================================================================
-- pg_orca_rs base regression tests
-- ================================================================

SET orca.enabled = on;
SET client_min_messages = 'warning';

-- Setup (clean slate)
DROP TABLE IF EXISTS orca_t, orca_orders, orca_cust CASCADE;

CREATE TABLE orca_t (a int, b text);
INSERT INTO orca_t VALUES (1, 'hello'), (2, 'world'), (3, 'foo');
ANALYZE orca_t;

CREATE TABLE orca_orders (id int, amount numeric, customer_id int);
INSERT INTO orca_orders SELECT i, (i * 1.5)::numeric, (i % 100)
  FROM generate_series(1, 1000) i;
CREATE INDEX orca_orders_id_idx ON orca_orders(id);
ANALYZE orca_orders;

CREATE TABLE orca_cust (id int PRIMARY KEY, name text);
INSERT INTO orca_cust VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');
ANALYZE orca_cust;

-- ================================================================
-- M1: Simple scan
-- ================================================================

EXPLAIN (COSTS OFF) SELECT * FROM orca_t;
SELECT * FROM orca_t ORDER BY a;

-- ================================================================
-- M3: WHERE clause filter
-- ================================================================

EXPLAIN (COSTS OFF) SELECT * FROM orca_t WHERE a > 1;
SELECT * FROM orca_t WHERE a > 1 ORDER BY a;

SELECT count(*) FROM orca_orders WHERE id > 990;

-- ================================================================
-- M4: JOIN (inner join via merge/nestloop/hash)
-- ================================================================

SELECT orca_t.a, orca_t.b, orca_cust.name
  FROM orca_t JOIN orca_cust ON orca_t.a = orca_cust.id
  ORDER BY orca_t.a;

-- ================================================================
-- M6: Aggregation
-- ================================================================

EXPLAIN (COSTS OFF) SELECT count(*) FROM orca_t;
SELECT count(*) FROM orca_t;
SELECT count(*) FROM orca_orders WHERE id > 900;

EXPLAIN (COSTS OFF) SELECT a, count(*) FROM orca_t GROUP BY a;
SELECT a, count(*) FROM orca_t GROUP BY a ORDER BY a;

-- ================================================================
-- M7: ORDER BY / LIMIT / DISTINCT
-- ================================================================

SELECT * FROM orca_t ORDER BY a DESC;

EXPLAIN (COSTS OFF) SELECT * FROM orca_t ORDER BY a LIMIT 2;
SELECT * FROM orca_t ORDER BY a LIMIT 2;

SELECT * FROM orca_t ORDER BY a LIMIT 1 OFFSET 1;

EXPLAIN (COSTS OFF) SELECT DISTINCT a FROM orca_t;
SELECT DISTINCT a FROM orca_t ORDER BY a;

-- ================================================================
-- M11: Window functions
-- ================================================================

EXPLAIN (COSTS OFF) SELECT a, row_number() OVER (ORDER BY a) FROM orca_t;
SELECT a, row_number() OVER (ORDER BY a) FROM orca_t;
SELECT a, rank() OVER (ORDER BY a) FROM orca_t;

-- ================================================================
-- UNION / UNION ALL
-- ================================================================

EXPLAIN (COSTS OFF) SELECT a, b FROM orca_t WHERE a = 1 UNION ALL SELECT a, b FROM orca_t WHERE a = 2;
SELECT a, b FROM orca_t WHERE a = 1 UNION ALL SELECT a, b FROM orca_t WHERE a = 2 ORDER BY a;

EXPLAIN (COSTS OFF) SELECT a FROM orca_t UNION SELECT a FROM orca_t;
SELECT a FROM orca_t UNION SELECT a FROM orca_t ORDER BY a;

-- Three-way UNION ALL
EXPLAIN (COSTS OFF) SELECT a, b FROM orca_t UNION ALL SELECT a, b FROM orca_t UNION ALL SELECT a, b FROM orca_t;
SELECT a, b FROM orca_t UNION ALL SELECT a, b FROM orca_t UNION ALL SELECT a, b FROM orca_t ORDER BY a, b;

-- ================================================================
-- Fallback (must not crash)
-- ================================================================

SELECT * FROM orca_t WHERE a IN (SELECT id FROM orca_cust) ORDER BY a;
WITH cte AS (SELECT * FROM orca_t) SELECT * FROM cte ORDER BY a;

-- ================================================================
-- Combined
-- ================================================================

SELECT count(*) FROM orca_orders WHERE id BETWEEN 100 AND 200;
SELECT a, count(*) FROM orca_t GROUP BY a ORDER BY a;

-- Cleanup
DROP TABLE orca_t, orca_orders, orca_cust;
