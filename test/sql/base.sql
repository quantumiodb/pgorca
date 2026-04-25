-- pg_orca base regression test
-- Tests: SELECT, WHERE, GROUP BY, JOIN, subqueries, CTE, UNION, EXPLAIN
LOAD 'pg_orca';
set pg_orca.enable_orca to off;

-- Create TPC-H schema tables (replaces pg_tpch dependency)

CREATE TABLE nation (
    n_nationkey INT NOT NULL,
    n_name      TEXT NOT NULL,
    n_regionkey INT NOT NULL,
    n_comment   TEXT
);

CREATE TABLE customer (
    c_custkey     INT NOT NULL,
    c_name        TEXT NOT NULL,
    c_address     TEXT NOT NULL,
    c_nationkey   INT NOT NULL,
    c_phone       TEXT NOT NULL,
    c_acctbal     FLOAT NOT NULL,
    c_mktsegment  TEXT NOT NULL,
    c_comment     TEXT NOT NULL
);

CREATE TABLE orders (
    o_orderkey      INT NOT NULL,
    o_custkey       INT NOT NULL,
    o_orderstatus   TEXT NOT NULL,
    o_totalprice    FLOAT NOT NULL,
    o_orderdate     DATE NOT NULL,
    o_orderpriority TEXT NOT NULL,
    o_clerk         TEXT NOT NULL,
    o_shippriority  INT NOT NULL,
    o_comment       TEXT NOT NULL
);

-- Additional test tables
CREATE TABLE product (
    pn     INT NOT NULL,
    pname  TEXT NOT NULL,
    pcolor TEXT,
    PRIMARY KEY (pn)
);

CREATE TABLE sale (
    cn  INT NOT NULL,
    vn  INT NOT NULL,
    pn  INT NOT NULL,
    dt  DATE NOT NULL,
    qty INT NOT NULL,
    prc FLOAT NOT NULL,
    PRIMARY KEY (cn, vn, pn)
);

CREATE TABLE test_table (a INT, b INT);
ALTER TABLE test_table ADD COLUMN c INT;
EXPLAIN SELECT * FROM test_table;
ALTER TABLE test_table DROP COLUMN c;
ALTER TABLE test_table ADD COLUMN c INT;

-- Enable ORCA
SET pg_orca.enable_orca TO on;

-- RTE_RESULT
VALUES(1,2);
-- RTE_VALUES
VALUES(1,2),(3,4);

-- Basic SELECT
EXPLAIN VERBOSE SELECT FROM orders;
EXPLAIN VERBOSE SELECT * FROM orders;
EXPLAIN VERBOSE SELECT o_custkey FROM orders;
EXPLAIN VERBOSE SELECT o_custkey, o_custkey FROM orders;
SELECT n_regionkey AS x, n_regionkey AS y FROM nation;

SELECT;
SELECT 1, '1', 2 AS x, 'xx' AS x;

EXPLAIN VERBOSE SELECT 1 + 1;

SELECT 1, 1 + 1, true, null, array[1, 2, 3], array[[1], [2], [3]], '[1, 2, 3]';
SELECT 1::text;
SELECT '{1,2,3}'::integer[], 1::text, 1::int, 'a'::text, '99999999'::int;
EXPLAIN VERBOSE SELECT 1+1 = 3 * 10 AND 2 > 1 OR 1 IS NULL WHERE 1=1;

-- WHERE
EXPLAIN VERBOSE SELECT o_orderkey FROM orders WHERE o_custkey > 10;

-- Column aliases with WHERE
EXPLAIN VERBOSE SELECT n_regionkey AS x, n_regionkey AS y FROM nation WHERE n_regionkey < 10;

-- IN-list constant
EXPLAIN VERBOSE SELECT 1 WHERE 1 IN (2, 3);

-- Expressions
EXPLAIN VERBOSE SELECT n_regionkey AS x, n_regionkey + 1 AS y FROM nation;

-- generate_series
EXPLAIN VERBOSE SELECT 1 FROM generate_series(1,10);
EXPLAIN VERBOSE SELECT g FROM generate_series(1,10) g;
EXPLAIN VERBOSE SELECT g + 1 FROM generate_series(1,10) g;
EXPLAIN VERBOSE SELECT g + 1 AS x FROM generate_series(1,10) g WHERE 1 < 10;

-- LIMIT
EXPLAIN VERBOSE SELECT n_regionkey AS x, n_regionkey + 1 AS y FROM nation LIMIT 10;

-- Arithmetic
EXPLAIN VERBOSE SELECT o_totalprice + 1, o_totalprice - 1, o_totalprice * 1, o_totalprice / 1 FROM orders WHERE o_orderkey = 1000 AND o_shippriority + 1 > 10;

-- ORDER BY + LIMIT (index scan tests)
EXPLAIN VERBOSE SELECT * FROM nation ORDER BY 1 LIMIT 10;
EXPLAIN VERBOSE SELECT * FROM nation ORDER BY 2 LIMIT 10;
EXPLAIN VERBOSE SELECT * FROM nation ORDER BY 3 LIMIT 10;
EXPLAIN VERBOSE SELECT * FROM nation ORDER BY 4 LIMIT 10;
EXPLAIN VERBOSE SELECT * FROM orders ORDER BY 1 DESC LIMIT 10;
EXPLAIN VERBOSE SELECT * FROM orders ORDER BY 1 DESC, 2 ASC LIMIT 10;

-- Composite index equality prefix: leading key fixed by equality predicate
-- should allow the scan to satisfy ORDER BY on trailing key without Sort.
CREATE TABLE boolindex (b bool, i int);
CREATE UNIQUE INDEX boolindex_b_i_key ON boolindex(b, i);
-- b is fixed by equality => index output is ordered by i => no Sort node
EXPLAIN (costs off) SELECT * FROM boolindex WHERE b ORDER BY i LIMIT 10;
-- range predicate on b => b not fixed => Sort still needed
EXPLAIN (costs off) SELECT * FROM boolindex WHERE b > false ORDER BY i LIMIT 10;
-- ORDER BY matches full index key order => no Sort
EXPLAIN (costs off) SELECT * FROM boolindex ORDER BY b, i LIMIT 10;
DROP TABLE boolindex;

-- Basic filter
EXPLAIN VERBOSE SELECT o_totalprice + 1 FROM orders WHERE o_orderkey = 1000 AND o_shippriority + 1 < 10;

-- IN-list
EXPLAIN VERBOSE SELECT * FROM customer WHERE c_custkey IN (1,2,2,3,123,34,345,453,56,567,23,213);
EXPLAIN VERBOSE SELECT * FROM customer WHERE c_custkey IN (1,2,2,3,123,34,345,453,56,567,23,213) OR 1+1=2;

-- Aggregates
EXPLAIN VERBOSE SELECT sum(n_regionkey) FROM nation;
EXPLAIN VERBOSE SELECT sum(n_regionkey) FROM nation GROUP BY n_name;
EXPLAIN VERBOSE SELECT o_orderkey, sum(o_custkey) FROM orders GROUP BY o_orderkey ORDER BY o_orderkey;
EXPLAIN VERBOSE SELECT o_orderkey, sum(o_custkey) FROM orders GROUP BY o_orderkey ORDER BY o_orderkey LIMIT 10;
EXPLAIN VERBOSE SELECT o_orderkey, sum(o_custkey + o_orderkey) FROM orders GROUP BY o_orderkey ORDER BY o_orderkey LIMIT 10;

-- UNION / INTERSECT / EXCEPT
EXPLAIN VERBOSE SELECT * FROM nation UNION SELECT * FROM nation ORDER BY 2 LIMIT 10;
EXPLAIN VERBOSE SELECT * FROM nation UNION ALL SELECT * FROM nation ORDER BY 2 LIMIT 10;
EXPLAIN SELECT * FROM nation EXCEPT SELECT * FROM nation ORDER BY 3 LIMIT 10;
EXPLAIN SELECT * FROM nation INTERSECT SELECT * FROM nation ORDER BY 3 LIMIT 10;

-- JOINs
EXPLAIN SELECT * FROM orders JOIN nation ON orders.o_custkey = nation.n_regionkey;
EXPLAIN SELECT * FROM orders JOIN nation ON orders.o_custkey = nation.n_regionkey AND nation.n_regionkey = 1;
EXPLAIN SELECT * FROM orders JOIN nation ON orders.o_custkey = nation.n_regionkey WHERE nation.n_regionkey = 1;

-- LEFT JOIN
EXPLAIN SELECT * FROM orders LEFT JOIN nation ON orders.o_custkey = nation.n_regionkey;
EXPLAIN SELECT * FROM orders LEFT JOIN nation ON orders.o_custkey = nation.n_regionkey AND nation.n_regionkey = 1;
EXPLAIN SELECT * FROM orders LEFT JOIN nation ON orders.o_custkey = nation.n_regionkey WHERE nation.n_regionkey = 1;

-- Subqueries (IN / EXISTS)
EXPLAIN VERBOSE SELECT * FROM customer WHERE c_custkey IN (SELECT c_custkey FROM nation);
EXPLAIN VERBOSE SELECT * FROM customer x WHERE c_custkey IN (SELECT x.c_nationkey FROM nation);

-- Self-join (non-equi)
EXPLAIN VERBOSE SELECT * FROM customer a JOIN customer b ON a.c_custkey != b.c_custkey;

-- Nested subquery
EXPLAIN VERBOSE SELECT * FROM customer WHERE customer.c_custkey IN (SELECT customer.c_nationkey + 1 FROM (
    SELECT * FROM nation WHERE nation.n_nationkey IN (SELECT nation.n_nationkey + 1 FROM orders)
) i);

-- CTEs
EXPLAIN VERBOSE WITH cte AS (SELECT * FROM orders) SELECT * FROM cte;
EXPLAIN VERBOSE WITH x AS (SELECT o_custkey a1, o_custkey a2, o_custkey FROM orders) SELECT * FROM x WHERE a1 = 1 AND a2 = 2;
EXPLAIN VERBOSE WITH cte AS (SELECT * FROM orders LEFT JOIN nation ON orders.o_custkey = nation.n_regionkey WHERE nation.n_regionkey = 1) SELECT * FROM cte WHERE o_orderkey = 1;

-- Semi-join: EXISTS
EXPLAIN VERBOSE SELECT * FROM customer WHERE EXISTS (SELECT * FROM nation WHERE c_custkey = n_nationkey AND c_nationkey <> n_regionkey);
EXPLAIN VERBOSE SELECT * FROM customer WHERE c_custkey IN (SELECT DISTINCT c_custkey FROM nation);
EXPLAIN VERBOSE SELECT * FROM customer WHERE c_custkey IN (SELECT DISTINCT n_regionkey FROM nation);

EXPLAIN VERBOSE SELECT * FROM customer WHERE c_custkey IN (SELECT c_custkey FROM nation LIMIT 3);

-- Scalar subquery
EXPLAIN VERBOSE SELECT * FROM customer WHERE c_custkey > (SELECT sum(n_nationkey) FROM nation);

EXPLAIN VERBOSE SELECT * FROM customer WHERE c_custkey IN (SELECT n_nationkey FROM nation);
EXPLAIN SELECT * FROM customer WHERE 1+c_custkey IN (SELECT c_nationkey+1 FROM nation);
EXPLAIN VERBOSE SELECT * FROM customer WHERE 2 IN (SELECT n_nationkey + 1 FROM nation);

-- Correlated EXISTS
EXPLAIN VERBOSE SELECT * FROM customer WHERE EXISTS(SELECT n_nationkey FROM nation WHERE nation.n_nationkey<>customer.c_custkey GROUP BY nation.n_nationkey);

-- Nested EXISTS
EXPLAIN VERBOSE SELECT pn, cn, vn FROM sale s WHERE EXISTS (SELECT * FROM customer WHERE EXISTS (SELECT * FROM product WHERE pn = s.pn));
EXPLAIN SELECT pn, cn, vn FROM sale s WHERE cn IN (SELECT s.pn FROM customer WHERE cn NOT IN (SELECT pn FROM product WHERE pn = s.pn));
EXPLAIN SELECT * FROM customer WHERE EXISTS (SELECT 1 FROM nation WHERE nation.n_nationkey = customer.c_custkey AND customer.c_custkey > 1);

-- Actual query execution with aggregate
SELECT o_orderkey, sum(o_custkey + o_orderkey)/20 FROM orders GROUP BY o_orderkey ORDER BY o_orderkey LIMIT 10;

-- Correlated subquery with execution
SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM nation WHERE nation.n_regionkey = orders.o_custkey AND nation.n_regionkey = 10);
SELECT *, (SELECT 2 FROM nation WHERE nation.n_regionkey = orders.o_custkey AND nation.n_regionkey = 10) FROM orders WHERE EXISTS (SELECT 1 FROM nation WHERE nation.n_regionkey = orders.o_custkey AND nation.n_regionkey = 10);

-- Cleanup: drop tables created in this test so subsequent tests start clean.
DROP TABLE IF EXISTS nation, customer, orders, product, sale, test_table, boolindex CASCADE;
