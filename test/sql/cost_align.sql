-- pg_orca cost regression: compares ORCA cost_model=pg vs PG native cost
-- for a curated set of EXPLAIN queries.  Plan-shape diffs are expected
-- and reported separately; the test asserts that *same-shape* plans
-- align within tolerance.
--
-- This file is run by test/cost_align.sh (not by the pg_regress
-- framework), so it does not need to be diff-stable against an .out file.
-- It seeds its own tables, runs each EXPLAIN twice (PG native and ORCA
-- with cost_model=pg), and prints a side-by-side cost comparison.
LOAD 'pg_orca';
SET client_min_messages = warning;

-- ---------------------------------------------------------------------
-- Seed reproducible PG-style test data (tenk1-equivalent + onek).
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS cal_tenk1, cal_onek;

CREATE TABLE cal_tenk1 (
    unique1     int4,
    unique2     int4,
    two         int4,
    four        int4,
    ten         int4,
    twenty      int4,
    hundred     int4,
    thousand    int4,
    twothousand int4,
    fivethous   int4,
    tenthous    int4,
    odd         int4,
    even        int4,
    stringu1    name,
    stringu2    name,
    string4     name
);
INSERT INTO cal_tenk1
SELECT g,
       (g + 5000) % 10000,
       g % 2,
       g % 4,
       g % 10,
       g % 20,
       g % 100,
       g % 1000,
       g % 2000,
       g % 5000,
       g,
       g % 2,
       (g + 1) % 2,
       'r' || g,
       's' || g,
       't' || g
FROM generate_series(0, 9999) g;

CREATE INDEX cal_tenk1_unique1 ON cal_tenk1 (unique1);
CREATE INDEX cal_tenk1_unique2 ON cal_tenk1 (unique2);
CREATE INDEX cal_tenk1_hundred ON cal_tenk1 (hundred);
CREATE INDEX cal_tenk1_thous_tenthous ON cal_tenk1 (thousand, tenthous);

CREATE TABLE cal_onek (unique1 int4, unique2 int4, ten int4, hundred int4);
INSERT INTO cal_onek
SELECT g, (g + 500) % 1000, g % 10, g % 100
FROM generate_series(0, 999) g;
CREATE INDEX cal_onek_unique1 ON cal_onek (unique1);

-- ---------------------------------------------------------------------
-- Partitioned tables for partition-pruning / partition-wise join / agg
-- tests, modeled on PG regress partition_join.sql / partition_aggregate.sql.
-- prt1_p: range-partitioned on a (4 partitions of 250 rows each)
-- prt2_p: range-partitioned on b (matching boundaries, smaller fact)
-- lprt:  list-partitioned on c (3 partitions)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS prt1_p, prt2_p, lprt;

CREATE TABLE prt1_p (a int4, b int4, c name) PARTITION BY RANGE(a);
CREATE TABLE prt1_p_p1 PARTITION OF prt1_p FOR VALUES FROM (0)   TO (250);
CREATE TABLE prt1_p_p2 PARTITION OF prt1_p FOR VALUES FROM (250) TO (500);
CREATE TABLE prt1_p_p3 PARTITION OF prt1_p FOR VALUES FROM (500) TO (750);
CREATE TABLE prt1_p_p4 PARTITION OF prt1_p FOR VALUES FROM (750) TO (1000);
INSERT INTO prt1_p
SELECT g, g % 100, 'k' || (g % 10)
FROM generate_series(0, 999) g;
CREATE INDEX prt1_p_a_idx ON prt1_p (a);

CREATE TABLE prt2_p (a int4, b int4, c name) PARTITION BY RANGE(b);
CREATE TABLE prt2_p_p1 PARTITION OF prt2_p FOR VALUES FROM (0)   TO (50);
CREATE TABLE prt2_p_p2 PARTITION OF prt2_p FOR VALUES FROM (50)  TO (100);
INSERT INTO prt2_p
SELECT g, g, 'k' || (g % 10)
FROM generate_series(0, 99) g;
CREATE INDEX prt2_p_b_idx ON prt2_p (b);

CREATE TABLE lprt (a int4, c text) PARTITION BY LIST(c);
CREATE TABLE lprt_x PARTITION OF lprt FOR VALUES IN ('x');
CREATE TABLE lprt_y PARTITION OF lprt FOR VALUES IN ('y');
CREATE TABLE lprt_z PARTITION OF lprt FOR VALUES IN ('z');
INSERT INTO lprt
SELECT g, (CASE (g % 3) WHEN 0 THEN 'x' WHEN 1 THEN 'y' ELSE 'z' END)
FROM generate_series(1, 999) g;

ANALYZE cal_tenk1;
ANALYZE cal_onek;
ANALYZE prt1_p; ANALYZE prt1_p_p1; ANALYZE prt1_p_p2; ANALYZE prt1_p_p3; ANALYZE prt1_p_p4;
ANALYZE prt2_p; ANALYZE prt2_p_p1; ANALYZE prt2_p_p2;
ANALYZE lprt; ANALYZE lprt_x; ANALYZE lprt_y; ANALYZE lprt_z;
SET enable_partitionwise_join = on;
SET enable_partitionwise_aggregate = on;

-- ---------------------------------------------------------------------
-- Queries -- each line is one EXPLAIN that the driver compares.
-- Keep one statement per line so the driver can split on newlines.
-- ---------------------------------------------------------------------
EXPLAIN SELECT * FROM cal_tenk1;
EXPLAIN SELECT * FROM cal_tenk1 WHERE unique1 = 42;
EXPLAIN SELECT * FROM cal_tenk1 WHERE unique1 < 100;
EXPLAIN SELECT * FROM cal_tenk1 WHERE unique1 BETWEEN 100 AND 200;
EXPLAIN SELECT * FROM cal_tenk1 WHERE hundred = 1;
EXPLAIN SELECT * FROM cal_tenk1 WHERE hundred = 50 AND ten = 5;
EXPLAIN SELECT unique1 FROM cal_tenk1 WHERE unique1 < 50;
EXPLAIN SELECT * FROM cal_tenk1 ORDER BY unique1 LIMIT 5;
EXPLAIN SELECT * FROM cal_tenk1 ORDER BY unique1 LIMIT 100;
EXPLAIN SELECT * FROM cal_tenk1 ORDER BY hundred LIMIT 20;
EXPLAIN SELECT * FROM cal_tenk1 ORDER BY stringu1 LIMIT 10;
EXPLAIN SELECT count(*) FROM cal_tenk1 WHERE unique1 < 1000;
EXPLAIN SELECT sum(unique1) FROM cal_tenk1 WHERE hundred = 50;
EXPLAIN SELECT count(*), ten FROM cal_tenk1 GROUP BY ten;
EXPLAIN SELECT count(*), hundred, ten FROM cal_tenk1 GROUP BY hundred, ten;
EXPLAIN SELECT hundred, min(unique1) FROM cal_tenk1 GROUP BY hundred;
EXPLAIN SELECT sum(unique1), avg(unique2) FROM cal_tenk1;
EXPLAIN SELECT DISTINCT ten, hundred FROM cal_tenk1;
EXPLAIN SELECT * FROM cal_tenk1 UNION ALL SELECT * FROM cal_tenk1;
EXPLAIN SELECT * FROM cal_tenk1 a JOIN cal_tenk1 b ON a.unique1 = b.unique2;
EXPLAIN SELECT 1;
EXPLAIN SELECT 1 + 1;
EXPLAIN SELECT unique1, unique2, unique1 + unique2 AS s FROM cal_tenk1 WHERE unique1 < 10;
EXPLAIN SELECT unique1 + unique2 AS s FROM cal_tenk1 WHERE hundred = 50;
EXPLAIN SELECT * FROM cal_tenk1 WHERE hundred BETWEEN 10 AND 20;
EXPLAIN SELECT * FROM cal_tenk1 WHERE thousand = 999;
EXPLAIN SELECT * FROM cal_tenk1 WHERE thousand = 999 AND tenthous < 5;
EXPLAIN SELECT * FROM cal_tenk1 WHERE hundred = 50 AND ten = 5 AND four = 1;
EXPLAIN SELECT count(*), four, ten FROM cal_tenk1 GROUP BY four, ten;
EXPLAIN SELECT four, max(unique1), min(unique2) FROM cal_tenk1 GROUP BY four;
EXPLAIN SELECT count(*) FILTER (WHERE four = 1) FROM cal_tenk1;
EXPLAIN SELECT * FROM cal_tenk1 ORDER BY twenty, hundred LIMIT 50;
EXPLAIN SELECT * FROM cal_tenk1 t1 JOIN cal_onek t2 ON t1.unique1 = t2.unique2;
EXPLAIN SELECT * FROM cal_tenk1 t1 JOIN cal_onek t2 ON t1.hundred = t2.hundred WHERE t1.ten < 5;
EXPLAIN SELECT count(*) FROM cal_tenk1 t1 JOIN cal_onek t2 ON t1.unique1 = t2.unique2;
EXPLAIN SELECT * FROM cal_tenk1 LEFT JOIN cal_onek ON cal_tenk1.unique1 = cal_onek.unique2;
EXPLAIN SELECT t1.unique1 FROM cal_tenk1 t1 WHERE t1.unique2 IN (SELECT unique1 FROM cal_onek);
EXPLAIN SELECT t1.unique1 FROM cal_tenk1 t1 WHERE EXISTS (SELECT 1 FROM cal_onek WHERE cal_onek.unique1 = t1.unique2);

-- More tightly-controlled cases (same plan shape on both sides)
EXPLAIN SELECT count(*) FROM cal_tenk1 a, cal_onek b WHERE a.unique1 = b.unique2;
EXPLAIN SELECT a.unique1, b.hundred FROM cal_tenk1 a JOIN cal_tenk1 b ON a.unique1 = b.unique2;
EXPLAIN SELECT count(DISTINCT hundred) FROM cal_tenk1;
EXPLAIN SELECT * FROM cal_tenk1 WHERE unique1 > 9000;
EXPLAIN SELECT * FROM cal_tenk1 WHERE unique1 = ANY (ARRAY[1, 5, 10, 50]);
EXPLAIN SELECT * FROM cal_tenk1 WHERE unique1 < 100 ORDER BY unique2 LIMIT 5;
EXPLAIN SELECT hundred FROM cal_tenk1 WHERE hundred BETWEEN 50 AND 60 GROUP BY hundred;
EXPLAIN SELECT * FROM cal_tenk1 WHERE unique1 < 50 AND hundred = 1;
EXPLAIN SELECT * FROM cal_tenk1 WHERE unique1 < 50 OR hundred = 1;
EXPLAIN SELECT max(unique2) FROM cal_tenk1 WHERE unique1 < 5000;
EXPLAIN SELECT count(*), avg(unique1) FROM cal_tenk1 WHERE unique1 < 5000 GROUP BY hundred;
EXPLAIN SELECT * FROM cal_tenk1 ORDER BY string4 DESC LIMIT 100;
EXPLAIN SELECT * FROM cal_tenk1 ORDER BY string4 DESC;
EXPLAIN SELECT * FROM (SELECT * FROM cal_tenk1 ORDER BY unique1) s LIMIT 50;
EXPLAIN SELECT count(*) FROM (SELECT * FROM cal_tenk1 LIMIT 100) s;

-- ---------------------------------------------------------------------
-- Join-focused cases, adapted from PG regress join.sql, join_hash.sql,
-- subselect.sql.  Mix of join types, multi-way joins, joins with
-- filters/aggregates, anti/semi joins, cross joins, and LATERAL.
-- ---------------------------------------------------------------------
EXPLAIN SELECT * FROM cal_tenk1 a JOIN cal_onek b ON a.unique1 = b.unique1 JOIN cal_onek c ON a.unique2 = c.unique1;
EXPLAIN SELECT * FROM cal_tenk1 a JOIN cal_tenk1 b ON a.unique1 = b.unique2 JOIN cal_tenk1 c ON b.unique1 = c.unique2;
EXPLAIN SELECT count(*) FROM cal_tenk1 a JOIN cal_onek b USING (unique1) JOIN cal_onek c USING (unique1);
EXPLAIN SELECT * FROM cal_tenk1 a JOIN cal_onek b ON a.hundred = b.hundred WHERE a.ten < 5 AND b.hundred < 10;
EXPLAIN SELECT * FROM cal_onek a CROSS JOIN cal_onek b WHERE a.unique1 = 1 AND b.unique1 < 10;
EXPLAIN SELECT count(*) FROM cal_onek a, cal_onek b WHERE a.unique1 < 5;
EXPLAIN SELECT * FROM cal_tenk1 a FULL JOIN cal_onek b ON a.unique1 = b.unique2;
EXPLAIN SELECT * FROM cal_onek a RIGHT JOIN cal_tenk1 b ON a.unique1 = b.unique2;
EXPLAIN SELECT * FROM cal_tenk1 a WHERE NOT EXISTS (SELECT 1 FROM cal_onek b WHERE b.unique1 = a.unique1);
EXPLAIN SELECT * FROM cal_tenk1 a WHERE a.unique1 NOT IN (SELECT unique1 FROM cal_onek);
EXPLAIN SELECT * FROM cal_tenk1 a JOIN cal_tenk1 b ON a.hundred = b.hundred AND a.ten = b.ten;
EXPLAIN SELECT * FROM cal_tenk1 a JOIN cal_onek b ON a.unique1 = b.unique2 WHERE a.hundred < 5;
EXPLAIN SELECT * FROM cal_tenk1 a JOIN cal_onek b ON a.unique1 = b.unique2 WHERE b.hundred < 5;
EXPLAIN SELECT a.hundred, count(*) FROM cal_tenk1 a JOIN cal_onek b ON a.unique1 = b.unique2 GROUP BY a.hundred;
EXPLAIN SELECT a.hundred, max(b.unique1) FROM cal_tenk1 a JOIN cal_onek b ON a.hundred = b.hundred GROUP BY a.hundred;
EXPLAIN SELECT a.unique1 FROM cal_tenk1 a JOIN cal_tenk1 b ON a.unique1 < b.unique1 AND a.hundred = b.hundred WHERE a.unique1 < 100;
EXPLAIN SELECT * FROM cal_tenk1 fact JOIN cal_onek d1 ON fact.unique1 = d1.unique1 JOIN cal_onek d2 ON fact.unique2 = d2.unique1 WHERE d1.hundred = 5;
EXPLAIN SELECT b.* FROM cal_tenk1 a JOIN cal_tenk1 b ON a.unique1 = b.unique2 WHERE a.unique1 < 50;
EXPLAIN SELECT count(*) FROM (SELECT unique1 FROM cal_tenk1 WHERE hundred = 5) s JOIN cal_onek b ON s.unique1 = b.unique2;
EXPLAIN SELECT a.unique1, l.cnt FROM cal_onek a, LATERAL (SELECT count(*) AS cnt FROM cal_tenk1 b WHERE b.unique1 < a.unique1) l WHERE a.unique1 < 10;
EXPLAIN SELECT * FROM cal_tenk1 a JOIN cal_onek b ON a.hundred = b.hundred AND a.ten < b.ten;
EXPLAIN SELECT count(*) FROM cal_tenk1 a JOIN cal_onek b ON a.thousand = b.unique1;
EXPLAIN SELECT * FROM cal_onek a LEFT JOIN cal_tenk1 b ON a.unique1 = b.unique1 WHERE b.unique1 IS NULL;
EXPLAIN SELECT a.unique1, b.unique1 FROM cal_tenk1 a, cal_tenk1 b WHERE a.unique1 = b.unique2 AND a.hundred = 5;
EXPLAIN SELECT * FROM cal_tenk1 a JOIN cal_onek b ON a.unique1 = b.unique1 ORDER BY a.hundred LIMIT 20;
EXPLAIN SELECT a.hundred FROM cal_tenk1 a JOIN cal_onek b ON a.hundred = b.hundred GROUP BY a.hundred HAVING count(*) > 5;
EXPLAIN SELECT * FROM cal_tenk1 a JOIN cal_onek b ON a.unique1 = b.unique2 JOIN cal_onek c ON b.hundred = c.hundred;
EXPLAIN SELECT DISTINCT a.hundred FROM cal_tenk1 a JOIN cal_onek b ON a.unique1 = b.unique2;
EXPLAIN SELECT count(*) FROM cal_tenk1 a WHERE a.unique1 IN (SELECT unique2 FROM cal_onek WHERE unique1 < 100);
EXPLAIN SELECT * FROM cal_tenk1 a JOIN cal_tenk1 b ON a.unique2 = b.unique2 WHERE a.unique1 = 5;

-- ---------------------------------------------------------------------
-- Aggregate-focused cases, adapted from PG regress aggregates.sql.
-- Mix of scalar aggs, group-by, having, filter, multi-agg, agg+join,
-- agg+distinct, agg+ordering, rollup/grouping sets.
-- ---------------------------------------------------------------------
EXPLAIN SELECT avg(ten) FROM cal_onek;
EXPLAIN SELECT sum(unique1) FROM cal_tenk1;
EXPLAIN SELECT max(hundred), min(hundred) FROM cal_tenk1;
EXPLAIN SELECT stddev_pop(unique1) FROM cal_tenk1;
EXPLAIN SELECT var_samp(unique1) FROM cal_tenk1;
EXPLAIN SELECT avg(unique1::numeric) FROM cal_tenk1;
EXPLAIN SELECT count(*), sum(unique1), avg(unique2)::numeric(10,2), max(hundred), min(ten) FROM cal_tenk1;
EXPLAIN SELECT ten, sum(unique1) FROM cal_onek GROUP BY ten ORDER BY ten;
EXPLAIN SELECT hundred, count(*), avg(unique1) FROM cal_tenk1 GROUP BY hundred;
EXPLAIN SELECT hundred, count(*), avg(unique1) FROM cal_tenk1 GROUP BY hundred ORDER BY hundred;
EXPLAIN SELECT hundred, count(*), avg(unique1) FROM cal_tenk1 GROUP BY hundred HAVING count(*) > 50;
EXPLAIN SELECT ten, hundred, count(*) FROM cal_onek GROUP BY ten, hundred;
EXPLAIN SELECT ten, hundred, count(*) FROM cal_onek GROUP BY ROLLUP(ten, hundred);
EXPLAIN SELECT ten, hundred, count(*) FROM cal_onek GROUP BY CUBE(ten, hundred);
EXPLAIN SELECT ten, hundred, count(*) FROM cal_onek GROUP BY GROUPING SETS ((ten), (hundred), ());
EXPLAIN SELECT count(DISTINCT unique1) FROM cal_tenk1;
EXPLAIN SELECT count(DISTINCT hundred) FROM cal_tenk1 WHERE unique1 < 1000;
EXPLAIN SELECT count(*) FILTER (WHERE hundred < 50) FROM cal_tenk1;
EXPLAIN SELECT count(*) FILTER (WHERE hundred < 50), count(*) FILTER (WHERE hundred >= 50) FROM cal_tenk1;
EXPLAIN SELECT hundred, count(*) FILTER (WHERE ten = 0) FROM cal_tenk1 GROUP BY hundred;
EXPLAIN SELECT array_agg(unique1 ORDER BY unique1) FROM cal_onek WHERE hundred = 5;
EXPLAIN SELECT string_agg(stringu1::text, ',' ORDER BY unique1) FROM cal_tenk1 WHERE unique1 < 10;
EXPLAIN SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY unique1) FROM cal_tenk1;
EXPLAIN SELECT mode() WITHIN GROUP (ORDER BY hundred) FROM cal_tenk1;
EXPLAIN SELECT count(*), avg(unique1) FROM cal_tenk1 GROUP BY GROUPING SETS ((hundred), (ten));
EXPLAIN SELECT a.hundred, count(*), avg(b.unique1) FROM cal_tenk1 a JOIN cal_onek b ON a.unique1 = b.unique2 GROUP BY a.hundred;
EXPLAIN SELECT a.hundred, count(*) FILTER (WHERE b.unique1 < 100) FROM cal_tenk1 a JOIN cal_onek b ON a.hundred = b.hundred GROUP BY a.hundred;
EXPLAIN SELECT sum(s.cnt) FROM (SELECT hundred, count(*) AS cnt FROM cal_tenk1 GROUP BY hundred) s;
EXPLAIN SELECT max(unique1) FROM cal_tenk1 WHERE unique1 < 100;
EXPLAIN SELECT min(unique1) FROM cal_tenk1;
EXPLAIN SELECT bool_and(unique1 < 9999), bool_or(unique1 = 0) FROM cal_tenk1;
EXPLAIN SELECT count(*) FROM cal_tenk1 WHERE unique1 IN (SELECT max(unique1) FROM cal_onek);
EXPLAIN SELECT corr(unique1, unique2), covar_pop(unique1, unique2) FROM cal_tenk1;
EXPLAIN SELECT hundred, ten, count(*) FROM cal_tenk1 GROUP BY hundred, ten HAVING sum(unique1) > 1000;
EXPLAIN SELECT hundred FROM cal_tenk1 GROUP BY hundred HAVING count(*) > 90 ORDER BY hundred LIMIT 10;

-- ---------------------------------------------------------------------
-- Window function cases, adapted from PG regress window.sql.  Covers
-- PARTITION BY only, ORDER BY only, both, ranking functions, offset
-- functions (lag/lead), value-position functions (first/last/nth_value),
-- explicit frames (ROWS / RANGE / GROUPS), named windows, multiple
-- windows in one query, window over aggregate, window with subquery,
-- window-after-filter, and window expressions in WHERE/SELECT/ORDER BY.
-- ---------------------------------------------------------------------
EXPLAIN SELECT hundred, sum(unique1) OVER (PARTITION BY hundred) FROM cal_tenk1;
EXPLAIN SELECT hundred, rank() OVER (PARTITION BY hundred ORDER BY unique1) FROM cal_tenk1;
EXPLAIN SELECT row_number() OVER (ORDER BY unique1) FROM cal_tenk1 WHERE unique2 < 100;
EXPLAIN SELECT dense_rank() OVER (PARTITION BY ten ORDER BY hundred) FROM cal_tenk1;
EXPLAIN SELECT percent_rank() OVER (PARTITION BY ten ORDER BY hundred) FROM cal_tenk1;
EXPLAIN SELECT cume_dist() OVER (PARTITION BY ten ORDER BY hundred) FROM cal_tenk1;
EXPLAIN SELECT ntile(10) OVER (ORDER BY unique1) FROM cal_tenk1;
EXPLAIN SELECT lag(unique1) OVER (PARTITION BY hundred ORDER BY unique1) FROM cal_tenk1;
EXPLAIN SELECT lead(unique1, 2, 0) OVER (PARTITION BY hundred ORDER BY unique1) FROM cal_tenk1;
EXPLAIN SELECT first_value(unique1) OVER (PARTITION BY hundred ORDER BY unique2) FROM cal_tenk1;
EXPLAIN SELECT last_value(unique1) OVER (PARTITION BY hundred ORDER BY unique2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM cal_tenk1;
EXPLAIN SELECT nth_value(unique1, 3) OVER (PARTITION BY hundred) FROM cal_tenk1;
EXPLAIN SELECT count(*) OVER () FROM cal_tenk1 WHERE unique2 < 50;
EXPLAIN SELECT count(*) OVER (PARTITION BY hundred) FROM cal_tenk1;
EXPLAIN SELECT avg(unique1) OVER (PARTITION BY hundred ORDER BY unique2 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM cal_tenk1;
EXPLAIN SELECT sum(unique1) OVER (PARTITION BY hundred ORDER BY unique2 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM cal_tenk1;
EXPLAIN SELECT min(unique1) OVER (ORDER BY unique2 GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM cal_tenk1;
EXPLAIN SELECT sum(unique1) OVER w, count(*) OVER w FROM cal_tenk1 WINDOW w AS (PARTITION BY hundred ORDER BY unique2);
EXPLAIN SELECT sum(unique1) OVER w1, rank() OVER w2 FROM cal_tenk1 WINDOW w1 AS (PARTITION BY hundred), w2 AS (PARTITION BY ten ORDER BY unique1);
EXPLAIN SELECT hundred, count(*), sum(count(*)) OVER (PARTITION BY hundred / 10 ORDER BY hundred) FROM cal_tenk1 GROUP BY hundred;
EXPLAIN SELECT count(*) OVER (PARTITION BY hundred) FROM (SELECT * FROM cal_tenk1 WHERE unique2 < 100) s;
EXPLAIN SELECT * FROM (SELECT rank() OVER (PARTITION BY hundred ORDER BY unique1) AS r, * FROM cal_tenk1) s WHERE r <= 3;
EXPLAIN SELECT rank() OVER (PARTITION BY hundred ORDER BY unique1) FROM cal_tenk1 ORDER BY hundred, unique1 LIMIT 100;
EXPLAIN SELECT sum(a.unique1) OVER (PARTITION BY a.hundred) FROM cal_tenk1 a JOIN cal_onek b ON a.unique1 = b.unique2;
EXPLAIN SELECT lag(b.unique1) OVER (PARTITION BY a.hundred ORDER BY b.unique2) FROM cal_tenk1 a JOIN cal_onek b ON a.hundred = b.hundred;
EXPLAIN SELECT hundred, sum(unique1) OVER (PARTITION BY hundred), avg(unique2) OVER (PARTITION BY hundred) FROM cal_tenk1;
EXPLAIN SELECT row_number() OVER (), unique1 FROM cal_tenk1;
EXPLAIN SELECT count(*) OVER (ORDER BY unique1 ROWS BETWEEN 100 PRECEDING AND 100 FOLLOWING) FROM cal_tenk1;
EXPLAIN SELECT hundred, sum(unique1) FILTER (WHERE ten = 0) OVER (PARTITION BY hundred) FROM cal_tenk1;

-- ---------------------------------------------------------------------
-- Set operations, adapted from PG regress union.sql.
-- ---------------------------------------------------------------------
EXPLAIN SELECT unique1 FROM cal_tenk1 UNION SELECT unique1 FROM cal_onek;
EXPLAIN SELECT unique1 FROM cal_tenk1 UNION ALL SELECT unique1 FROM cal_onek;
EXPLAIN SELECT unique1 FROM cal_tenk1 INTERSECT SELECT unique1 FROM cal_onek;
EXPLAIN SELECT unique1 FROM cal_tenk1 INTERSECT ALL SELECT unique1 FROM cal_onek;
EXPLAIN SELECT unique1 FROM cal_tenk1 EXCEPT SELECT unique1 FROM cal_onek;
EXPLAIN SELECT unique1 FROM cal_tenk1 EXCEPT ALL SELECT unique1 FROM cal_onek;
EXPLAIN SELECT hundred, count(*) FROM (SELECT hundred FROM cal_tenk1 UNION ALL SELECT hundred FROM cal_onek) s GROUP BY hundred;
EXPLAIN SELECT unique1 FROM cal_tenk1 WHERE hundred = 5 UNION ALL SELECT unique1 FROM cal_onek WHERE hundred = 5;

-- ---------------------------------------------------------------------
-- CTE and recursive CTE, adapted from PG regress with.sql.
-- ---------------------------------------------------------------------
EXPLAIN WITH t AS (SELECT * FROM cal_tenk1 WHERE hundred = 5) SELECT count(*) FROM t;
EXPLAIN WITH t AS (SELECT hundred, count(*) AS c FROM cal_tenk1 GROUP BY hundred) SELECT * FROM t WHERE c > 50;
EXPLAIN WITH t AS MATERIALIZED (SELECT * FROM cal_tenk1 WHERE unique1 < 100) SELECT count(*) FROM t JOIN cal_onek ON t.unique1 = cal_onek.unique2;
EXPLAIN WITH t AS NOT MATERIALIZED (SELECT * FROM cal_tenk1 WHERE unique1 < 100) SELECT count(*) FROM t;
EXPLAIN WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 10) SELECT * FROM r;
EXPLAIN WITH a AS (SELECT * FROM cal_tenk1 WHERE unique1 < 100), b AS (SELECT * FROM cal_onek WHERE unique1 < 100) SELECT a.unique1, b.unique2 FROM a JOIN b ON a.unique1 = b.unique1;
EXPLAIN WITH t AS (SELECT unique1, hundred FROM cal_tenk1) SELECT hundred, count(*) FROM t WHERE unique1 < 500 GROUP BY hundred;

-- ---------------------------------------------------------------------
-- Subselect and correlated subqueries, adapted from PG regress subselect.sql.
-- ---------------------------------------------------------------------
EXPLAIN SELECT * FROM cal_tenk1 WHERE unique1 = (SELECT max(unique1) FROM cal_onek);
EXPLAIN SELECT * FROM cal_tenk1 WHERE unique1 < (SELECT avg(unique1) FROM cal_onek);
EXPLAIN SELECT unique1, (SELECT count(*) FROM cal_onek WHERE cal_onek.unique1 < cal_tenk1.unique1) AS c FROM cal_tenk1 WHERE unique1 < 10;
EXPLAIN SELECT * FROM cal_tenk1 WHERE EXISTS (SELECT 1 FROM cal_onek WHERE cal_onek.unique1 = cal_tenk1.hundred);
EXPLAIN SELECT * FROM cal_tenk1 WHERE NOT EXISTS (SELECT 1 FROM cal_onek WHERE cal_onek.unique1 = cal_tenk1.hundred);
EXPLAIN SELECT * FROM cal_tenk1 WHERE unique1 = ANY (SELECT unique1 FROM cal_onek WHERE unique1 < 100);
EXPLAIN SELECT * FROM cal_tenk1 WHERE unique1 = ALL (ARRAY[(SELECT max(unique1) FROM cal_onek)]);
EXPLAIN SELECT * FROM cal_tenk1 WHERE unique1 > ALL (SELECT unique1 FROM cal_onek WHERE hundred = 5);
EXPLAIN SELECT * FROM (SELECT unique1, unique2 FROM cal_tenk1) s WHERE s.unique1 < 10;
EXPLAIN SELECT * FROM (SELECT hundred, count(*) c FROM cal_tenk1 GROUP BY hundred) s WHERE c > 90;

-- ---------------------------------------------------------------------
-- DISTINCT (basic) and DISTINCT ON, adapted from PG regress
-- select_distinct.sql / select_distinct_on.sql.
-- ---------------------------------------------------------------------
EXPLAIN SELECT DISTINCT hundred FROM cal_tenk1;
EXPLAIN SELECT DISTINCT hundred, ten FROM cal_tenk1;
EXPLAIN SELECT DISTINCT hundred FROM cal_tenk1 ORDER BY hundred;
EXPLAIN SELECT DISTINCT unique1 FROM cal_tenk1 WHERE unique1 < 100;
EXPLAIN SELECT DISTINCT ON (hundred) hundred, unique1 FROM cal_tenk1 ORDER BY hundred, unique1;
EXPLAIN SELECT DISTINCT ON (hundred) hundred, unique1 FROM cal_tenk1 ORDER BY hundred, unique1 DESC;
EXPLAIN SELECT DISTINCT ON (hundred, ten) hundred, ten, unique1 FROM cal_tenk1 ORDER BY hundred, ten, unique1;

-- ---------------------------------------------------------------------
-- CASE expressions, adapted from PG regress case.sql.
-- ---------------------------------------------------------------------
EXPLAIN SELECT CASE WHEN hundred < 50 THEN 'lo' ELSE 'hi' END FROM cal_tenk1;
EXPLAIN SELECT CASE hundred WHEN 0 THEN 'zero' WHEN 1 THEN 'one' ELSE 'many' END FROM cal_tenk1;
EXPLAIN SELECT sum(CASE WHEN ten = 5 THEN unique1 ELSE 0 END) FROM cal_tenk1;
EXPLAIN SELECT hundred, sum(CASE WHEN ten < 5 THEN 1 ELSE 0 END) FROM cal_tenk1 GROUP BY hundred;
EXPLAIN SELECT * FROM cal_tenk1 WHERE CASE WHEN hundred < 50 THEN unique1 ELSE unique2 END < 100;
EXPLAIN SELECT COALESCE(NULLIF(unique1, 0), -1) FROM cal_tenk1 WHERE unique1 < 10;

-- ---------------------------------------------------------------------
-- LIMIT/OFFSET, adapted from PG regress limit.sql.
-- ---------------------------------------------------------------------
EXPLAIN SELECT * FROM cal_tenk1 ORDER BY unique1 OFFSET 100 LIMIT 10;
EXPLAIN SELECT * FROM cal_tenk1 ORDER BY unique1 LIMIT 10 OFFSET 1000;
EXPLAIN SELECT * FROM cal_tenk1 ORDER BY unique1 OFFSET 9990;
EXPLAIN SELECT * FROM cal_tenk1 ORDER BY hundred LIMIT 1;
EXPLAIN SELECT * FROM cal_tenk1 OFFSET 0 LIMIT 0;
EXPLAIN SELECT * FROM cal_tenk1 LIMIT NULL;
EXPLAIN SELECT * FROM cal_tenk1 LIMIT ALL;
EXPLAIN SELECT * FROM cal_tenk1 WHERE hundred = 5 ORDER BY unique1 LIMIT 5;
EXPLAIN SELECT count(*) FROM (SELECT * FROM cal_tenk1 ORDER BY unique1 LIMIT 100 OFFSET 50) s;

-- ---------------------------------------------------------------------
-- GROUPING SETS expansion, adapted from PG regress groupingsets.sql.
-- ---------------------------------------------------------------------
EXPLAIN SELECT ten, hundred, count(*) FROM cal_tenk1 GROUP BY GROUPING SETS ((ten), (hundred), ());
EXPLAIN SELECT ten, hundred, count(*) FROM cal_tenk1 GROUP BY ROLLUP(ten, hundred);
EXPLAIN SELECT ten, hundred, count(*) FROM cal_tenk1 GROUP BY CUBE(ten, hundred);
EXPLAIN SELECT ten, count(*) FROM cal_tenk1 GROUP BY GROUPING SETS ((ten), ()) HAVING count(*) > 100;

-- ---------------------------------------------------------------------
-- Pattern matching: LIKE / ILIKE / regex, adapted from PG regress
-- strings.sql / regex.sql.
-- ---------------------------------------------------------------------
EXPLAIN SELECT * FROM cal_tenk1 WHERE stringu1 LIKE 'A%';
EXPLAIN SELECT * FROM cal_tenk1 WHERE stringu1 LIKE '%XYZ%';
EXPLAIN SELECT * FROM cal_tenk1 WHERE stringu1 LIKE 'r1_';
EXPLAIN SELECT * FROM cal_tenk1 WHERE stringu1 NOT LIKE 'r%';
EXPLAIN SELECT * FROM cal_tenk1 WHERE stringu1 ILIKE 'r5%';
EXPLAIN SELECT * FROM cal_tenk1 WHERE stringu1 ~ '^r1[0-9]+$';
EXPLAIN SELECT count(*) FROM cal_tenk1 WHERE string4 LIKE 't%';

-- ---------------------------------------------------------------------
-- NULL semantics: IS NULL / IS NOT NULL / COALESCE / NULLIF.
-- Note: our seed data has no NULLs, but the cost path still differs.
-- ---------------------------------------------------------------------
EXPLAIN SELECT * FROM cal_tenk1 WHERE unique1 IS NULL;
EXPLAIN SELECT * FROM cal_tenk1 WHERE unique1 IS NOT NULL;
EXPLAIN SELECT count(*) FROM cal_tenk1 WHERE stringu1 IS NULL;
EXPLAIN SELECT * FROM cal_tenk1 WHERE COALESCE(stringu1, 'x') = 'r5';
EXPLAIN SELECT NULLIF(unique1, 0) FROM cal_tenk1 WHERE unique1 < 10;

-- ---------------------------------------------------------------------
-- SAOP and IN-list variations, adapted from PG regress join.sql/select.sql.
-- ---------------------------------------------------------------------
EXPLAIN SELECT * FROM cal_tenk1 WHERE unique1 IN (1, 2, 3);
EXPLAIN SELECT * FROM cal_tenk1 WHERE unique1 IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
EXPLAIN SELECT * FROM cal_tenk1 WHERE hundred IN (1, 5, 10, 15, 20);
EXPLAIN SELECT * FROM cal_tenk1 WHERE thousand IN (10, 100, 500);
EXPLAIN SELECT * FROM cal_tenk1 WHERE unique1 NOT IN (1, 2, 3, 4, 5);

-- ---------------------------------------------------------------------
-- LATERAL with limit / aggregate, adapted from PG regress join.sql.
-- ---------------------------------------------------------------------
EXPLAIN SELECT a.unique1, b.unique2 FROM cal_tenk1 a CROSS JOIN LATERAL (SELECT unique2 FROM cal_onek WHERE cal_onek.unique1 < a.unique1 LIMIT 1) b WHERE a.unique1 < 10;
EXPLAIN SELECT a.unique1, l.s FROM cal_onek a, LATERAL (SELECT sum(unique2) s FROM cal_tenk1 WHERE cal_tenk1.hundred = a.hundred) l WHERE a.unique1 < 20;
EXPLAIN SELECT * FROM cal_tenk1 a, LATERAL (SELECT * FROM cal_onek b WHERE b.unique1 = a.unique1 ORDER BY b.unique2 LIMIT 5) lim WHERE a.unique1 < 30;

-- ---------------------------------------------------------------------
-- Multi-level correlated subqueries, adapted from PG regress subselect.sql.
-- ---------------------------------------------------------------------
EXPLAIN SELECT a.unique1 FROM cal_tenk1 a WHERE EXISTS (SELECT 1 FROM cal_onek b WHERE b.unique1 = a.unique1 AND EXISTS (SELECT 1 FROM cal_onek c WHERE c.unique2 = b.unique2));
EXPLAIN SELECT a.unique1, (SELECT (SELECT max(c.unique1) FROM cal_onek c WHERE c.hundred = b.hundred) FROM cal_onek b WHERE b.unique1 = a.hundred) FROM cal_tenk1 a WHERE a.unique1 < 20;

-- ---------------------------------------------------------------------
-- CTE referenced multiple times.
-- ---------------------------------------------------------------------
EXPLAIN WITH t AS (SELECT * FROM cal_tenk1 WHERE hundred < 5) SELECT count(*) FROM t a, t b WHERE a.unique1 = b.unique2;
EXPLAIN WITH t AS MATERIALIZED (SELECT * FROM cal_tenk1 WHERE hundred = 5) SELECT a.unique1, b.unique2 FROM t a JOIN t b ON a.unique2 = b.unique1;

-- ---------------------------------------------------------------------
-- ORDER BY / GROUP BY expressions.
-- ---------------------------------------------------------------------
EXPLAIN SELECT unique1 FROM cal_tenk1 ORDER BY unique1 + unique2;
EXPLAIN SELECT unique1, unique2 FROM cal_tenk1 ORDER BY unique1 * 2 LIMIT 10;
EXPLAIN SELECT hundred / 10 AS g, count(*) FROM cal_tenk1 GROUP BY hundred / 10;
EXPLAIN SELECT count(*) FROM cal_tenk1 GROUP BY (CASE WHEN hundred < 50 THEN 'lo' ELSE 'hi' END);
EXPLAIN SELECT * FROM cal_tenk1 ORDER BY CASE WHEN hundred < 50 THEN unique1 ELSE -unique1 END LIMIT 20;

-- ---------------------------------------------------------------------
-- VALUES list (as table) and FROM-list combinations.
-- ---------------------------------------------------------------------
EXPLAIN SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS v(x);
EXPLAIN SELECT * FROM cal_tenk1 t JOIN (VALUES (1), (5), (10), (50)) AS v(x) ON t.unique1 = v.x;
EXPLAIN SELECT v.x, count(*) FROM cal_tenk1 t JOIN (VALUES (0), (10), (50)) AS v(x) ON t.hundred = v.x GROUP BY v.x;

-- ---------------------------------------------------------------------
-- NOT IN / NOT EXISTS anti-join variants.
-- ---------------------------------------------------------------------
EXPLAIN SELECT * FROM cal_tenk1 WHERE hundred NOT IN (SELECT unique1 FROM cal_onek WHERE unique1 < 50);
EXPLAIN SELECT count(*) FROM cal_onek a WHERE NOT EXISTS (SELECT 1 FROM cal_tenk1 b WHERE b.unique1 = a.unique1 AND b.hundred < 5);
EXPLAIN SELECT * FROM cal_onek a WHERE a.unique1 NOT IN (SELECT hundred FROM cal_tenk1);

-- ---------------------------------------------------------------------
-- GROUPING() in aggregate output (with ROLLUP).
-- ---------------------------------------------------------------------
EXPLAIN SELECT GROUPING(hundred), GROUPING(ten), hundred, ten, count(*) FROM cal_tenk1 GROUP BY ROLLUP(hundred, ten);

-- ---------------------------------------------------------------------
-- ARRAY constructors, slicing, ANY/ALL in WHERE.
-- ---------------------------------------------------------------------
EXPLAIN SELECT ARRAY[unique1, unique2] FROM cal_tenk1 WHERE unique1 < 10;
EXPLAIN SELECT * FROM cal_tenk1 WHERE unique1 = ANY (ARRAY[1, 2, 3, 4, 5]);
EXPLAIN SELECT * FROM cal_tenk1 WHERE unique1 < ALL (ARRAY[10, 20, 30]);

-- ---------------------------------------------------------------------
-- Partition table tests, adapted from PG regress partition_prune.sql /
-- partition_join.sql / partition_aggregate.sql.
-- ---------------------------------------------------------------------

-- Partition pruning: equality / range / IN / IS NULL on partition key
EXPLAIN SELECT * FROM prt1_p;
EXPLAIN SELECT * FROM prt1_p WHERE a < 250;
EXPLAIN SELECT * FROM prt1_p WHERE a = 500;
EXPLAIN SELECT * FROM prt1_p WHERE a BETWEEN 200 AND 300;
EXPLAIN SELECT * FROM prt1_p WHERE a IN (10, 300, 600);
EXPLAIN SELECT * FROM prt1_p WHERE a < 100 OR a > 900;
EXPLAIN SELECT * FROM prt1_p WHERE a IS NULL;
EXPLAIN SELECT * FROM prt1_p WHERE b = 50;
EXPLAIN SELECT count(*) FROM prt1_p WHERE a >= 500;

-- LIST partition pruning
EXPLAIN SELECT * FROM lprt WHERE c = 'x';
EXPLAIN SELECT * FROM lprt WHERE c IN ('x', 'y');
EXPLAIN SELECT * FROM lprt WHERE c <> 'z';
EXPLAIN SELECT c, count(*) FROM lprt GROUP BY c;
EXPLAIN SELECT count(*) FROM lprt WHERE c = 'x' AND a < 100;

-- Partition-wise join (matching keys + boundaries)
EXPLAIN SELECT count(*) FROM prt1_p t1 JOIN prt2_p t2 ON t1.a = t2.b;
EXPLAIN SELECT t1.a, t2.b FROM prt1_p t1 JOIN prt2_p t2 ON t1.a = t2.b WHERE t1.b < 10;
EXPLAIN SELECT t1.c, t1.a FROM prt1_p t1 JOIN prt2_p t2 ON t1.a = t2.b WHERE t1.a < 100;

-- Partition-wise aggregate (group key = partition key)
EXPLAIN SELECT a, count(*) FROM prt1_p GROUP BY a;
EXPLAIN SELECT a, sum(b) FROM prt1_p WHERE a < 500 GROUP BY a;
EXPLAIN SELECT c, count(*) FROM prt1_p GROUP BY c;
EXPLAIN SELECT c, count(*), avg(a) FROM lprt GROUP BY c;
EXPLAIN SELECT c, sum(a) FROM lprt WHERE a < 500 GROUP BY c;

-- Cross-partition / non-key filter
EXPLAIN SELECT * FROM prt1_p WHERE c = 'k1';
EXPLAIN SELECT a FROM prt1_p WHERE a < 100 ORDER BY a LIMIT 10;
EXPLAIN SELECT count(*) FROM prt1_p t1 JOIN cal_onek t2 ON t1.a = t2.unique1;
