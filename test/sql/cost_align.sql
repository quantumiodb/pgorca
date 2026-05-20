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

ANALYZE cal_tenk1;
ANALYZE cal_onek;

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
