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
