-- pg_orca partition regression tests
-- Ported from testrepo/mpp/gpdb/tests/storage/basic/partition/sql/mpp18162_duplicate_value_across_parts.sql
-- MPP-18162: List partitioning correctness -- no "duplicate values" errors

LOAD 'pg_orca';
SET pg_orca.enable_orca = on;
SET client_min_messages = warning;
SET DateStyle = 'ISO';

-- ===================================================================
-- CASE 1: LIST partitioning on int
-- ===================================================================

DROP TABLE IF EXISTS mpp18162_int;
CREATE TABLE mpp18162_int (i1 int, i2 int)
PARTITION BY LIST (i1);
CREATE TABLE mpp18162_int_p1 PARTITION OF mpp18162_int FOR VALUES IN (1);
CREATE TABLE mpp18162_int_p2 PARTITION OF mpp18162_int FOR VALUES IN (2);
CREATE TABLE mpp18162_int_p3 PARTITION OF mpp18162_int FOR VALUES IN (3);

-- Insert one row into each partition
INSERT INTO mpp18162_int VALUES (1, 10);
INSERT INTO mpp18162_int VALUES (2, 20);
INSERT INTO mpp18162_int VALUES (3, 30);

-- Verify rows landed in correct partitions
SELECT i1, count(*) FROM mpp18162_int GROUP BY i1 ORDER BY i1;

-- Insert a value that has no matching partition -- should ERROR
INSERT INTO mpp18162_int VALUES (99, 99);

-- EXPLAIN shows ORCA static partition pruning
EXPLAIN (costs off) SELECT * FROM mpp18162_int WHERE i1 = 2;

-- SELECT with partition key filter
SELECT * FROM mpp18162_int WHERE i1 = 2;

DROP TABLE mpp18162_int;

-- ===================================================================
-- CASE 2: LIST partitioning on text
-- ===================================================================

DROP TABLE IF EXISTS mpp18162_text;
CREATE TABLE mpp18162_text (i1 text, i2 varchar(10))
PARTITION BY LIST (i1);
CREATE TABLE mpp18162_text_p1 PARTITION OF mpp18162_text FOR VALUES IN ('1');
CREATE TABLE mpp18162_text_p2 PARTITION OF mpp18162_text FOR VALUES IN ('2');
CREATE TABLE mpp18162_text_p3 PARTITION OF mpp18162_text FOR VALUES IN ('3');

-- Insert one row into each partition
INSERT INTO mpp18162_text VALUES ('1', 'a');
INSERT INTO mpp18162_text VALUES ('2', 'b');
INSERT INTO mpp18162_text VALUES ('3', 'c');

-- Verify rows landed in correct partitions
SELECT i1, count(*) FROM mpp18162_text GROUP BY i1 ORDER BY i1;

-- Insert a value that has no matching partition -- should ERROR
INSERT INTO mpp18162_text VALUES ('99', 'z');

-- EXPLAIN shows ORCA static partition pruning
EXPLAIN (costs off) SELECT * FROM mpp18162_text WHERE i1 = '2';

-- SELECT with partition key filter
SELECT * FROM mpp18162_text WHERE i1 = '2';

DROP TABLE mpp18162_text;

-- ===================================================================
-- CASE 3: LIST partitioning on date
-- ===================================================================

DROP TABLE IF EXISTS mpp18162_date;
CREATE TABLE mpp18162_date (i1 date, i2 date)
PARTITION BY LIST (i1);
CREATE TABLE mpp18162_date_p1 PARTITION OF mpp18162_date FOR VALUES IN ('2008-01-01');
CREATE TABLE mpp18162_date_p2 PARTITION OF mpp18162_date FOR VALUES IN ('2008-02-01');
CREATE TABLE mpp18162_date_p3 PARTITION OF mpp18162_date FOR VALUES IN ('2008-03-01');
CREATE TABLE mpp18162_date_p4 PARTITION OF mpp18162_date FOR VALUES IN ('2008-04-01');

-- Insert one row into each partition
INSERT INTO mpp18162_date VALUES ('2008-01-01', '2008-01-01');
INSERT INTO mpp18162_date VALUES ('2008-02-01', '2008-02-01');
INSERT INTO mpp18162_date VALUES ('2008-03-01', '2008-03-01');
INSERT INTO mpp18162_date VALUES ('2008-04-01', '2008-04-01');

-- Verify rows landed in correct partitions
SELECT i1, count(*) FROM mpp18162_date GROUP BY i1 ORDER BY i1;

-- Insert a value that has no matching partition -- should ERROR
INSERT INTO mpp18162_date VALUES ('2008-05-01', '2008-05-01');

-- EXPLAIN shows ORCA static partition pruning
EXPLAIN (costs off) SELECT * FROM mpp18162_date WHERE i1 = '2008-03-01';

-- SELECT with partition key filter
SELECT * FROM mpp18162_date WHERE i1 = '2008-03-01';

DROP TABLE mpp18162_date;
