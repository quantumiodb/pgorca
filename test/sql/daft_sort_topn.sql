-- =====================================================================
-- daft_sort_topn.sql  (P1-003-C)
-- Real PG/Daft Sort and TopN differential, admission and memory gates.
-- =====================================================================

SET client_min_messages = warning;
SET pg_orca.enable_orca = on;
SET pg_orca.daft_enable = 'off';
SET pg_orca.daft_cost_calibration_profile = 'test_prefer_daft_v1';
SET pg_orca.cost_model = 'pg';
SET pg_orca.daft_min_cost = 0;
SET pg_orca.daft_min_rows = 0;
SET work_mem = '4MB';
SET effective_cache_size = '4GB';
SET default_statistics_target = 10000;
SET TIME ZONE 'UTC';

-- Freeze the exact non-collatable btree ordering identities admitted by the
-- production lowering. TEXT/VARCHAR and FLOAT remain closed in this phase.
SELECT opc.opcname,
       opc.opcintype::regtype AS input_type,
       amop.amopstrategy,
       amop.amopopr
FROM pg_opclass opc
JOIN pg_am am ON am.oid = opc.opcmethod
JOIN pg_amop amop
  ON amop.amopfamily = opc.opcfamily
 AND amop.amoplefttype = opc.opcintype
 AND amop.amoprighttype = opc.opcintype
WHERE am.amname = 'btree'
  AND opc.opcname IN (
      'bool_ops', 'int2_ops', 'int4_ops', 'int8_ops',
      'date_ops', 'timestamp_ops', 'timestamptz_ops')
  AND amop.amopstrategy IN (1, 5)
ORDER BY opc.opcintype, opc.opcname, amop.amopstrategy;

CREATE TABLE daft_sort_input(
    id int4 NOT NULL,
    b bool,
    i2 int2,
    i4 int4,
    i8 int8,
    d date,
    ts timestamp,
    tstz timestamptz,
    rejected_text text,
    rejected_varchar varchar(16),
    rejected_float float8
);
INSERT INTO daft_sort_input
SELECT i,
       CASE WHEN i % 11 = 0 THEN NULL ELSE i % 2 = 0 END,
       CASE WHEN i % 7 = 0 THEN NULL ELSE (i % 13)::int2 END,
       CASE WHEN i % 5 = 0 THEN NULL ELSE i % 17 END,
       CASE WHEN i % 3 = 0 THEN NULL ELSE (i % 19)::int8 END,
       CASE WHEN i % 13 = 0 THEN NULL ELSE DATE '2000-01-01' + (i % 23) END,
       CASE WHEN i % 17 = 0 THEN NULL
            ELSE TIMESTAMP '2000-01-01 00:00:00' + (i % 29) * INTERVAL '1 hour' END,
       CASE WHEN i % 19 = 0 THEN NULL
            ELSE TIMESTAMPTZ '2000-01-01 00:00:00+00' + (i % 31) * INTERVAL '1 hour' END,
       'text-' || (i % 37),
       ('varchar-' || (i % 41))::varchar(16),
       (i % 43)::float8
FROM generate_series(1, 12000) AS series(i);
ANALYZE daft_sort_input;

-- Keep a non-zero estimate but present no Arrow batches at execution time.
CREATE TABLE daft_sort_empty (LIKE daft_sort_input);
INSERT INTO daft_sort_empty SELECT * FROM daft_sort_input LIMIT 1000;
ANALYZE daft_sort_empty;
DELETE FROM daft_sort_empty;

CREATE FUNCTION daft_sort_plan_shape(query_text text)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    plan_line text;
    plan_text text := '';
BEGIN
    FOR plan_line IN EXECUTE 'EXPLAIN (COSTS OFF) ' || query_text LOOP
        plan_text := plan_text || plan_line || E'\n';
    END LOOP;
    RETURN CASE
        WHEN position('Custom Scan (DaftExec)' IN plan_text) > 0
         AND position('TopN' IN plan_text) > 0
            THEN 'daft/top_n'
        WHEN position('Custom Scan (DaftExec)' IN plan_text) > 0
         AND position('Sort' IN plan_text) > 0
            THEN 'daft/sort'
        WHEN position('Custom Scan (DaftExec)' IN plan_text) > 0
         AND position('Daft Operators: Limit' IN plan_text) > 0
            THEN 'daft/limit'
        WHEN position('Custom Scan (DaftExec)' IN plan_text) > 0
            THEN 'daft/other'
        ELSE 'pg'
    END;
END
$$;

-- Dynamic SQL forces a fresh plan after each engine-mode change. The Sort
-- capture assigns a semantic row position, so order mismatches are observable.
CREATE FUNCTION daft_sort_capture(query_text text)
RETURNS TABLE(pos bigint, id int4)
LANGUAGE plpgsql
AS $$
DECLARE
    result_row record;
    sequence_no bigint := 0;
BEGIN
    FOR result_row IN EXECUTE query_text LOOP
        sequence_no := sequence_no + 1;
        pos := sequence_no;
        id := result_row.id;
        RETURN NEXT;
    END LOOP;
END
$$;

CREATE FUNCTION daft_topn_capture(query_text text)
RETURNS TABLE(id int4)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY EXECUTE query_text;
END
$$;

SET pg_orca.daft_enable = 'force';
SET pg_orca.daft_prefer_plan = off;
SET pg_orca.daft_trace = on;

-- Direction/NULL combinations, a mixed key over every admitted type, direct
-- TopN, and ordinary unordered Limit are distinct selected plan shapes.
SELECT daft_sort_plan_shape(
    'SELECT id FROM daft_sort_input ORDER BY i4 ASC NULLS FIRST, id ASC');
SELECT daft_sort_plan_shape(
    'SELECT id FROM daft_sort_input ORDER BY i4 ASC NULLS LAST, id ASC');
SELECT daft_sort_plan_shape(
    'SELECT id FROM daft_sort_input ORDER BY i4 DESC NULLS FIRST, id ASC');
SELECT daft_sort_plan_shape(
    'SELECT id FROM daft_sort_input ORDER BY i4 DESC NULLS LAST, id ASC');
SELECT daft_sort_plan_shape(
    'SELECT id FROM daft_sort_input ORDER BY b DESC NULLS FIRST, i2 ASC NULLS LAST, i4 DESC NULLS LAST, i8 ASC NULLS FIRST, d DESC NULLS LAST, ts ASC NULLS FIRST, tstz DESC NULLS FIRST, id ASC');
SELECT daft_sort_plan_shape(
    'SELECT id FROM daft_sort_input ORDER BY i4 ASC NULLS LAST, id ASC LIMIT 7 OFFSET 5');
SELECT daft_sort_plan_shape('SELECT id FROM daft_sort_input LIMIT 7');
SELECT daft_sort_plan_shape(
    'SELECT id FROM daft_sort_empty ORDER BY i4, id');
SELECT daft_sort_plan_shape(
    'SELECT id FROM daft_sort_empty ORDER BY i4, id LIMIT 3 OFFSET 2');

-- Unsupported ordering identities must never produce a Daft fragment.
SELECT daft_sort_plan_shape(
    'SELECT id FROM daft_sort_input ORDER BY rejected_text, id');
SELECT daft_sort_plan_shape(
    'SELECT id FROM daft_sort_input ORDER BY rejected_varchar, id');
SELECT daft_sort_plan_shape(
    'SELECT id FROM daft_sort_input ORDER BY rejected_text COLLATE "default", id');
SELECT daft_sort_plan_shape(
    'SELECT id FROM daft_sort_input ORDER BY rejected_float, id');
-- PostgreSQL reference for all NULL/direction combinations plus the mixed
-- all-type key. Duplicate leading keys are broken only by unique id.
SET pg_orca.daft_enable = 'off';
CREATE TEMP TABLE pg_sort_result(case_name text, pos bigint, id int4);
INSERT INTO pg_sort_result
SELECT 'asc_nulls_first', * FROM daft_sort_capture(
    'SELECT id FROM daft_sort_input ORDER BY i4 ASC NULLS FIRST, id ASC')
UNION ALL
SELECT 'asc_nulls_last', * FROM daft_sort_capture(
    'SELECT id FROM daft_sort_input ORDER BY i4 ASC NULLS LAST, id ASC')
UNION ALL
SELECT 'desc_nulls_first', * FROM daft_sort_capture(
    'SELECT id FROM daft_sort_input ORDER BY i4 DESC NULLS FIRST, id ASC')
UNION ALL
SELECT 'desc_nulls_last', * FROM daft_sort_capture(
    'SELECT id FROM daft_sort_input ORDER BY i4 DESC NULLS LAST, id ASC')
UNION ALL
SELECT 'mixed_all_types', * FROM daft_sort_capture(
    'SELECT id FROM daft_sort_input ORDER BY b DESC NULLS FIRST, i2 ASC NULLS LAST, i4 DESC NULLS LAST, i8 ASC NULLS FIRST, d DESC NULLS LAST, ts ASC NULLS FIRST, tstz DESC NULLS FIRST, id ASC');

SET pg_orca.daft_enable = 'force';
SET pg_orca.daft_prefer_plan = off;
CREATE TEMP TABLE daft_sort_result (LIKE pg_sort_result);
INSERT INTO daft_sort_result
SELECT 'asc_nulls_first', * FROM daft_sort_capture(
    'SELECT id FROM daft_sort_input ORDER BY i4 ASC NULLS FIRST, id ASC')
UNION ALL
SELECT 'asc_nulls_last', * FROM daft_sort_capture(
    'SELECT id FROM daft_sort_input ORDER BY i4 ASC NULLS LAST, id ASC')
UNION ALL
SELECT 'desc_nulls_first', * FROM daft_sort_capture(
    'SELECT id FROM daft_sort_input ORDER BY i4 DESC NULLS FIRST, id ASC')
UNION ALL
SELECT 'desc_nulls_last', * FROM daft_sort_capture(
    'SELECT id FROM daft_sort_input ORDER BY i4 DESC NULLS LAST, id ASC')
UNION ALL
SELECT 'mixed_all_types', * FROM daft_sort_capture(
    'SELECT id FROM daft_sort_input ORDER BY b DESC NULLS FIRST, i2 ASC NULLS LAST, i4 DESC NULLS LAST, i8 ASC NULLS FIRST, d DESC NULLS LAST, ts ASC NULLS FIRST, tstz DESC NULLS FIRST, id ASC');

SET pg_orca.daft_enable = 'off';
SELECT case_name, count(*) AS rows, count(DISTINCT id) AS distinct_ids
FROM daft_sort_result GROUP BY case_name ORDER BY case_name;
SELECT count(*) AS differing_rows
FROM (
    (SELECT * FROM pg_sort_result EXCEPT ALL SELECT * FROM daft_sort_result)
    UNION ALL
    (SELECT * FROM daft_sort_result EXCEPT ALL SELECT * FROM pg_sort_result)
) AS delta;

-- TopN exact-set differential plus count/offset boundaries.
CREATE TEMP TABLE pg_topn_result(case_name text, id int4);
INSERT INTO pg_topn_result
SELECT 'limit_offset', * FROM daft_topn_capture(
    'SELECT id FROM daft_sort_input ORDER BY i4 ASC NULLS LAST, id ASC LIMIT 7 OFFSET 5')
UNION ALL
SELECT 'limit_zero', * FROM daft_topn_capture(
    'SELECT id FROM daft_sort_input ORDER BY i4 ASC NULLS LAST, id ASC LIMIT 0')
UNION ALL
SELECT 'offset_past_end', * FROM daft_topn_capture(
    'SELECT id FROM daft_sort_input ORDER BY i4 ASC NULLS LAST, id ASC LIMIT 3 OFFSET 99999');

SET pg_orca.daft_enable = 'force';
SET pg_orca.daft_prefer_plan = off;
CREATE TEMP TABLE daft_topn_result (LIKE pg_topn_result);
INSERT INTO daft_topn_result
SELECT 'limit_offset', * FROM daft_topn_capture(
    'SELECT id FROM daft_sort_input ORDER BY i4 ASC NULLS LAST, id ASC LIMIT 7 OFFSET 5')
UNION ALL
SELECT 'limit_zero', * FROM daft_topn_capture(
    'SELECT id FROM daft_sort_input ORDER BY i4 ASC NULLS LAST, id ASC LIMIT 0')
UNION ALL
SELECT 'offset_past_end', * FROM daft_topn_capture(
    'SELECT id FROM daft_sort_input ORDER BY i4 ASC NULLS LAST, id ASC LIMIT 3 OFFSET 99999');
INSERT INTO daft_topn_result
SELECT 'empty_input', * FROM daft_topn_capture(
    'SELECT id FROM daft_sort_empty ORDER BY i4 ASC NULLS LAST, id ASC LIMIT 3 OFFSET 2');

SET pg_orca.daft_enable = 'off';
SELECT case_name, count(*) AS rows
FROM daft_topn_result GROUP BY case_name ORDER BY case_name;
SELECT count(*) AS differing_rows
FROM (
    (SELECT * FROM pg_topn_result EXCEPT ALL SELECT * FROM daft_topn_result)
    UNION ALL
    (SELECT * FROM daft_topn_result EXCEPT ALL SELECT * FROM pg_topn_result)
) AS delta;

-- The planner sees 12k analyzed rows (enough for the calibrated Daft winner),
-- while execution receives enough retained int8 data to cross half of the
-- 1 MB fragment budget.  PREPARE freezes the valid 512 MB profile; execution
-- then proves that the Bridge reads the current 1 MB runtime gate.
CREATE TABLE daft_sort_memory(v int8 NOT NULL) WITH (autovacuum_enabled = false);
INSERT INTO daft_sort_memory SELECT i FROM generate_series(1, 12000) AS series(i);
ANALYZE daft_sort_memory;
INSERT INTO daft_sort_memory
SELECT i FROM generate_series(12001, 120000) AS series(i);

SET pg_orca.daft_enable = 'force';
SET pg_orca.daft_prefer_plan = off;
SET pg_orca.daft_memory_limit_mb = 512;
PREPARE daft_sort_memory_plan AS
SELECT v FROM daft_sort_memory ORDER BY v DESC;
SELECT daft_sort_plan_shape('EXECUTE daft_sort_memory_plan');
SET pg_orca.daft_memory_limit_mb = 1;
EXECUTE daft_sort_memory_plan;
DEALLOCATE daft_sort_memory_plan;

SET pg_orca.daft_memory_limit_mb = 512;
PREPARE daft_topn_memory_plan AS
SELECT v FROM daft_sort_memory ORDER BY v ASC LIMIT 3 OFFSET 2;
SELECT daft_sort_plan_shape('EXECUTE daft_topn_memory_plan');
SET pg_orca.daft_memory_limit_mb = 1;
EXECUTE daft_topn_memory_plan;
DEALLOCATE daft_topn_memory_plan;

RESET pg_orca.daft_memory_limit_mb;
DROP FUNCTION daft_topn_capture(text);
DROP FUNCTION daft_sort_capture(text);
DROP FUNCTION daft_sort_plan_shape(text);
DROP TABLE daft_sort_memory, daft_sort_empty, daft_sort_input;
