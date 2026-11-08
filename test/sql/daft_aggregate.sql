-- =====================================================================
-- daft_aggregate.sql  (P1-002-C)
-- Real PG/Daft Aggregate plan selection and semantic differential matrix.
-- =====================================================================

SET pg_orca.enable_orca = on;
SET pg_orca.daft_enable = 'off';
SET pg_orca.daft_cost_calibration_profile = 'test_prefer_daft_v1';
SET client_min_messages = warning;
SET pg_orca.cost_model = 'pg';
SET work_mem = '4MB';
SET hash_mem_multiplier = 2.0;
SET effective_cache_size = '4GB';
SET pg_orca.daft_min_cost = 0;
SET pg_orca.daft_min_rows = 0;
SET default_statistics_target = 10000;

CREATE TABLE daft_agg_input(g int4, v2 int2, v4 int4);
INSERT INTO daft_agg_input VALUES
    (1,    1,    10),
    (1,    1,    10),
    (1,    2,  NULL),
    (1, NULL,    -5),
    (NULL, 3,    20),
    (NULL, NULL, NULL);
INSERT INTO daft_agg_input
SELECT CASE WHEN i % 5 = 0 THEN NULL ELSE i % 7 END,
       CASE WHEN i % 4 = 0 THEN NULL ELSE (i % 11)::int2 END,
       CASE WHEN i % 3 = 0 THEN NULL ELSE (i % 101) - 50 END
FROM generate_series(1, 12000) AS series(i);
CREATE TABLE daft_agg_empty(g int4, v2 int2, v4 int4);
-- Preserve a large planner estimate while presenting zero rows at execution.
-- This proves the selected Daft fragment handles a PG source that sends no
-- Arrow batches; a genuinely zero-cost relation would correctly remain PG.
INSERT INTO daft_agg_empty
SELECT i % 7, (i % 11)::int2, (i % 101) - 50
FROM generate_series(1, 12000) AS series(i);
CREATE TABLE daft_agg_all_null(g int4, v2 int2, v4 int4);
INSERT INTO daft_agg_all_null
SELECT 7, NULL::int2, NULL::int4
FROM generate_series(1, 12000);
CREATE TABLE daft_agg_plan_probe(v4 int4);
INSERT INTO daft_agg_plan_probe
SELECT (i % 101) - 50 FROM generate_series(1, 1000) AS series(i);
ANALYZE daft_agg_input;
ANALYZE daft_agg_empty;
ANALYZE daft_agg_all_null;
ANALYZE daft_agg_plan_probe;
DELETE FROM daft_agg_empty;

-- Freeze the PG19 aggregate identities consumed by the ORCA admission gate.
SELECT p.oid,
       p.proname,
       pg_get_function_identity_arguments(p.oid) AS args,
       a.aggtransfn::regproc AS transition,
       a.aggcombinefn::regproc AS combine,
       p.prorettype::regtype AS output_type
FROM pg_aggregate a
JOIN pg_proc p ON p.oid = a.aggfnoid
WHERE p.oid IN (2803, 2147, 2108, 2109, 2116, 2117, 2132, 2133)
ORDER BY p.oid;

-- COUNT outputs int8/non-NULL; SUM(int2/int4) outputs nullable int8;
-- MIN/MAX preserve int2 or int4 and are nullable.
SELECT pg_typeof(count(*)) AS count_star_type,
       pg_typeof(count(v4)) AS count_value_type,
       pg_typeof(sum(v2)) AS sum_int2_type,
       pg_typeof(sum(v4)) AS sum_int4_type,
       pg_typeof(min(v2)) AS min_int2_type,
       pg_typeof(min(v4)) AS min_int4_type,
       pg_typeof(max(v2)) AS max_int2_type,
       pg_typeof(max(v4)) AS max_int4_type
FROM daft_agg_input;

CREATE FUNCTION daft_aggregate_plan_shape(query_text text)
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
         AND position('Daft Operators: HashAggregate' IN plan_text) > 0
            THEN 'daft/hash_aggregate'
        WHEN position('Custom Scan (DaftExec)' IN plan_text) > 0
            THEN 'daft/other'
        ELSE 'pg'
    END;
END
$$;

-- The same Memo contains PG and Daft Aggregate alternatives. With the normal
-- multiplier the PG implementation remains cheaper; the test-only preference multiplier
-- makes the legal Daft alternative observable without bypassing CBO.
SET pg_orca.daft_enable = 'force';
SET pg_orca.daft_trace = on;
SET pg_orca.daft_prefer_plan = off;
SELECT daft_aggregate_plan_shape(
    'SELECT count(*), sum(v4), min(v4), max(v4) FROM daft_agg_plan_probe');
SET pg_orca.daft_prefer_plan = on;
SELECT daft_aggregate_plan_shape(
    'SELECT count(*), sum(v4), min(v4), max(v4) FROM daft_agg_plan_probe');
SELECT daft_aggregate_plan_shape(
    'SELECT g, count(*), sum(v4) FROM daft_agg_input GROUP BY g');
SELECT daft_aggregate_plan_shape(
    'SELECT count(*), count(v4), sum(v4), min(v4), max(v4) FROM daft_agg_empty');
SELECT daft_aggregate_plan_shape(
    'SELECT count(*), count(v4), sum(v4), min(v4), max(v4) FROM daft_agg_all_null');
SELECT daft_aggregate_plan_shape(
    'SELECT g, count(*) FROM daft_agg_empty GROUP BY g');

-- Unsupported modifiers and aggregate identities never produce a Daft
-- HashAggregate candidate.  Their stable PGDAFT_AGG_* rejection codes are
-- tested at the IR/Bridge boundary.  Independently admitted descendants may
-- still use DaftExec below the PostgreSQL-owned Aggregate boundary.
SELECT daft_aggregate_plan_shape(
    'SELECT count(DISTINCT v4) FROM daft_agg_input');
-- Freeze a non-trivial mixed-engine tree: DISTINCT remains a PostgreSQL
-- Aggregate chain, while its required input ordering is supplied by an
-- admitted Daft Sort fragment which must not claim HashAggregate ownership.
EXPLAIN (COSTS OFF)
SELECT count(DISTINCT v4) FROM daft_agg_input;
SELECT daft_aggregate_plan_shape(
    'SELECT avg(v4) FROM daft_agg_input');
SET pg_orca.daft_trace = off;

-- Freeze PG results first, then execute exactly the same admitted tasks through
-- a selected Daft fragment.  EXCEPT ALL preserves duplicate multiplicity.
SET pg_orca.daft_enable = 'off';
CREATE TEMP TABLE pg_agg_global AS
SELECT count(*) AS count_star, count(v4) AS count_value,
       sum(v2) AS sum_int2, sum(v4) AS sum_int4,
       min(v2) AS min_int2, min(v4) AS min_int4,
       max(v2) AS max_int2, max(v4) AS max_int4
FROM daft_agg_input;
CREATE TEMP TABLE pg_agg_empty AS
SELECT count(*) AS count_star, count(v4) AS count_value,
       sum(v2) AS sum_int2, sum(v4) AS sum_int4,
       min(v2) AS min_int2, min(v4) AS min_int4,
       max(v2) AS max_int2, max(v4) AS max_int4
FROM daft_agg_empty;
CREATE TEMP TABLE pg_agg_all_null AS
SELECT count(*) AS count_star, count(v4) AS count_value,
       sum(v2) AS sum_int2, sum(v4) AS sum_int4,
       min(v2) AS min_int2, min(v4) AS min_int4,
       max(v2) AS max_int2, max(v4) AS max_int4
FROM daft_agg_all_null;
CREATE TEMP TABLE pg_agg_grouped AS
SELECT g, count(*) AS count_star, count(v4) AS count_value,
       sum(v2) AS sum_int2, sum(v4) AS sum_int4,
       min(v2) AS min_int2, min(v4) AS min_int4,
       max(v2) AS max_int2, max(v4) AS max_int4
FROM daft_agg_input GROUP BY g;
CREATE TEMP TABLE pg_agg_grouped_empty AS
SELECT g, count(*) AS count_star
FROM daft_agg_empty GROUP BY g;

SET pg_orca.daft_enable = 'force';
SET pg_orca.daft_prefer_plan = on;
SET pg_orca.daft_trace = on;
CREATE TEMP TABLE daft_agg_global_result AS
SELECT count(*) AS count_star, count(v4) AS count_value,
       sum(v2) AS sum_int2, sum(v4) AS sum_int4,
       min(v2) AS min_int2, min(v4) AS min_int4,
       max(v2) AS max_int2, max(v4) AS max_int4
FROM daft_agg_input;
CREATE TEMP TABLE daft_agg_empty_result AS
SELECT count(*) AS count_star, count(v4) AS count_value,
       sum(v2) AS sum_int2, sum(v4) AS sum_int4,
       min(v2) AS min_int2, min(v4) AS min_int4,
       max(v2) AS max_int2, max(v4) AS max_int4
FROM daft_agg_empty;
CREATE TEMP TABLE daft_agg_all_null_result AS
SELECT count(*) AS count_star, count(v4) AS count_value,
       sum(v2) AS sum_int2, sum(v4) AS sum_int4,
       min(v2) AS min_int2, min(v4) AS min_int4,
       max(v2) AS max_int2, max(v4) AS max_int4
FROM daft_agg_all_null;
CREATE TEMP TABLE daft_agg_grouped_result AS
SELECT g, count(*) AS count_star, count(v4) AS count_value,
       sum(v2) AS sum_int2, sum(v4) AS sum_int4,
       min(v2) AS min_int2, min(v4) AS min_int4,
       max(v2) AS max_int2, max(v4) AS max_int4
FROM daft_agg_input GROUP BY g;
CREATE TEMP TABLE daft_agg_grouped_empty_result AS
SELECT g, count(*) AS count_star
FROM daft_agg_empty GROUP BY g;

SET pg_orca.daft_enable = 'off';
SET pg_orca.daft_trace = off;
SELECT 'global' AS case_name,
       (SELECT count(*) FROM pg_agg_global) AS pg_rows,
       (SELECT count(*) FROM daft_agg_global_result) AS daft_rows,
       (SELECT count(*) FROM (
          (SELECT * FROM pg_agg_global EXCEPT ALL SELECT * FROM daft_agg_global_result)
          UNION ALL
          (SELECT * FROM daft_agg_global_result EXCEPT ALL SELECT * FROM pg_agg_global)
       ) delta) AS differing_rows;
SELECT 'empty_global' AS case_name,
       (SELECT count(*) FROM pg_agg_empty) AS pg_rows,
       (SELECT count(*) FROM daft_agg_empty_result) AS daft_rows,
       (SELECT count(*) FROM (
          (SELECT * FROM pg_agg_empty EXCEPT ALL SELECT * FROM daft_agg_empty_result)
          UNION ALL
          (SELECT * FROM daft_agg_empty_result EXCEPT ALL SELECT * FROM pg_agg_empty)
       ) delta) AS differing_rows;
SELECT 'all_null' AS case_name,
       (SELECT count(*) FROM pg_agg_all_null) AS pg_rows,
       (SELECT count(*) FROM daft_agg_all_null_result) AS daft_rows,
       (SELECT count(*) FROM (
          (SELECT * FROM pg_agg_all_null EXCEPT ALL SELECT * FROM daft_agg_all_null_result)
          UNION ALL
          (SELECT * FROM daft_agg_all_null_result EXCEPT ALL SELECT * FROM pg_agg_all_null)
       ) delta) AS differing_rows;
SELECT 'group_null_duplicates' AS case_name,
       (SELECT count(*) FROM pg_agg_grouped) AS pg_rows,
       (SELECT count(*) FROM daft_agg_grouped_result) AS daft_rows,
       (SELECT count(*) FROM (
          (SELECT * FROM pg_agg_grouped EXCEPT ALL SELECT * FROM daft_agg_grouped_result)
          UNION ALL
          (SELECT * FROM daft_agg_grouped_result EXCEPT ALL SELECT * FROM pg_agg_grouped)
       ) delta) AS differing_rows;
SELECT 'grouped_empty' AS case_name,
       (SELECT count(*) FROM pg_agg_grouped_empty) AS pg_rows,
       (SELECT count(*) FROM daft_agg_grouped_empty_result) AS daft_rows,
       (SELECT count(*) FROM (
          (SELECT * FROM pg_agg_grouped_empty EXCEPT ALL SELECT * FROM daft_agg_grouped_empty_result)
          UNION ALL
          (SELECT * FROM daft_agg_grouped_empty_result EXCEPT ALL SELECT * FROM pg_agg_grouped_empty)
       ) delta) AS differing_rows;

-- sum(int2/int4) combines int8 states through int8pl. The Bridge lowering
-- must report an error at this boundary; wrapping or returning NULL is not an
-- acceptable substitute. P1-002-C adds the Daft-path error differential.
SELECT int8pl(9223372036854775807, 1);

DROP FUNCTION daft_aggregate_plan_shape(text);
DROP TABLE daft_agg_plan_probe, daft_agg_all_null, daft_agg_empty, daft_agg_input;
