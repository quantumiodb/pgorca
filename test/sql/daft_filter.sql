-- =====================================================================
-- daft_filter.sql  (P1-001-A)
-- Costed Daft Filter alternative, immutable predicate whitelist, SQL
-- three-valued logic, and fail-closed admission boundaries.
-- =====================================================================

SET pg_orca.enable_orca = on;
SET pg_orca.daft_enable = 'force';
SET pg_orca.daft_cost_calibration_profile = 'test_prefer_daft_v1';
SET pg_orca.daft_trace = on;
SET client_min_messages = warning;
SET work_mem = '4MB';
SET hash_mem_multiplier = 2.0;
SET effective_cache_size = '4GB';
SET pg_orca.cost_model = 'pg';
SET default_statistics_target = 10000;

CREATE TABLE daft_filter_t(
    id int,
    small_value smallint,
    big_value bigint,
    keep boolean,
    label text
);
INSERT INTO daft_filter_t
SELECT i,
       CASE WHEN i % 7 = 0 THEN NULL ELSE (i % 100)::smallint END,
       i::bigint * 10,
       CASE WHEN i % 3 = 0 THEN NULL ELSE i % 2 = 0 END,
       CASE WHEN i % 10 = 0 THEN 'x' ELSE 'y' END
FROM generate_series(1, 100000) AS g(i);
ANALYZE daft_filter_t;

SET pg_orca.daft_enable = 'off';
CREATE TABLE daft_filter_small AS
SELECT id FROM daft_filter_t WHERE id <= 10;
ANALYZE daft_filter_small;
SET pg_orca.daft_enable = 'force';

CREATE FUNCTION daft_filter_plan_uses_daft(query_text text)
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    plan_line text;
    plan_text text := '';
BEGIN
    FOR plan_line IN EXECUTE 'EXPLAIN (COSTS OFF) ' || query_text LOOP
        plan_text := plan_text || plan_line || E'\n';
    END LOOP;
    RETURN position('Custom Scan (DaftExec)' IN plan_text) > 0;
END
$$;

-- The same legal Filter has both PG and Daft physical implementations.  With
-- the unbiased complete cost, the boundary dominates for ten rows while the
-- Daft Filter wins for the 100000-row input.
SET pg_orca.daft_prefer_plan = off;
SELECT daft_filter_plan_uses_daft(
    'SELECT id FROM daft_filter_small WHERE id >= 1 AND id < 10');
SELECT daft_filter_plan_uses_daft(
    'SELECT id FROM daft_filter_t WHERE id >= 99990 AND id < 100000');

EXPLAIN (COSTS OFF)
SELECT id FROM daft_filter_t WHERE id >= 99990 AND id < 100000;

-- Compare the complete result sets produced by the PG winner and Daft winner.
SET pg_orca.daft_enable = 'off';
CREATE TEMP TABLE pg_filter_result AS
SELECT id, small_value, big_value, keep
FROM daft_filter_t
WHERE small_value > 50::smallint;

SET pg_orca.daft_enable = 'force';
SET pg_orca.daft_prefer_plan = on;
CREATE TEMP TABLE daft_filter_result AS
SELECT id, small_value, big_value, keep
FROM daft_filter_t
WHERE small_value > 50::smallint;

SELECT
    (SELECT count(*) FROM (
        (SELECT * FROM pg_filter_result EXCEPT ALL SELECT * FROM daft_filter_result)
        UNION ALL
        (SELECT * FROM daft_filter_result EXCEPT ALL SELECT * FROM pg_filter_result)
    ) AS delta) AS differing_rows;
SELECT (SELECT count(*) FROM pg_filter_result) AS pg_rows,
       (SELECT count(*) FROM daft_filter_result) AS daft_rows;

-- The nullable comparison above produces TRUE, FALSE and UNKNOWN in one Daft
-- execution; the zero-row delta proves WHERE retained only TRUE exactly as PG.
-- NOT and IS NULL are separately admitted physical alternatives, but CBO is
-- still free to keep the PG implementation when its full cost is lower.
SELECT daft_filter_plan_uses_daft(
    'SELECT id FROM daft_filter_t WHERE small_value > 50::smallint');
SELECT daft_filter_plan_uses_daft(
    'SELECT id FROM daft_filter_t WHERE NOT (small_value > 50::smallint)');
SELECT daft_filter_plan_uses_daft(
    'SELECT id FROM daft_filter_t WHERE small_value IS NULL');
SELECT count(*) FROM daft_filter_t WHERE small_value > 50::smallint;
SELECT count(*) FROM daft_filter_t WHERE NOT (small_value > 50::smallint);
SELECT count(*) FROM daft_filter_t WHERE small_value IS NULL;
SELECT count(*) FROM daft_filter_t WHERE TRUE;
SELECT count(*) FROM daft_filter_t WHERE FALSE;
SELECT count(*) FROM daft_filter_t WHERE NULL::boolean;
SET pg_orca.daft_prefer_plan = off;

-- Unsupported or evaluation-order-sensitive expressions remain PG plans.
SELECT daft_filter_plan_uses_daft(
    'SELECT id FROM daft_filter_t WHERE random() < 0.5');
SELECT daft_filter_plan_uses_daft(
    'SELECT id FROM daft_filter_t WHERE id = 0 AND 10 / id > 1');
SELECT daft_filter_plan_uses_daft(
    $$SELECT id FROM daft_filter_t WHERE label = 'x'$$);

CREATE VIEW daft_filter_barrier WITH (security_barrier = true) AS
SELECT * FROM daft_filter_t WHERE id > 0;
SELECT daft_filter_plan_uses_daft(
    'SELECT id FROM daft_filter_barrier WHERE id < 10');

DROP FUNCTION daft_filter_plan_uses_daft(text);
DROP VIEW daft_filter_barrier;
DROP TABLE daft_filter_small;
DROP TABLE daft_filter_t;
-- end of daft_filter
