-- =====================================================================
-- daft_rules.sql  (P1-004-C)
-- Rule-registry golden plans, semantic differential and cost-choice gate.
-- The canonical registry keeps every Daft logical optimizer rule BLOCKED;
-- the Bridge directly lowers the physical fragment already selected by ORCA.
-- =====================================================================

SET client_min_messages = warning;
SET pg_orca.enable_orca = on;
SET pg_orca.daft_enable = 'off';
SET pg_orca.daft_cost_calibration_profile = 'test_prefer_daft_v1';
SET pg_orca.cost_model = 'pg';
SET pg_orca.daft_min_cost = 0;
SET pg_orca.daft_min_rows = 0;
SET work_mem = '4MB';
SET hash_mem_multiplier = 2.0;
SET effective_cache_size = '4GB';
SET default_statistics_target = 10000;

CREATE TABLE daft_rule_probe(v int4);
INSERT INTO daft_rule_probe
SELECT (i % 101) - 50 FROM generate_series(1, 1000) AS series(i);

CREATE TABLE daft_rule_input(id int4 NOT NULL, k int4, v int4, v2 int2);
INSERT INTO daft_rule_input
SELECT i,
       CASE WHEN i % 29 = 0 THEN NULL ELSE i % 97 END,
       CASE WHEN i % 17 = 0 THEN NULL ELSE (i % 101) - 50 END,
       CASE WHEN i % 13 = 0 THEN NULL ELSE (i % 100)::int2 END
FROM generate_series(1, 12000) AS series(i);
ANALYZE daft_rule_probe;
ANALYZE daft_rule_input;

CREATE FUNCTION daft_rule_plan_shape(query_text text)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    plan_line text;
    plan_text text := '';
    operator_match text[];
BEGIN
    FOR plan_line IN EXECUTE 'EXPLAIN (COSTS OFF) ' || query_text LOOP
        plan_text := plan_text || plan_line || E'\n';
    END LOOP;
    IF position('Custom Scan (DaftExec)' IN plan_text) = 0 THEN
        RETURN 'pg';
    END IF;
    operator_match := regexp_match(plan_text, 'Daft Operators: ([^\n]+)');
    RETURN 'daft/' || COALESCE(operator_match[1], 'unknown');
END
$$;

-- Cost-choice golden: the same legal aggregate alternatives live in the ORCA
-- Memo.  The complete unbiased cost keeps the small input in PostgreSQL; the
-- explicit test-only preference changes ranking and selects Daft.
SET pg_orca.daft_enable = 'force';
SET pg_orca.daft_trace = on;
SET pg_orca.daft_prefer_plan = off;
SELECT daft_rule_plan_shape(
    'SELECT count(*), sum(v), min(v), max(v) FROM daft_rule_probe');
SET pg_orca.daft_prefer_plan = on;
SELECT daft_rule_plan_shape(
    'SELECT count(*), sum(v), min(v), max(v) FROM daft_rule_probe');

-- Representative physical-plan goldens for mapped rule families.  These are
-- ORCA-selected operators, not a second Daft logical optimization pass.
SELECT daft_rule_plan_shape(
    'SELECT id FROM daft_rule_input WHERE v2 > 90::smallint');
SELECT daft_rule_plan_shape(
    'SELECT id FROM daft_rule_input ORDER BY v ASC NULLS LAST, id LIMIT 17 OFFSET 5');

-- Semantic differential for representative Filter/Project, HashAggregate and
-- TopN/offset shapes.  EXCEPT ALL retains duplicate multiplicity.
SET pg_orca.daft_enable = 'off';
CREATE TEMP TABLE pg_rule_filter AS
SELECT id, k, v, v2 FROM daft_rule_input WHERE v2 > 50::smallint;
CREATE TEMP TABLE pg_rule_aggregate AS
SELECT count(*) AS n, sum(v) AS total, min(v) AS minimum, max(v) AS maximum
FROM daft_rule_probe;
CREATE TEMP TABLE pg_rule_topn AS
SELECT id, v FROM daft_rule_input
ORDER BY v ASC NULLS LAST, id LIMIT 17 OFFSET 5;

SET pg_orca.daft_enable = 'force';
SET pg_orca.daft_prefer_plan = on;
CREATE TEMP TABLE daft_rule_filter AS
SELECT id, k, v, v2 FROM daft_rule_input WHERE v2 > 50::smallint;
CREATE TEMP TABLE daft_rule_aggregate AS
SELECT count(*) AS n, sum(v) AS total, min(v) AS minimum, max(v) AS maximum
FROM daft_rule_probe;
CREATE TEMP TABLE daft_rule_topn AS
SELECT id, v FROM daft_rule_input
ORDER BY v ASC NULLS LAST, id LIMIT 17 OFFSET 5;

SET pg_orca.daft_enable = 'off';
SET pg_orca.daft_prefer_plan = off;
SET pg_orca.daft_trace = off;
SELECT 'filter_project' AS case_name,
       (SELECT count(*) FROM pg_rule_filter) AS pg_rows,
       (SELECT count(*) FROM daft_rule_filter) AS daft_rows,
       (SELECT count(*) FROM (
          (SELECT * FROM pg_rule_filter EXCEPT ALL SELECT * FROM daft_rule_filter)
          UNION ALL
          (SELECT * FROM daft_rule_filter EXCEPT ALL SELECT * FROM pg_rule_filter)
       ) AS delta) AS differing_rows;
SELECT 'hash_aggregate' AS case_name,
       (SELECT count(*) FROM pg_rule_aggregate) AS pg_rows,
       (SELECT count(*) FROM daft_rule_aggregate) AS daft_rows,
       (SELECT count(*) FROM (
          (SELECT * FROM pg_rule_aggregate EXCEPT ALL SELECT * FROM daft_rule_aggregate)
          UNION ALL
          (SELECT * FROM daft_rule_aggregate EXCEPT ALL SELECT * FROM pg_rule_aggregate)
       ) AS delta) AS differing_rows;
SELECT 'topn_offset' AS case_name,
       (SELECT count(*) FROM pg_rule_topn) AS pg_rows,
       (SELECT count(*) FROM daft_rule_topn) AS daft_rows,
       (SELECT count(*) FROM (
          (SELECT * FROM pg_rule_topn EXCEPT ALL SELECT * FROM daft_rule_topn)
          UNION ALL
          (SELECT * FROM daft_rule_topn EXCEPT ALL SELECT * FROM pg_rule_topn)
       ) AS delta) AS differing_rows;

DROP FUNCTION daft_rule_plan_shape(text);
DROP TABLE daft_rule_input, daft_rule_probe;
-- end of daft_rules
