-- =====================================================================
-- daft_join.sql  (P1-001-D)
-- HashJoin differential matrix, explainable Memo winners, large output
-- batch splitting, and bounded multi-source execution.
-- =====================================================================

SET pg_orca.enable_orca = on;
SET pg_orca.daft_cost_calibration_profile = 'test_prefer_daft_v1';
SET client_min_messages = warning;
SET work_mem = '4MB';
SET hash_mem_multiplier = 2.0;
SET effective_cache_size = '4GB';
SET pg_orca.cost_model = 'pg';
SET default_statistics_target = 10000;

CREATE TABLE daft_join_l(lid int, k int, payload int);
CREATE TABLE daft_join_r(rid int, k int, payload int);
CREATE TABLE daft_join_tag(tid int, k int);
CREATE TABLE daft_join_empty(rid int, k int, payload int);
CREATE TABLE daft_join_small_l(lid int, k int);
CREATE TABLE daft_join_small_r(rid int, k int);

-- The large inputs contain NULL keys, unmatched rows and duplicate keys.  The
-- duplicate fanout deliberately produces output larger than one 65536-row
-- Arrow batch, exercising the Bridge's bounded output slicing.
INSERT INTO daft_join_l
SELECT i, CASE WHEN i % 17 = 0 THEN NULL ELSE i % 2000 END, i * 3
FROM generate_series(1, 30000) AS g(i);
INSERT INTO daft_join_r
SELECT i, CASE WHEN i % 19 = 0 THEN NULL ELSE i % 2000 END, i * 5
FROM generate_series(1, 8000) AS g(i);
INSERT INTO daft_join_tag
SELECT i, CASE WHEN i % 23 = 0 THEN NULL ELSE i % 2000 END
FROM generate_series(1, 4000) AS g(i);
INSERT INTO daft_join_small_l SELECT i, i FROM generate_series(1, 10) AS g(i);
INSERT INTO daft_join_small_r SELECT i, i FROM generate_series(1, 5) AS g(i);

ANALYZE daft_join_l;
ANALYZE daft_join_r;
ANALYZE daft_join_tag;
ANALYZE daft_join_empty;
ANALYZE daft_join_small_l;
ANALYZE daft_join_small_r;

CREATE FUNCTION daft_join_plan_shape(query_text text)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    plan_line text;
    plan_text text := '';
    engine text;
    sources text := '0';
BEGIN
    FOR plan_line IN EXECUTE 'EXPLAIN (COSTS OFF) ' || query_text LOOP
        plan_text := plan_text || plan_line || E'\n';
    END LOOP;
    engine := CASE WHEN position('Custom Scan (DaftExec)' IN plan_text) > 0
                   THEN 'daft' ELSE 'pg' END;
    IF position('PG Sources: 3' IN plan_text) > 0 THEN
        sources := '3';
    ELSIF position('PG Sources: 2' IN plan_text) > 0 THEN
        sources := '2';
    END IF;
    RETURN engine || '/sources=' || sources;
END
$$;

-- With one complete cost profile and no preference multiplier, cardinality
-- alone flips the engine winner.  The three-source case proves the chosen
-- Join tree is serialized as one bounded Daft fragment.
SET pg_orca.daft_enable = 'force';
SET pg_orca.daft_trace = on;
SET pg_orca.daft_prefer_plan = off;
SELECT daft_join_plan_shape(
    'SELECT l.lid, r.rid FROM daft_join_small_l l JOIN daft_join_small_r r ON l.k = r.k');
SELECT daft_join_plan_shape(
    'SELECT l.lid, r.rid, l.k FROM daft_join_l l JOIN daft_join_r r ON l.k = r.k');
SELECT daft_join_plan_shape(
    'SELECT l.lid, r.rid, l.k FROM daft_join_l l LEFT JOIN daft_join_r r ON l.k = r.k');
SELECT daft_join_plan_shape(
    'SELECT l.lid, r.rid, t.tid FROM daft_join_l l JOIN daft_join_r r ON l.k = r.k JOIN daft_join_tag t ON l.k = t.k');

-- Freeze the Join-specific fields exposed by ExplainDaftCustomScan.  Keep
-- COSTS OFF so the regression covers stable structure and redacted counters,
-- not calibrated cost estimates.  Inner and Left are both included because
-- they use different legal build-side sets even though SQL identifiers and
-- join keys are deliberately not copied into the Daft EXPLAIN properties.
EXPLAIN (COSTS OFF)
SELECT l.lid, r.rid, l.k
FROM daft_join_l l JOIN daft_join_r r ON l.k = r.k;
EXPLAIN (COSTS OFF)
SELECT l.lid, r.rid, l.k
FROM daft_join_l l LEFT JOIN daft_join_r r ON l.k = r.k;

-- Freeze a three-layer mixed-engine boundary. AVG remains in PG and float8
-- equality keeps the Join out of Daft admission.  The semantic subquery LIMIT
-- prevents the outer cross-input Filter from being folded into the Join, so
-- the admitted Daft Limit can sit between the PG Aggregate/Filter parents and
-- the PG Limit/HashJoin child tree.
SET pg_orca.daft_prefer_plan = on;
EXPLAIN (COSTS OFF)
SELECT avg(lid)
FROM (
    SELECT l.lid, r.rid
    FROM daft_join_l l JOIN daft_join_r r
      ON l.k::float8 = r.k::float8
    LIMIT 50000
) AS limited_join
WHERE lid > rid;
SET pg_orca.daft_prefer_plan = off;

-- These semantics are not advertised for Daft HashJoin and must remain PG.
SELECT daft_join_plan_shape(
    'SELECT l.lid, r.rid FROM daft_join_small_l l JOIN daft_join_small_r r ON l.k < r.k');
SELECT daft_join_plan_shape(
    'SELECT l.lid, r.rid FROM daft_join_small_l l JOIN daft_join_small_r r ON l.k IS NOT DISTINCT FROM r.k');
SELECT daft_join_plan_shape(
    'SELECT l.lid, r.rid FROM daft_join_small_l l JOIN daft_join_small_r r ON l.k::float8 = r.k::float8');
SELECT daft_join_plan_shape(
    'SELECT l.lid, r.rid FROM daft_join_small_l l FULL JOIN daft_join_small_r r ON l.k = r.k');

-- INNER: duplicates, NULL keys, unmatched rows, and output > 65536 rows.
SET pg_orca.daft_trace = off;
SET pg_orca.daft_enable = 'off';
CREATE TEMP TABLE pg_join_inner AS
SELECT l.lid, r.rid, l.k
FROM daft_join_l l JOIN daft_join_r r ON l.k = r.k;
SET pg_orca.daft_trace = on;
SET pg_orca.daft_enable = 'force';
SET pg_orca.daft_prefer_plan = on;
CREATE TEMP TABLE daft_join_inner AS
SELECT l.lid, r.rid, l.k
FROM daft_join_l l JOIN daft_join_r r ON l.k = r.k;
SET pg_orca.daft_enable = 'off';
SET pg_orca.daft_trace = off;
SELECT 'inner' AS case_name,
       (SELECT count(*) FROM pg_join_inner) AS pg_rows,
       (SELECT count(*) FROM daft_join_inner) AS daft_rows,
       (SELECT count(*) FROM (
          (SELECT * FROM pg_join_inner EXCEPT ALL SELECT * FROM daft_join_inner)
          UNION ALL
          (SELECT * FROM daft_join_inner EXCEPT ALL SELECT * FROM pg_join_inner)
       ) AS delta) AS differing_rows;
DROP TABLE pg_join_inner, daft_join_inner;

-- LEFT: unmatched and NULL-key left rows must be NULL-extended exactly once.
SET pg_orca.daft_enable = 'off';
CREATE TEMP TABLE pg_join_left AS
SELECT l.lid, r.rid, l.k
FROM daft_join_l l LEFT JOIN daft_join_r r ON l.k = r.k;
SET pg_orca.daft_trace = on;
SET pg_orca.daft_enable = 'force';
SET pg_orca.daft_prefer_plan = on;
CREATE TEMP TABLE daft_join_left AS
SELECT l.lid, r.rid, l.k
FROM daft_join_l l LEFT JOIN daft_join_r r ON l.k = r.k;
SET pg_orca.daft_enable = 'off';
SET pg_orca.daft_trace = off;
SELECT 'left' AS case_name,
       (SELECT count(*) FROM pg_join_left) AS pg_rows,
       (SELECT count(*) FROM daft_join_left) AS daft_rows,
       (SELECT count(*) FROM (
          (SELECT * FROM pg_join_left EXCEPT ALL SELECT * FROM daft_join_left)
          UNION ALL
          (SELECT * FROM daft_join_left EXCEPT ALL SELECT * FROM pg_join_left)
       ) AS delta) AS differing_rows;
DROP TABLE pg_join_left, daft_join_left;

-- A PG post-join filter above DaftExec must observe LEFT JOIN null extension.
SET pg_orca.daft_enable = 'off';
CREATE TEMP TABLE pg_join_post_filter AS
SELECT l.lid, r.rid, l.k
FROM daft_join_l l LEFT JOIN daft_join_r r ON l.k = r.k
WHERE r.rid IS NULL OR l.lid > 100;
SET pg_orca.daft_trace = on;
SET pg_orca.daft_enable = 'force';
SET pg_orca.daft_prefer_plan = on;
CREATE TEMP TABLE daft_join_post_filter AS
SELECT l.lid, r.rid, l.k
FROM daft_join_l l LEFT JOIN daft_join_r r ON l.k = r.k
WHERE r.rid IS NULL OR l.lid > 100;
SET pg_orca.daft_enable = 'off';
SET pg_orca.daft_trace = off;
SELECT 'left_post_filter' AS case_name,
       (SELECT count(*) FROM pg_join_post_filter) AS pg_rows,
       (SELECT count(*) FROM daft_join_post_filter) AS daft_rows,
       (SELECT count(*) FROM (
          (SELECT * FROM pg_join_post_filter EXCEPT ALL SELECT * FROM daft_join_post_filter)
          UNION ALL
          (SELECT * FROM daft_join_post_filter EXCEPT ALL SELECT * FROM pg_join_post_filter)
       ) AS delta) AS differing_rows;
DROP TABLE pg_join_post_filter, daft_join_post_filter;

-- Empty build/probe behavior for both advertised Join types.
SET pg_orca.daft_enable = 'off';
CREATE TEMP TABLE pg_join_empty_inner AS
SELECT l.lid, e.rid, l.k
FROM daft_join_l l JOIN daft_join_empty e ON l.k = e.k;
CREATE TEMP TABLE pg_join_empty_left AS
SELECT l.lid, e.rid, l.k
FROM daft_join_l l LEFT JOIN daft_join_empty e ON l.k = e.k;
SET pg_orca.daft_trace = on;
SET pg_orca.daft_enable = 'force';
SET pg_orca.daft_prefer_plan = on;
CREATE TEMP TABLE daft_join_empty_inner AS
SELECT l.lid, e.rid, l.k
FROM daft_join_l l JOIN daft_join_empty e ON l.k = e.k;
CREATE TEMP TABLE daft_join_empty_left AS
SELECT l.lid, e.rid, l.k
FROM daft_join_l l LEFT JOIN daft_join_empty e ON l.k = e.k;
SET pg_orca.daft_enable = 'off';
SET pg_orca.daft_trace = off;
SELECT 'empty_inner' AS case_name,
       (SELECT count(*) FROM pg_join_empty_inner) AS pg_rows,
       (SELECT count(*) FROM daft_join_empty_inner) AS daft_rows,
       (SELECT count(*) FROM (
          (SELECT * FROM pg_join_empty_inner EXCEPT ALL SELECT * FROM daft_join_empty_inner)
          UNION ALL
          (SELECT * FROM daft_join_empty_inner EXCEPT ALL SELECT * FROM pg_join_empty_inner)
       ) AS delta) AS differing_rows;
SELECT 'empty_left' AS case_name,
       (SELECT count(*) FROM pg_join_empty_left) AS pg_rows,
       (SELECT count(*) FROM daft_join_empty_left) AS daft_rows,
       (SELECT count(*) FROM (
          (SELECT * FROM pg_join_empty_left EXCEPT ALL SELECT * FROM daft_join_empty_left)
          UNION ALL
          (SELECT * FROM daft_join_empty_left EXCEPT ALL SELECT * FROM pg_join_empty_left)
       ) AS delta) AS differing_rows;
DROP TABLE pg_join_empty_inner, daft_join_empty_inner,
           pg_join_empty_left, daft_join_empty_left;

-- Three PG-owned sources, duplicate fanout and multiple output batches.
SET pg_orca.daft_enable = 'off';
CREATE TEMP TABLE pg_join_three AS
SELECT l.lid, r.rid, t.tid, l.k
FROM daft_join_l l
JOIN daft_join_r r ON l.k = r.k
JOIN daft_join_tag t ON l.k = t.k;
SET pg_orca.daft_trace = on;
SET pg_orca.daft_enable = 'force';
SET pg_orca.daft_prefer_plan = on;
CREATE TEMP TABLE daft_join_three AS
SELECT l.lid, r.rid, t.tid, l.k
FROM daft_join_l l
JOIN daft_join_r r ON l.k = r.k
JOIN daft_join_tag t ON l.k = t.k;
SET pg_orca.daft_enable = 'off';
SET pg_orca.daft_trace = off;
SELECT 'three_source' AS case_name,
       (SELECT count(*) FROM pg_join_three) AS pg_rows,
       (SELECT count(*) FROM daft_join_three) AS daft_rows,
       (SELECT count(*) FROM (
          (SELECT * FROM pg_join_three EXCEPT ALL SELECT * FROM daft_join_three)
          UNION ALL
          (SELECT * FROM daft_join_three EXCEPT ALL SELECT * FROM pg_join_three)
       ) AS delta) AS differing_rows;
DROP TABLE pg_join_three, daft_join_three;

DROP FUNCTION daft_join_plan_shape(text);
DROP TABLE daft_join_small_r, daft_join_small_l, daft_join_empty,
           daft_join_tag, daft_join_r, daft_join_l;
-- end of daft_join
