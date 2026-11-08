-- =====================================================================
-- daft_explain.sql  (P0-009-D)
-- P0 EXPLAIN observability.  Auto mode has no production-calibrated cost
-- profile yet, so it must yield a normal PG/ORCA plan and a stable, redacted
-- daft_trace reason.  A synthetic force+trace profile makes the P0 candidate
-- eligible.  The explicit, default-off daft_prefer_plan policy then discounts
-- one complete Daft fragment once, making the real CBO-selected DaftExec path
-- deterministic for integration testing without claiming a naturally faster
-- calibrated plan.
-- =====================================================================

SET pg_orca.enable_orca = on;
SET client_min_messages = info;
SET pg_orca.daft_trace = on;

CREATE TABLE daft_ex_t(a int);
INSERT INTO daft_ex_t VALUES (1),(2),(3),(4),(5);

-- off: plain plan, must not contain a DaftExec Custom Scan.
SET pg_orca.daft_enable = 'off';
EXPLAIN (COSTS OFF) SELECT count(*) FROM daft_ex_t;

-- auto: no production-calibrated profile -> no DaftExec candidate.
SET pg_orca.daft_enable = 'auto';
EXPLAIN (COSTS OFF) SELECT count(*) FROM daft_ex_t;

-- The synthetic profile is test-only.  It makes Daft alternatives eligible
-- so daft_trace can expose actual canonical cost calls and the CBO winner;
-- it does not force a semantically/cost-wise inferior Daft plan to win.
SET pg_orca.daft_enable = 'force';
SET pg_orca.daft_cost_calibration_profile = 'test_prefer_daft_v1';
SET work_mem = '4MB';
SET hash_mem_multiplier = 2.0;
SET effective_cache_size = '4GB';
SET pg_orca.cost_model = 'pg';
SET pg_orca.daft_prefer_plan = off;
EXPLAIN (COSTS OFF) SELECT a FROM daft_ex_t LIMIT 2;

-- Regression for the execution-format enforcer lower-bound cycle found by
-- P0-010-B.  At this scale Daft boundary/Limit alternatives are costed, but
-- the truthful P0 winner remains PG.  Planning must terminate without a
-- CostLowerBound self-recursion or backend crash.
CREATE TABLE daft_ex_big(a bigint);
INSERT INTO daft_ex_big SELECT i FROM generate_series(1, 100000) i;
ANALYZE daft_ex_big;
EXPLAIN (COSTS OFF) SELECT a FROM daft_ex_big LIMIT 3000;

-- Explicit preference changes only ranking of already-legal alternatives.
-- The selected plan must be a real DaftExec, and CTAS must consume all rows
-- through the dynamically loaded Rust ABI.  Validate its persisted result
-- with ordinary PostgreSQL after returning the policy to its safe default.
SET pg_orca.daft_prefer_plan = on;
EXPLAIN (COSTS OFF) SELECT a FROM daft_ex_big LIMIT 3000;
CREATE TEMP TABLE daft_ex_result AS
SELECT a FROM daft_ex_big LIMIT 3000;
SET pg_orca.daft_prefer_plan = off;
SET pg_orca.daft_trace = off;
SELECT count(*) AS cnt, sum(a) AS total FROM daft_ex_result;
SELECT count(*) AS cnt FROM daft_ex_t;

-- The debug harness executes the unchanged P0 Project/Limit path, asserts
-- ABI 1.1 boundary reconciliation, and internally walks EXPLAIN ANALYZE to
-- require every P0/P1 field while rejecting payload/value/address classes.
-- Its stable return text deliberately remains the P0 contract.
CREATE OR REPLACE FUNCTION pg_daft_exec_selftest_v1(bigint, bigint, bigint, integer)
RETURNS text
AS '$libdir/pg_orca', 'pg_daft_exec_selftest_v1'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
\pset tuples_only on
SELECT pg_daft_exec_selftest_v1(5, 5, -1, 0);
\pset tuples_only off
-- end of daft_explain
