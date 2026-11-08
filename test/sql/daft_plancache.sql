-- =====================================================================
-- daft_plancache.sql  (P0-008-F)
-- Policy: a successfully loaded capability registry/Bridge is immutable for
-- the backend lifetime.  Registry/rule/ABI upgrades require old backends to
-- terminate; cost-only changes require reconnect or DISCARD PLANS only to
-- regain optimality and are not execution-compatibility gates.  A digest is
-- an execution stamp, not PostgreSQL automatic invalidation metadata.
--
-- The debug-only stale-plan harness enters the real DaftExec Begin path with a
-- structurally valid payload whose frozen capability digest is obsolete.  PG
-- preflight must reject it before Bridge open and before the PG child is
-- pulled, for both custom and generic prepared plans.
-- =====================================================================

SET pg_orca.enable_orca = on;
SET pg_orca.daft_enable = 'force';

CREATE OR REPLACE FUNCTION pg_daft_exec_stale_plan_selftest_v1(bigint)
RETURNS void
AS '$libdir/pg_orca', 'pg_daft_exec_stale_plan_selftest_v1'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
CREATE OR REPLACE FUNCTION pg_daft_test_source_pulls_v1()
RETURNS bigint
AS '$libdir/pg_orca', 'pg_daft_test_source_pulls_v1'
LANGUAGE C STABLE PARALLEL UNSAFE;
CREATE OR REPLACE FUNCTION pg_daft_bridge_open_attempts_v1()
RETURNS bigint
AS '$libdir/pg_orca', 'pg_daft_bridge_open_attempts_v1'
LANGUAGE C STABLE PARALLEL UNSAFE;
CREATE OR REPLACE FUNCTION pg_daft_exec_selftest_v1(bigint, bigint, bigint, integer)
RETURNS text
AS '$libdir/pg_orca', 'pg_daft_exec_selftest_v1'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

PREPARE daft_pc_stale(bigint) AS
SELECT pg_daft_exec_stale_plan_selftest_v1($1);
\set VERBOSITY terse
SET plan_cache_mode = force_custom_plan;
EXECUTE daft_pc_stale(1);
SELECT pg_daft_bridge_open_attempts_v1() AS custom_bridge_opens,
       pg_daft_test_source_pulls_v1() AS custom_child_pulls;
SET plan_cache_mode = force_generic_plan;
EXECUTE daft_pc_stale(2);
SELECT pg_daft_bridge_open_attempts_v1() AS generic_bridge_opens,
       pg_daft_test_source_pulls_v1() AS generic_child_pulls;
SELECT generic_plans, custom_plans
FROM pg_prepared_statements WHERE name = 'daft_pc_stale';
\set VERBOSITY default
DEALLOCATE daft_pc_stale;

-- Rejection does not poison the backend or its immutable Bridge loader state;
-- both prepared-plan modes retain healthy Daft execution capability.
SET pg_orca.daft_batch_rows = 2;
PREPARE daft_pc_healthy(bigint) AS
SELECT pg_daft_exec_selftest_v1($1, 3, -1, 0);
\pset tuples_only on
SET plan_cache_mode = force_custom_plan;
EXECUTE daft_pc_healthy(3);
SET plan_cache_mode = force_generic_plan;
EXECUTE daft_pc_healthy(3);
\pset tuples_only off
SELECT generic_plans, custom_plans
FROM pg_prepared_statements WHERE name = 'daft_pc_healthy';
DEALLOCATE daft_pc_healthy;
RESET plan_cache_mode;

CREATE TABLE daft_pc_t(id int primary key, x int);
INSERT INTO daft_pc_t
SELECT i, i * 10 FROM generate_series(1, 100000) AS g(i);
ANALYZE daft_pc_t;

-- A no-parameter PREPARE is a real PostgreSQL generic cached plan.  Prove it
-- retains a selected DaftExec rather than permanently losing the capability.
SET pg_orca.daft_cost_calibration_profile = 'test_prefer_daft_v1';
RESET pg_orca.daft_batch_rows;
SET work_mem = '4MB';
SET hash_mem_multiplier = 2.0;
SET effective_cache_size = '4GB';
SET pg_orca.cost_model = 'pg';
SET pg_orca.daft_prefer_plan = on;
SET pg_orca.daft_trace = on;
PREPARE daft_pc_real_generic AS
SELECT id FROM daft_pc_t LIMIT 3000;
EXPLAIN (COSTS OFF) EXECUTE daft_pc_real_generic;
EXPLAIN (COSTS OFF) EXECUTE daft_pc_real_generic;
-- A cost-policy change is not an execution-compatibility invalidation: the
-- already cached Daft plan remains selected until an explicit DISCARD.
SET pg_orca.daft_prefer_plan = off;
EXPLAIN (COSTS OFF) EXECUTE daft_pc_real_generic;
DISCARD PLANS;
EXPLAIN (COSTS OFF) EXECUTE daft_pc_real_generic;
SELECT generic_plans, custom_plans
FROM pg_prepared_statements WHERE name = 'daft_pc_real_generic';
DEALLOCATE daft_pc_real_generic;
SET pg_orca.daft_trace = off;

PREPARE daft_pc_p(int) AS SELECT x FROM daft_pc_t WHERE id = $1;
EXECUTE daft_pc_p(2);
EXECUTE daft_pc_p(3);
DEALLOCATE daft_pc_p;
-- end of daft_plancache
