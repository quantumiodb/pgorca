-- =====================================================================
-- daft_planstate.sql  (P0-009-D/P2-001-D)
-- PostgreSQL PlanState integration for a real DaftExec boundary.  The
-- debug-only harness initializes the CustomScan child through custom_ps,
-- walks it with PostgreSQL's planstate_tree_walker and EXPLAIN machinery,
-- and verifies that ExecEndNode reaches the child exactly once.  A failing
-- child followed by a healthy execution in the same backend proves that
-- error cleanup also releases the active-fragment guard.
-- =====================================================================

SET pg_orca.enable_orca = on;
CREATE OR REPLACE FUNCTION pg_daft_exec_selftest_v1(bigint, bigint, bigint, integer)
RETURNS text
AS '$libdir/pg_orca', 'pg_daft_exec_selftest_v1'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
SET pg_orca.daft_enable = 'force';
SET pg_orca.daft_batch_rows = 2;

\pset tuples_only on
-- Stable return fields are assertions produced after the real PG walker,
-- EXPLAIN and child End callbacks have all completed successfully.
SELECT pg_daft_exec_selftest_v1(5, 5, -1, 0);

-- Error after two child pulls.  PostgreSQL must unwind both CustomScan
-- states and release the bridge/fragment guard before the next statement.
SELECT pg_daft_exec_selftest_v1(10, 10, 2, 0);
SELECT pg_daft_exec_selftest_v1(3, 3, -1, 0);
\pset tuples_only off

RESET pg_orca.daft_batch_rows;
SET pg_orca.daft_enable = 'auto';
-- end of daft_planstate
