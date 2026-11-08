-- =====================================================================
-- daft_cancel.sql  (P0-009-E)
-- Cancel / interrupt does not hang.  statement_timeout fires while a real
-- DaftExec repeatedly pulls a deliberately slow PG child and exchanges Arrow
-- batches with the Rust Bridge.  A second DaftExec proves cleanup completed.
-- Covers acceptance #2 (cancel paths do not hang).
-- =====================================================================

SET pg_orca.enable_orca = on;
CREATE OR REPLACE FUNCTION pg_daft_exec_selftest_v1(bigint, bigint, bigint, integer)
RETURNS text
AS '$libdir/pg_orca', 'pg_daft_exec_selftest_v1'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
SET pg_orca.daft_enable = 'force';
SET pg_orca.daft_batch_rows = 1;

-- 10000 rows * 10 ms/row is much longer than this timeout.
\pset tuples_only on
SET statement_timeout = 100;
SELECT pg_daft_exec_selftest_v1(10000, 10000, -1, 10000);

-- Backend and Bridge recover: the timeout did not leak the active handle.
RESET statement_timeout;
SELECT pg_daft_exec_selftest_v1(3, 3, -1, 0);
\pset tuples_only off
