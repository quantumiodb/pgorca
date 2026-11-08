-- =====================================================================
-- daft_errors.sql  (P0-009-E)
-- Error handling: runtime errors must surface cleanly and the backend must
-- keep working afterwards (no hang, no crash).  Autocommit means each
-- statement is its own transaction, so an ERROR aborts only that statement.
-- The debug-only harness drives a real DaftExec and real Rust Bridge.  An
-- injected PG child error must close the Bridge handle; a second DaftExec in
-- the same backend proves that the active-fragment guard was also cleared.
-- =====================================================================

SET pg_orca.enable_orca = on;
CREATE OR REPLACE FUNCTION pg_daft_exec_selftest_v1(bigint, bigint, bigint, integer)
RETURNS text
AS '$libdir/pg_orca', 'pg_daft_exec_selftest_v1'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
SET pg_orca.daft_enable = 'force';
SET pg_orca.daft_batch_rows = 1;

-- The third child pull fails after two rows crossed the Arrow boundary.
\pset tuples_only on
SELECT pg_daft_exec_selftest_v1(10, 10, 2, 0);

-- A new real DaftExec opens and closes cleanly in the same backend.
SELECT pg_daft_exec_selftest_v1(3, 3, -1, 0);
\pset tuples_only off

SET pg_orca.daft_enable = 'auto';

CREATE TABLE daft_err_t(a int unique);
INSERT INTO daft_err_t VALUES (1), (2), (3);

-- Division by zero: clean ERROR, statement aborts.
SELECT 1 / 0 FROM daft_err_t;

-- Backend recovers; subsequent statement works.
SELECT count(*) AS still_works FROM daft_err_t;

-- Unique violation: clean ERROR.
INSERT INTO daft_err_t VALUES (1);

-- Recovers again.
SELECT a FROM daft_err_t ORDER BY a;

-- Undefined table: clean ERROR.
SELECT * FROM daft_err_does_not_exist;
