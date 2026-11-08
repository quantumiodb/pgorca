-- P0-007-A freezes metadata; P0-007-D adds the end-to-end batch roundtrip.

-- The ten admitted OIDs are PostgreSQL built-ins with exact Arrow formats in
-- spec/pg_daft_arrow_v1.yaml.  This query detects a PostgreSQL catalog drift.
SELECT oid, typname
FROM pg_type
WHERE oid IN (16, 17, 20, 21, 23, 25, 1043, 1082, 1114, 1184)
ORDER BY oid;

-- PostgreSQL date/timestamp values use the 2000 epoch; Arrow uses 1970.
SELECT DATE '2000-01-01' - DATE '1970-01-01' AS date_epoch_days;
SELECT (extract(epoch FROM
        (TIMESTAMP '2000-01-01 00:00:00' -
         TIMESTAMP '1970-01-01 00:00:00')) * 1000000)::bigint
       AS timestamp_epoch_microseconds;

-- These types deliberately have no P0 mapping.  Their presence in pg_type
-- does not make them eligible for Arrow transport or Daft execution.
SELECT oid, typname, false AS p0_enabled
FROM pg_type
WHERE oid IN (700, 701, 1700)
ORDER BY oid;

SELECT 'schema fingerprint keeps column order, OID, typmod, collation, nullability and exact Arrow format' AS contract;

-- Exercise the real C builder inside a backend.  The probe creates all ten
-- P0 columns, resets the per-row MemoryContext after append, verifies the raw
-- C Data buffers, and releases array/schema on a joined non-PG pthread.
CREATE FUNCTION pg_daft_arrow_input_selftest_v1()
RETURNS text
AS '$libdir/pg_orca', 'pg_daft_arrow_input_selftest_v1'
LANGUAGE C STRICT;

CREATE FUNCTION pg_daft_arrow_boundary_selftest_v1()
RETURNS text
AS '$libdir/pg_orca', 'pg_daft_arrow_boundary_selftest_v1'
LANGUAGE C STRICT;

SELECT pg_daft_arrow_input_selftest_v1();
SELECT pg_daft_arrow_boundary_selftest_v1();

DROP FUNCTION pg_daft_arrow_input_selftest_v1();
DROP FUNCTION pg_daft_arrow_boundary_selftest_v1();

-- End of P0-007-A metadata checks.
