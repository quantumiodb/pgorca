-- P0-006-C: lazy Bridge loader failure policy and preload isolation.
-- The helper creates two non-Rust shared-library fixtures under an exact,
-- non-sensitive test path; it never loads either fixture in the postmaster.
\! bash "$PGD_WORKSPACE/公共文档/多模优化与计算/执行包/scripts/test_p0_006_c.sh" setup

SET pg_orca.enable_orca = on;

-- auto: an absent file must silently disable Daft for this plan while ORCA
-- still returns and executes its PostgreSQL alternative.
SET pg_orca.daft_bridge_library = '/tmp/pg_orca_p0_006_c/not_present.so';
SET pg_orca.daft_enable = auto;
SELECT count(*) >= 0 AS auto_missing_library_ok FROM pg_class;

-- force: the same condition must become a stable, redacted diagnostic.
SET pg_orca.daft_enable = force;
SELECT oid FROM pg_class LIMIT 1;

-- A library with only one exported ABI symbol must not publish a partial
-- vtable.  auto remains safe; force reports the exact compatibility reason.
SET pg_orca.daft_bridge_library = '/tmp/pg_orca_p0_006_c/missing_symbol.so';
SET pg_orca.daft_enable = auto;
SELECT count(*) >= 0 AS auto_missing_symbol_ok FROM pg_class;
SET pg_orca.daft_enable = force;
SELECT oid FROM pg_class LIMIT 1;

-- All symbols are present, but ABI major 2 is incompatible with pg_orca v1.
SET pg_orca.daft_bridge_library = '/tmp/pg_orca_p0_006_c/abi_major_mismatch.so';
SET pg_orca.daft_enable = auto;
SELECT count(*) >= 0 AS auto_abi_mismatch_ok FROM pg_class;
SET pg_orca.daft_enable = force;
SELECT oid FROM pg_class LIMIT 1;

-- shared_preload_libraries=pg_orca must still leave the postmaster without a
-- Bridge mapping or Daft/Tokio thread after child backends exercised failures.
\! bash "$PGD_WORKSPACE/公共文档/多模优化与计算/执行包/scripts/test_p0_006_c.sh" assert-preload

RESET pg_orca.daft_enable;
RESET pg_orca.daft_bridge_library;
RESET pg_orca.enable_orca;
