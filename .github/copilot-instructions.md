# Copilot instructions for pg_orca

pg_orca is a PostgreSQL 18 extension that plugs the ORCA cost-based optimizer
(from Apache Cloudberry/Greenplum) into single-node PostgreSQL via
`planner_hook`.  On any ORCA failure it falls back to `standard_planner`.

## Environment

`.github/workflows/copilot-setup-steps.yml` has already installed the
dependencies on this Ubuntu 24.04 runner and left a configured Debug build in
`build/`:

- PostgreSQL 18 from PGDG: `PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config`
- xerces-c, CMake, Ninja, gcc, gdb

Always use the PG 18 `pg_config` above; the runner image also ships other
PostgreSQL versions.

## Build

```bash
cmake --build build -j "$(nproc)"   # incremental rebuild
sudo cmake --install build          # tests load the installed extension
```

Reconfigure only if `build/` is missing:

```bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug \
  -DPG_CONFIG=/usr/lib/postgresql/18/bin/pg_config
```

## Test

Install first, then run pg_orca's own regression schedule (`test/schedule`):

```bash
PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config test/test.sh --orca-tests
```

- Results and diffs: `build/test_parallel/` (`regression.diffs`)
- Server logs: `build/tmp_check_parallel/log/`
- Expected outputs live in `test/expected/`; output is compared with
  `test/gpdiff.pl`, so plan-shape changes show up as diffs.
- `test/test.sh` without `--orca-tests` runs PostgreSQL's own regression suite
  and needs `PG_REGRESS_SQL` pointing at a PG source tree; skip it unless the
  task requires it.

## Code layout

- `pg_orca.cpp` — extension entry point, planner hook, GUCs
- `gpopt/`, `include/gpopt/` — PG ↔ ORCA bridge (Query→DXL, DXL→PlannedStmt,
  relcache metadata, GUC→ORCA config)
- `compat/` — stubs for MPP-only Cloudberry types
- `libgpos/`, `libnaucrates/`, `libgpopt/`, `libgpdbcost/` — vendored ORCA
  (edit only when the fix belongs in the optimizer itself)

## Conventions

- C++17; ORCA objects are allocated with `GPOS_NEW(mp)` from ORCA memory pools,
  not plain `new`.
- Match the style of the surrounding code.
- Keep fallback-to-`standard_planner` behaviour intact: unsupported queries
  must not error out.
