# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Summary

pg_orca is a PostgreSQL 18 extension that integrates the ORCA query optimizer (from Apache Cloudberry/Greenplum) into single-node PostgreSQL. ORCA is a cost-based optimizer using an intermediate representation called DXL (Data eXchange Language).

## Build Commands

```bash
# Configure (from repo root)
mkdir -p build && cd build
cmake .. -DPG_CONFIG=/path/to/pg_config -DCMAKE_BUILD_TYPE=Debug -GNinja

# Build
ninja -j$(nproc)

# Install into PG
ninja install

# Rebuild after source changes (no reconfigure needed)
cd build && ninja -j$(nproc)
```

Requirements: PostgreSQL 18, xerces-c, CMake >= 3.20, Ninja, C++17 compiler.

## Architecture

### Core Flow (Query Optimization Path)

1. `_PG_init` registers `planner_hook` and `ExplainOneQuery_hook`
2. `pg_orca_planner` (pg_orca.cpp) intercepts SELECT queries → calls `InitGPOPT()` once, then `GPOPTOptimizedPlan()`
3. `GPOPTOptimizedPlan` → `COptTasks::GPOPTOptimizedPlan` (gpopt/utils/) → translates PG Query to DXL → runs ORCA optimization → translates DXL back to PlannedStmt
4. On any failure, falls back to `standard_planner`

### Code Layout — What's Project Code vs. Vendored

**Project-specific code** (the files you'll edit):
- `pg_orca.cpp` — Extension entry point, planner hook, GUC definitions, `_PG_init`/`_PG_fini`
- `gpopt/` — PG↔ORCA bridge: config/ (GUC→ORCA param mapping), relcache/ (metadata via PG relcache), translate/ (Query↔DXL and DXL↔PlannedStmt), utils/ (COptTasks optimizer entry, memory pool)
- `include/gpopt/` — Headers for the gpopt integration layer
- `compat/` — Stub headers replacing MPP-only Cloudberry types (GpPolicy, Motion, DynamicSeqScan, etc.) with single-node PG18-compatible stubs

**Vendored ORCA libraries** (rarely edited directly):
- `libgpos/` — Memory pools, error handling, concurrency (ORCA base library)
- `libnaucrates/` — DXL XML parser/serializer, metadata abstractions, operators, statistics
- `libgpopt/` — Core optimizer: search engine, transformation rules (xforms), cost model framework
- `libgpdbcost/` — GPDB-specific cost model

### Key Design Decisions

- **MPP stubs**: Cloudberry's MPP-only types are stubbed in `compat/cdb/` — ORCA never generates these nodes in single-node mode
- **GPDB GUCs**: Re-registered as real PG GUCs with `optimizer_*` prefix so existing ORCA code references work unchanged
- **Walker macros**: PG18 removed `optimizer/walkers.h`; code uses `_impl` variants via `#define` in translation files
- **ORCA is lazily initialized**: `InitGPOPT()` is called on first SELECT, not at `_PG_init` time

## Testing

No project-specific test suite exists yet. Manual testing via psql:

```sql
CREATE EXTENSION pg_orca;
LOAD 'pg_orca';
SET pg_orca.enable_orca = on;
EXPLAIN SELECT * FROM t WHERE id > 100;
```

## Coding Conventions

- C++17, compiled with `-Wall -Wno-unused-parameter -Wno-sign-compare -fvisibility=hidden`
- Compile definitions: `GPOS_DEBUG`, `USE_CMAKE`
- ORCA code uses `GPOS_NEW(mpool)` for memory allocation via ORCA's own memory pools (not regular `new`)
- The extension is built as a single MODULE library linking all ORCA object code plus the gpopt integration layer
- On macOS: uses `-bundle_loader` pointing at the postgres binary; `.dylib` symlink created at install time
