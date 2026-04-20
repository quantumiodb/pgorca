# pg_orca

A PostgreSQL 18 extension that plugs the **ORCA query optimizer** (originally from Greenplum / Apache Cloudberry) into a standard single-node PostgreSQL instance.

## Overview

ORCA is a cost-based, rule-driven query optimizer that operates on an intermediate representation called DXL (Data eXchange Language). It was designed for massively-parallel processing (MPP) databases but contains a powerful optimization engine that is useful in single-node mode as well.

This project extracts ORCA's four core libraries and the PostgreSQL integration layer from Apache Cloudberry, adapts them for PG18, and packages the result as a `CREATE EXTENSION`-installable plugin.

### Components

| Directory | Description |
|-----------|-------------|
| `libgpos/` | ORCA memory pool, error handling, concurrency primitives |
| `libnaucrates/` | DXL parser/serializer, metadata abstractions |
| `libgpopt/` | Core optimizer: search, transformation rules, cost model |
| `libgpdbcost/` | GPDB-specific cost model implementation |
| `gpopt/` | PostgreSQL ↔ DXL translation layer (relcache, planner bridge) |
| `compat/` | Stub headers replacing MPP-only Cloudberry types |
| `pg_orca.cpp` | Extension entry point, `planner_hook`, GUC definitions |

## Requirements

- PostgreSQL 18
- [xerces-c](https://xerces.apache.org/xerces-c/) (XML parsing for DXL)
- CMake ≥ 3.16
- C++17 compiler (clang or gcc)

On macOS with Homebrew:
```bash
brew install xerces-c cmake
```

## Build & Install

```bash
mkdir build && cd build
cmake .. -DPG_CONFIG=/Users/jianghua/pg-install/bin/pg_config -DCMAKE_BUILD_TYPE=Debug -GNinja
ninja -j$(nproc)
ninja install
```

To rebuild after source changes:

```bash
cd build && ninja -j$(nproc)
```

## Usage
- Configure shared_preload_libraries = 'pg_orca', or manually load 'pg_orca.so';

```sql
-- Load the extension (once per database)
CREATE EXTENSION pg_orca;

-- Enable ORCA for the current session
SET pg_orca.enable_orca = on;

-- Run a query — ORCA will optimize it
EXPLAIN SELECT * FROM t WHERE id > 100;
```

If ORCA cannot handle a query (unsupported feature or internal error) it falls back to the standard PostgreSQL planner automatically.

## GUC Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `pg_orca.enable_orca` | `off` | Enable ORCA optimizer |
| `pg_orca.trace_fallback` | `off` | Log a message on fallback to standard planner |
| `optimizer_segments` | `1` | Number of segments for cost estimation |
| `optimizer_sort_factor` | `1.0` | Cost scaling factor for sort operations |
| `optimizer_metadata_caching` | `on` | Cache relation metadata between calls |
| `optimizer_mdcache_size` | `16384` | Metadata cache size (KB) |
| `optimizer_search_strategy_path` | `""` | Path to custom search strategy XML (empty = built-in) |

## Current Status (Phase 1)

Phase 1 goal: **compile and load on PG18 single-node**.

- [x] All four ORCA libraries compile against PG18 headers
- [x] DXL ↔ PG plan translation layer compiles
- [x] Extension loads via `CREATE EXTENSION`
- [x] Basic `SELECT`, `WHERE`, `GROUP BY`, `JOIN` queries produce valid plans
- [ ] Full regression test suite
- [ ] MPP motion nodes disabled cleanly (no crashes on partitioned tables)
- [ ] Parallel query support

## Architecture Notes

### MPP Stubs

Cloudberry's translation layer references many MPP-only types (`Motion`, `PlanSlice`, `GpPolicy`, `DynamicSeqScan`, etc.). These are stubbed in `compat/cdb/cdb_plan_nodes.h` so the code compiles. ORCA will never generate these nodes in single-node mode.

### GPDB GUCs

Many ORCA configuration knobs were GPDB-specific GUCs. They are re-defined as real GUCs in `pg_orca.cpp` under the `optimizer.*` prefix, so existing ORCA code referencing them continues to work.

### walker macros

PG18 removed `optimizer/walkers.h`. The expression-walker macros (`expression_tree_walker`, `query_tree_walker`, etc.) are redirected to their `_impl` variants via `#define` in each translation file that needs them.


