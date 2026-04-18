# pg_orca

Rust Cascades query optimizer for PostgreSQL 17, implemented as a pgrx extension.

## Build

```bash
# Set PG config path (required for all commands)
export PGRX_PG_CONFIG_PATH=/home/administrator/workspace/install/bin/pg_config

# Build both crates
cargo build -p optimizer_core -p pg_bridge

# Install extension into PG
cd pg_orca_rs/pg_bridge
cargo pgrx install --release --pg-config $PGRX_PG_CONFIG_PATH
```

## Test

```bash
export PGRX_PG_CONFIG_PATH=/home/administrator/workspace/install/bin/pg_config

# Unit tests (optimizer_core, no PG needed)
cargo test -p optimizer_core

# Integration tests (auto-creates ephemeral PG instance, requires pg_bridge.so installed)
cargo test -p pg_bridge --test integration -- --test-threads=1
```

## Project layout

```
pg_orca_rs/
  optimizer_core/    # Pure Rust Cascades engine (no PG dependency)
  pg_bridge/         # pgrx extension: planner_hook + inbound/outbound translation
    src/
      lib.rs             # _PG_init, planner hook, GUCs
      inbound/           # PG Query -> LogicalExpr
      outbound/          # PhysicalPlan -> PlannedStmt
    tests/
      integration.rs     # SQL integration tests (self-contained PG lifecycle)
```

## PG17 notes

- `OUTER_VAR = -2`, `INNER_VAR = -1` (changed from 65000/65001 in PG16)
- `STATRELATTINH` syscache requires 3 keys: `SearchSysCache3(relid, attnum, stainherit)`
- `RelationGetIndexList` returns OID list (use `cell.oid_value`, not `cell.ptr_value`)
- Must set `opfuncid = get_opcode(opno)` on all OpExpr nodes (no `set_plan_references`)
