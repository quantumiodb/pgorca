# Optional Daft Bridge packaging and operations

The deployable unit is deliberately split in two.  The `pg_orca` core package
contains the PostgreSQL extension and the machine-readable
`pg_orca-daft-manifest.json`.  A Daft-capable core is compiled with
`PG_ORCA_ENABLE_DAFT=ON`, but the runtime GUC still defaults to
`pg_orca.daft_enable=off`.  The Rust Bridge is a separate, optional artifact;
the core package never owns or overwrites it.

## Package layout

Build the Daft-capable core package with an explicit, absolute, versioned
Bridge path:

```bash
packaging/build-packages.sh --deb --enable-daft \
  --bridge-default /usr/lib/postgresql/19/lib/pg_orca/bridge-<release-id>/libdaft_pg_bridge.so
```

Install the separately built release Bridge at that exact path with root or
PostgreSQL-server ownership and mode `0755`.  The parent version directory must
not be group/world writable.  Never replace a Bridge file that may already be
mapped by a backend; install a new release ID beside it.

The manifest freezes ABI major 1, minimum minor 1, all 14 symbols required by
the current loader, and four previously published compatibility entry points.
Packaging compares the full 18-symbol `dpg_*` export set with that allowlist;
the four legacy symbols are not part of vtable negotiation.  `pg_orca.so`
itself must not have a dynamic dependency on the Bridge: it resolves the
complete table with `dlopen`/`dlsym` only in a regular client backend.

## Safe enable and diagnostics

After installing the core extension, leave Daft off and restart PostgreSQL.
Confirm the default before any smoke test:

```sql
SHOW pg_orca.daft_enable;  -- off
```

Set `pg_orca.daft_bridge_library` to the new absolute versioned path, then use
`SET pg_orca.daft_enable = auto` in a smoke-test session.  In `auto`, a missing,
untrusted, incomplete, ABI-incompatible, or digest-incompatible Bridge disables
Daft candidates and leaves legal PostgreSQL plans available.  `force` is only
for diagnosis and returns a stable `PGDAFT_BRIDGE_*` reason.

Useful checks:

```sql
SHOW pg_orca.daft_bridge_library;
SHOW pg_orca.daft_enable;
SET pg_orca.daft_trace = on;
```

The postmaster must never map `libdaft_pg_bridge.so` or start Daft/Tokio
threads.  Only a regular query backend may load it, and a successful mapping is
kept until that backend exits.

## Upgrade

1. Set `pg_orca.daft_enable=off` system-wide and reload.
2. Wait for active Daft queries to finish and drain/recycle all old connection
   pool backends.
3. Install the new Bridge into a new versioned directory.  Verify its SHA-256,
   ownership/mode, 18-symbol public allowlist, ABI major/minor, runtime bits, and
   capability-registry digest.
4. Install the paired `pg_orca` core package, set the new absolute Bridge path,
   and restart PostgreSQL.  A reload alone is insufficient because old
   backends retain immutable function pointers and prepared-plan state.
5. Keep the system default off.  Run `auto` smoke and result-differential tests
   in dedicated sessions, then enable wider traffic only under a separate
   release decision.

Do not run `initdb`, `pg_upgrade`, or rewrite application tables for this
extension/Bridge upgrade.  PostgreSQL owns the existing data directory and
catalog dependency invalidation.

## Rollback

Set Daft off, drain old backends, restore the previous core package and the
previous absolute versioned Bridge path, and restart PostgreSQL.  Confirm the
previous SHA/digest pair and repeat the `auto` smoke.  The same data directory
must remain in place; rollback that requires deleting, reinitializing, or
rewriting it is a stop condition.

If a cached Daft plan reaches a backend with an incompatible ABI, runtime bit
set, or registry digest, execution rejects it before Bridge open and before
pulling a child tuple.  Never make a new Bridge guess an old payload or replace
the selected plan tree inside the executor.
