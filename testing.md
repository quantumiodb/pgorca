# Testing pg_orca

## Prerequisites

Before running any tests, set these two environment variables. Both are **required**.

```bash
export PG_CONFIG=/Users/jianghua/pg-install/bin/pg_config
export PG_REGRESS_SQL=/Users/jianghua/code/postgresql/src/test/regress
```

- `PG_CONFIG` — points to the `pg_config` binary of the PostgreSQL 18 installation that pg_orca was built and installed into.
- `PG_REGRESS_SQL` — points to the PostgreSQL source tree's `src/test/regress` directory. The SQL and expected-output files for the standard regression tests are **not** installed by `make install`, so they must be read from the source tree at test time.

If either variable is unset, `test/test.sh` will abort with an explicit error message.

## Build First

Tests require the extension to be built and installed:

```bash
mkdir build && cd build
cmake .. -DPG_CONFIG="$PG_CONFIG" -DCMAKE_BUILD_TYPE=Debug -GNinja
ninja -j$(nproc)
ninja install
```

## Running Tests

All test commands are driven by `test/test.sh` from the repository root.

### pg_orca's own regression tests

These tests live in `test/sql/` and `test/expected/`. They load pg_orca, enable the ORCA optimizer, and verify ORCA-specific behavior.

```bash
test/test.sh --orca-tests
```

### PostgreSQL standard regression suite with ORCA loaded

Runs PostgreSQL's full `parallel_schedule` with `pg_orca` loaded as an extension. This checks that ORCA does not break standard SQL semantics.

```bash
test/test.sh --pg-tests
```

`--pg-tests` is the default when no mode flag is given.

### Run individual tests

Pass test names as positional arguments. `PG_REGRESS_SQL` must be set because the SQL files are read from the PostgreSQL source tree.

```bash
# Single test
test/test.sh select

# Multiple tests
test/test.sh select join aggregates
```

### Use a running PostgreSQL instance

By default the test harness spins up a temporary `--temp-instance`. To run against an already-running server:

```bash
test/test.sh --use-existing --orca-tests
```

### Ignoring plan differences (`--ignore-plans`)

When running PG's standard regression suite with ORCA loaded, ORCA often produces plans that are semantically equivalent but textually different from what PG's stock planner would produce (different join order, different scan nodes, etc.). These differences cause `EXPLAIN`-containing tests to fail even though the results are correct.

Use `--ignore-plans` to suppress plan-shape noise and focus on result-set correctness:

```bash
test/test.sh --pg-tests --ignore-plans
```

This flag sets `GPD_IGNORE_PLANS=1`, which tells `gpdiff.pl` to strip plan output before comparing expected vs. actual. It is the recommended default when running the full PG regression suite under ORCA. Omit it only when you specifically want to audit or lock down ORCA's plan shapes.

## Options

| Option | Description |
|--------|-------------|
| `--orca-tests` | Run pg_orca's own `test/schedule` |
| `--pg-tests` | Run PG's `parallel_schedule` with pg_orca loaded (default) |
| `--use-existing` | Connect to a running server instead of starting a temp instance |
| `--ignore-plans` | Ignore plan differences when comparing output (passes `--gpd_ignore_plans` to `gpdiff.pl`) |
| `--init-file=FILE` | Add an extra `gpdiff.pl` init file (repeatable) |

Any other `--foo` flags are forwarded directly to `pg_regress`.

## Output

Test output lands in `build/test_parallel/`. On failure, `pg_regress` prints a diff of expected vs. actual output. The custom diff wrapper in `test/bin/` uses `test/gpdiff.pl` so that plan-shape differences can be selectively ignored.

## Troubleshooting

**`ERROR: PG_REGRESS_SQL is not set`** — export the variable as shown at the top of this document.

**`pg_config: command not found`** — export `PG_CONFIG` as shown at the top, or add the PG `bin/` directory to `PATH`.

**Extension not found** — run `ninja install` inside `build/` before running tests.

**Plan diffs on PG tests** — use `--ignore-plans` to suppress plan-shape noise and focus on result-set correctness.
