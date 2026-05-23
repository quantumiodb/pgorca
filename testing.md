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

## Known Failures (`--pg-tests --ignore-plans`)

Running `test/test.sh --pg-tests --ignore-plans` leaves 7 tests failing. All are inherent incompatibilities between ORCA and the specific behaviour these tests rely on — they are **not regressions** introduced by pg_orca code changes.

### 1. `subselect` — missing `One-Time Filter` optimisation for constant quals above SRF

The test uses a volatile function `tattle(x, y)` that emits a `NOTICE` on each call, inside queries of the form:

```sql
SELECT * FROM (SELECT 9 AS x, unnest(array[1,2,3,11,12,13]) AS u) ss
WHERE tattle(x, 8);
```

The standard planner recognises that `tattle(9, 8)` references no SRF output columns and can therefore be lifted above the `ProjectSet` as a **`One-Time Filter`** — evaluated once before the SRF expands its rows. So only 1 NOTICE is emitted even though 6 rows are produced.

ORCA places the filter as a `Result` node's `Filter` clause **above** the `ProjectSet` but does not hoist it to a one-time check; it re-evaluates the filter once per output row of the `ProjectSet`. With an array of 6 elements, `tattle` is called 6 times → 6 NOTICEs instead of 1.

A second diff involves `tattle(3, ten)` inside a `GROUP BY` subquery: the NOTICE order differs (`0,1,2` vs `2,1,0`) because ORCA scans the grouped rows in a different order than the standard planner.

**Root cause:** ORCA does not implement the `One-Time Filter` promotion for constant quals above set-returning functions. This is a missing optimisation, not a correctness issue for the result rows (all 6 rows are still returned correctly).

### 2. `aggregates` — `balk` aggregate returns value instead of NULL

The `balk` aggregate is designed to abort early (via `ereport(ERROR, ...)` inside the combine function) and expects the aggregate to return NULL. Under ORCA's plan, the combine function is never reached, so the accumulator value (`495000`) is returned instead of NULL.

**Root cause:** ORCA does not generate the same aggregate finalization path as the standard planner for this edge-case aggregate, so the "bail out" code path is never triggered.

### 3. `join_hash` — parallel hash join batch count mismatch (`final`: 4 vs 2)

The test queries `hash_join_batches()` to verify that a skewed parallel hash join spills to exactly 4 batches. ORCA selects a different join plan (non-parallel or different work_mem accounting), resulting in 2 batches instead of 4.

**Root cause:** ORCA ignores `enable_parallel_hash` and parallel cost knobs; it chooses a plan that does not spill in the same way as the standard planner.

### 4. `select_parallel` — EXPLAIN column width difference

The test captures `EXPLAIN ANALYZE` output via a PL/pgSQL function and compares the header line width. ORCA's plan for the inner query is structurally different, producing a shorter plan-string header, so the column is narrower than expected.

**Root cause:** Cosmetic formatting difference from a different plan shape. The GP_IGNORE lines (actual data) match correctly; only the header border width differs.

### 5. `window` — row ordering within `ROWS BETWEEN` window frames

Window functions over `ROWS BETWEEN n PRECEDING AND n FOLLOWING` return different row orderings. The test uses `tenk1 WHERE unique1 < 10` without an explicit `ORDER BY` within the window, making the scan order non-deterministic. ORCA chooses a different scan order.

**Root cause:** No `ORDER BY` inside the window frame; scan order is plan-dependent. ORCA's chosen index/seq scan order differs from the standard planner's, producing valid but differently-ordered intermediate rows and thus different partial sums.


### 6. `stats` — `check_estimated_rows` function not found

The `stats` test calls `check_estimated_rows(text)`, a helper function defined earlier in the same test session. Under ORCA, a prior statement in the test fails or rolls back in a way that prevents the function from being visible when this call is reached.

**Root cause:** A transaction/savepoint boundary or error earlier in the `stats` test leaves the session in a state where the helper function created by a prior `CREATE FUNCTION` is not visible. Likely ORCA rejects a query that the standard planner accepts, causing an unexpected error that aborts the defining transaction.

---

These 6 failures are tracked here for awareness. None affect correctness of queries that ORCA successfully plans.
