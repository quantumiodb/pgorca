#!/usr/bin/env bash
set -euo pipefail

PG_CONFIG=${PG_CONFIG:-$(command -v pg_config)}
_PGXS=$("$PG_CONFIG" --pgxs)                          # .../pgxs/src/makefiles/pgxs.mk
_PGXS_REGRESS=${_PGXS%/src/makefiles/pgxs.mk}/src/test/regress
PG_REGRESS=${PG_REGRESS:-$_PGXS_REGRESS/pg_regress}
# PG_REGRESS_SQL must point to the PostgreSQL source tree's src/test/regress
# (SQL/expected files are not installed). Set this env var before running.
# e.g.: export PG_REGRESS_SQL=/path/to/postgresql/src/test/regress

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$SCRIPT_DIR/../build"
TEMP_INSTANCE="$BUILD_DIR/tmp_check_parallel"
OUTPUT_DIR="$BUILD_DIR/test_parallel"

mkdir -p "$OUTPUT_DIR"

# ---------------------------------------------------------------------------
# Parse our own options before forwarding the rest to pg_regress.
#
#   --ignore-plans         Pass --gpd_ignore_plans to gpdiff.pl
#   --init-file=FILE       Pass FILE as --gpd_init to gpdiff.pl (repeatable)
#   --pg-tests             Run PG's own parallel_schedule (default)
#   --orca-tests           Run pg_orca's own test/schedule
#   --use-existing         Use a running PG instance instead of --temp-instance
#
# All other --foo options are forwarded to pg_regress.
# Positional args are treated as test names (run without --schedule).
# ---------------------------------------------------------------------------
PG_REGRESS_OPTS=()
TEST_NAMES=()
USE_EXISTING=0
RUN_PG_TESTS=1   # default: run PG's parallel_schedule

for arg in "$@"; do
    case "$arg" in
        --ignore-plans)
            export GPD_IGNORE_PLANS=1
            ;;
        --init-file=*)
            f="${arg#--init-file=}"
            export GPD_INIT_FILES="${GPD_INIT_FILES:+$GPD_INIT_FILES:}$f"
            ;;
        --pg-tests)
            RUN_PG_TESTS=1
            ;;
        --orca-tests)
            RUN_PG_TESTS=0
            ;;
        --use-existing)
            USE_EXISTING=1
            PG_REGRESS_OPTS+=(--use-existing)
            ;;
        --*)
            PG_REGRESS_OPTS+=("$arg")
            ;;
        *)
            TEST_NAMES+=("$arg")
            ;;
    esac
done

# Export gpdiff.pl path so bin/diff can find it regardless of CWD.
export GPDIFF_PATH="$SCRIPT_DIR/gpdiff.pl"

# Always load the default init file (can be supplemented with --init-file).
export GPD_INIT_FILES="${GPD_INIT_FILES:+$GPD_INIT_FILES:}$SCRIPT_DIR/init_file"

# Prepend test/bin to PATH so pg_regress picks up our gpdiff-backed diff wrapper.
export PATH="$SCRIPT_DIR/bin:$PATH"

# Disable parallel query workers so ORCA plans are not mixed with parallel plans.
#export PGOPTIONS="${PGOPTIONS:+$PGOPTIONS }-c max_parallel_workers_per_gather=0"

# Common pg_regress arguments
COMMON_OPTS=(
    --temp-config="$SCRIPT_DIR/regression.conf"
    --outputdir="$OUTPUT_DIR"
    --load-extension=pg_orca
    --max-connections=4
)

if [[ $USE_EXISTING -eq 0 ]]; then
    COMMON_OPTS+=(--temp-instance="$TEMP_INSTANCE")
fi

if [[ ${#TEST_NAMES[@]} -gt 0 ]]; then
    # Run only the named tests against the PG regress SQL dir
    if [[ -z "${PG_REGRESS_SQL:-}" ]]; then
        echo "ERROR: PG_REGRESS_SQL is not set. Point it to the PostgreSQL source tree:" >&2
        echo "  export PG_REGRESS_SQL=/path/to/postgresql/src/test/regress" >&2
        exit 1
    fi
    exec "$PG_REGRESS" \
        "${COMMON_OPTS[@]}" \
        --inputdir="$PG_REGRESS_SQL" \
        "${PG_REGRESS_OPTS[@]+"${PG_REGRESS_OPTS[@]}"}" \
        "${TEST_NAMES[@]}"
elif [[ $RUN_PG_TESTS -eq 1 ]]; then
    # Run PG's full parallel_schedule with pg_orca loaded
    if [[ -z "${PG_REGRESS_SQL:-}" ]]; then
        echo "ERROR: PG_REGRESS_SQL is not set. Point it to the PostgreSQL source tree:" >&2
        echo "  export PG_REGRESS_SQL=/path/to/postgresql/src/test/regress" >&2
        exit 1
    fi
    exec "$PG_REGRESS" \
        "${COMMON_OPTS[@]}" \
        --inputdir="$PG_REGRESS_SQL" \
        --schedule="$SCRIPT_DIR/parallel_schedule" \
        "${PG_REGRESS_OPTS[@]+"${PG_REGRESS_OPTS[@]}"}"
else
    # Run pg_orca's own test schedule
    exec "$PG_REGRESS" \
        "${COMMON_OPTS[@]}" \
        --inputdir="$SCRIPT_DIR" \
        --schedule="$SCRIPT_DIR/schedule" \
        "${PG_REGRESS_OPTS[@]+"${PG_REGRESS_OPTS[@]}"}"
fi
