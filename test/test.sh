#!/usr/bin/env bash
set -euo pipefail

PG_INSTALL=${PG_INSTALL:-/Users/jianghua/pg-install}
PG_REGRESS=${PG_REGRESS:-$PG_INSTALL/lib/postgresql/pgxs/src/test/regress/pg_regress}
PG_REGRESS_SQL=${PG_REGRESS_SQL:-/Users/jianghua/code/postgresql/src/test/regress}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$SCRIPT_DIR/../build"
TEMP_INSTANCE="$BUILD_DIR/tmp_check_parallel"
OUTPUT_DIR="$BUILD_DIR/test_parallel"

mkdir -p "$OUTPUT_DIR"

# Separate pg_regress options (--foo) from test names (positional args).
PG_REGRESS_OPTS=()
TEST_NAMES=()
for arg in "$@"; do
    if [[ "$arg" == --* ]]; then
        PG_REGRESS_OPTS+=("$arg")
    else
        TEST_NAMES+=("$arg")
    fi
done

# If specific test names were given, run only those (no --schedule).
# Otherwise fall back to the default parallel schedule.
if [[ ${#TEST_NAMES[@]} -gt 0 ]]; then
    exec "$PG_REGRESS" \
        --temp-instance="$TEMP_INSTANCE" \
        --temp-config="$SCRIPT_DIR/regression.conf" \
        --inputdir="$PG_REGRESS_SQL" \
        --outputdir="$OUTPUT_DIR" \
        --load-extension=pg_orca \
        --max-connections=1 \
        "${PG_REGRESS_OPTS[@]+"${PG_REGRESS_OPTS[@]}"}" \
        "${TEST_NAMES[@]+"${TEST_NAMES[@]}"}"
else
    exec "$PG_REGRESS" \
        --temp-instance="$TEMP_INSTANCE" \
        --temp-config="$SCRIPT_DIR/regression.conf" \
        --inputdir="$PG_REGRESS_SQL" \
        --outputdir="$OUTPUT_DIR" \
        --load-extension=pg_orca \
        --max-connections=1 \
        --schedule="$PG_REGRESS_SQL/parallel_schedule" \
        "${PG_REGRESS_OPTS[@]+"${PG_REGRESS_OPTS[@]}"}"
fi
