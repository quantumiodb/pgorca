#!/bin/bash
# Run PG regression tests with pg_orca loaded, then re-diff failures
# ignoring plan differences and row ordering.
#
# Usage:
#   ./test/test.sh                          # run full schedule
#   ./test/test.sh select join aggregates   # run specific tests

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PG_REGRESS_DIR=/home/administrator/workspace/postgres/src/test/regress
PG_REGRESS=/home/administrator/workspace/install/lib/postgresql/pgxs/src/test/regress/pg_regress
TEMP_INSTANCE=/tmp/pg_orca_regress
OUTPUT_DIR=/tmp/pg_orca_regress_out
DIFF_TOOL="$SCRIPT_DIR/diff_no_plan.sh"
SCHEDULE="$PG_REGRESS_DIR/parallel_schedule"

# Clean up previous run
rm -rf "$TEMP_INSTANCE" "$OUTPUT_DIR"

echo "=== Running pg_regress with pg_orca ==="
cd "$PG_REGRESS_DIR"

if [ $# -eq 0 ]; then
  # Run full schedule (serial mode: one test at a time)
  $PG_REGRESS \
    --temp-instance="$TEMP_INSTANCE" \
    --temp-config="$SCRIPT_DIR/regression.conf" \
    --inputdir=. \
    --outputdir="$OUTPUT_DIR" \
    --max-connections=1 \
    --schedule="$SCHEDULE" 2>&1
else
  # Run specific tests
  $PG_REGRESS \
    --temp-instance="$TEMP_INSTANCE" \
    --temp-config="$SCRIPT_DIR/regression.conf" \
    --inputdir=. \
    --outputdir="$OUTPUT_DIR" \
    "$@" 2>&1
fi
REGRESS_EXIT=$?

if [ $REGRESS_EXIT -eq 0 ]; then
  echo ""
  echo "=== All tests passed ==="
  exit 0
fi

# Parse pg_regress output to find which tests actually failed
echo ""
echo "=== Re-diffing failures (ignore plans + sort unordered rows) ==="

real_failures=0
while IFS= read -r line; do
  if [[ "$line" =~ ^not\ ok\ [0-9]+[[:space:]]+-[[:space:]]+([^[:space:]]+) ]]; then
    f="${BASH_REMATCH[1]}"
    expected="$PG_REGRESS_DIR/expected/$f.out"
    result="$OUTPUT_DIR/results/$f.out"

    if [ ! -f "$result" ]; then
      echo "  $f: FAIL (no result file)"
      real_failures=$((real_failures + 1))
      continue
    fi

    if bash "$DIFF_TOOL" "$expected" "$result" > /dev/null 2>&1; then
      echo "  $f: OK (plan/order diff only)"
    else
      echo "  $f: FAIL (real diff)"
      bash "$DIFF_TOOL" "$expected" "$result" | head -60
      echo "  ..."
      real_failures=$((real_failures + 1))
    fi
  fi
done < "$OUTPUT_DIR/regression.out"

echo ""
if [ $real_failures -eq 0 ]; then
  echo "=== All failures are plan/order diffs only — pg_orca OK ==="
  exit 0
else
  echo "=== $real_failures test(s) have real differences ==="
  exit 1
fi
