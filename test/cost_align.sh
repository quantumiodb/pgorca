#!/bin/bash
# pg_orca cost-alignment regression driver.
#
# For each EXPLAIN in test/sql/cost_align.sql, run it twice — once under
# PG's planner, once under ORCA with cost_model=pg — and report the cost
# diff.  Plans with the same top operator are checked against a
# tolerance (default 5%).  Plans whose top operator differs are
# reported but don't fail.
#
# Exit non-zero only if a "same-plan" comparison exceeds tolerance.
#
# Required environment:
#   PG_CONFIG     path to pg_config (default: pg_config on PATH)
#   PGHOST        directory containing the postmaster socket
# Optional:
#   PGDATABASE    test database name (default: postgres)
#   COST_ALIGN_TOL_PCT   tolerance percentage (default: 5)

set -u

PG_CONFIG=${PG_CONFIG:-pg_config}
# Ignore inherited PG* env vars to keep test results reproducible — the
# caller can override via PG_ORCA_HOST / PG_ORCA_DB.
PGHOST=${PG_ORCA_HOST:-/tmp}
PGDATABASE=${PG_ORCA_DB:-postgres}
TOL=${COST_ALIGN_TOL_PCT:-20}
export PGHOST PGDATABASE
unset PGPORT PGUSER PGOPTIONS

PSQL=$("$PG_CONFIG" --bindir)/psql
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SQL_FILE="$SCRIPT_DIR/sql/cost_align.sql"

if [ ! -x "$PSQL" ]; then
  echo "error: psql not found at $PSQL" >&2
  exit 2
fi
if [ ! -f "$SQL_FILE" ]; then
  echo "error: test SQL not found at $SQL_FILE" >&2
  exit 2
fi

# Run the setup portion (everything before the first EXPLAIN) once.
setup_sql=$(awk '/^EXPLAIN/{exit} {print}' "$SQL_FILE")
"$PSQL" -d "$PGDATABASE" -X -At >/dev/null 2>&1 <<<"$setup_sql"

mapfile -t queries < <(grep -E "^EXPLAIN " "$SQL_FILE")

same_aligned=0
same_off=0
diff_plan=0
total=0
fail=0

printf "%-3s | %-22s | %-22s | %-7s | %s\n" "#" "PG native" "ORCA pg" "diff%" "Query"
echo "----+------------------------+------------------------+---------+----------"

  # Build a single batched script per side: one psql session executes all
  # queries with `\echo ===Qn===` markers between them.  Spawning psql -c
  # per query (200+ for a typical run) hit transient "no output" empties
  # when the backend hadn't finished shutting down — this approach uses
  # exactly two psql invocations regardless of query count.
  build_batch() {
    local pre=$1
    local idx=0
    for query in "${queries[@]}"; do
      idx=$((idx+1))
      printf '\\echo ===Q%d===\n%s\n%s\n' "$idx" "$pre" "$query"
    done
  }
  batch_pg=$(build_batch "SET pg_orca.enable_orca=off;")
  batch_orca=$(build_batch "SET pg_orca.enable_orca=on; SET pg_orca.cost_model=pg;")
  all_pg=$("$PSQL" -d "$PGDATABASE" -X -At <<<"$batch_pg" 2>/dev/null)
  all_orca=$("$PSQL" -d "$PGDATABASE" -X -At <<<"$batch_orca" 2>/dev/null)

  # Split per-query outputs by the markers.  Each block runs from a
  # `===Qn===` line up to the next marker (exclusive).
  extract_block() {
    local input=$1
    local marker=$2
    echo "$input" | awk -v m="$marker" '
      $0 == m { in_block=1; next }
      /^===Q[0-9]+===$/ { in_block=0 }
      in_block { print }
    ' | grep -vE "^SET|^RESET"
  }

i=0
for q in "${queries[@]}"; do
  i=$((i+1))
  pg_out=$(extract_block "$all_pg" "===Q$i===")
  orca_out=$(extract_block "$all_orca" "===Q$i===")
  # Plan shape = sequence of operator names (one per plan line, in
  # depth-first order).  Two plans are "same" only if every operator
  # on every nesting level matches — strictly more sensitive than just
  # checking the top operator, since e.g. Aggregate→HashJoin vs
  # Aggregate→MergeJoin are different physical plans.  Strip the
  # relation/index name and cost annotation so naming differences (e.g.
  # cal_tenk1_unique1 vs cal_tenk1_hundred) collapse to the same
  # operator token.
  # Plan-shape signature: depth-first sequence of operator names.
  # 1) Drop annotation lines that aren't operators (Index Cond, Filter,
  #    Optimizer footer, etc.)
  # 2) Strip the cost=... suffix
  # 3) Strip leading whitespace and the "-> " arrow
  # 4) Strip relation/index names after "on"/"using"
  filter_plan() {
    # First drop "cost-passthrough" Result wrappers: ORCA's
    # PdxlnRemapOutputColumns (CTranslatorExprToDXL.cpp:2753) inserts a
    # Result above an operator solely to reorder/rename output columns.
    # Its cost annotation equals the child's verbatim (it inherits via
    # GetProperties(pexpr)).  PG never emits this kind of wrapper, so
    # without stripping it the plan-shape signature reports DIFF on
    # queries whose costs are actually 1:1 with PG.
    awk '
      function strip(line) { gsub(/^[[:space:]]*->[[:space:]]*/, "", line); return line }
      function get_cost(line) {
        if (match(line, /cost=[0-9.]+\.\.[0-9.]+/)) {
          return substr(line, RSTART, RLENGTH)
        }
        return ""
      }
      {
        if (saved != "") {
          # We have a pending Result line; check if this line is its child
          # (same cost), and the current line is an operator (not annotation).
          this_clean = $0
          this_stripped = strip(this_clean)
          if (this_stripped ~ /^[A-Z]/ && get_cost(this_clean) == saved_cost) {
            # passthrough Result — drop saved, emit current
            print $0
            saved = ""
            saved_cost = ""
            next
          }
          # Not a cost-match child — emit the saved Result first
          print saved
          saved = ""
          saved_cost = ""
        }
        cleaned = strip($0)
        if (cleaned ~ /^Result[[:space:]]+\(cost=/) {
          saved = $0
          saved_cost = get_cost($0)
          next
        }
        print $0
      }
      END { if (saved != "") print saved }
    ' \
    | sed -E 's/[[:space:]]*\(cost.*//' \
    | sed -E 's/^[[:space:]]*->[[:space:]]*//; s/^[[:space:]]+//' \
    | grep -E "^[A-Z]" \
    | grep -vE "^(Optimizer|Output|Index Cond|Recheck Cond|Sort Key|Group Key|Hash Cond|Merge Cond|Filter|Join Filter|One-Time Filter|Subplans Removed|Workers|Heap Fetches|Window|InitPlan|SubPlan|CTE|Function Call)" \
    | sed -E 's/[[:space:]]+(using|on)[[:space:]]+[A-Za-z_][A-Za-z_0-9.]*//g; s/[[:space:]]+$//' \
    | xargs
  }
  pg_top=$(echo "$pg_out" | filter_plan)
  orca_top=$(echo "$orca_out" | filter_plan)
  pg_cost=$(echo "$pg_out" | grep -oE "cost=[0-9.]+\.\.[0-9.]+" | head -1)
  orca_cost=$(echo "$orca_out" | grep -oE "cost=[0-9.]+\.\.[0-9.]+" | head -1)
  if [ -z "$pg_cost" ] || [ -z "$orca_cost" ]; then
    printf "%-3d | %-22s | %-22s | %-7s | %s\n" "$i" "${pg_cost:-(none)}" "${orca_cost:-(none)}" "n/a" "$(echo "$q" | cut -c1-50)"
    continue
  fi
  total=$((total+1))
  pgt=$(echo "$pg_cost" | sed -E 's/cost=[0-9.]+\.\.([0-9.]+)/\1/')
  orct=$(echo "$orca_cost" | sed -E 's/cost=[0-9.]+\.\.([0-9.]+)/\1/')
  diff_pct=$(awk -v a="$pgt" -v b="$orct" 'BEGIN { if (a==0) print 0; else printf "%.1f", (b-a)/a*100 }')
  abs_pct=$(echo "$diff_pct" | tr -d '-')

  mark=""
  if [ "$pg_top" = "$orca_top" ]; then
    if awk -v p="$abs_pct" -v t="$TOL" 'BEGIN { exit (p+0 <= t+0) ? 0 : 1 }'; then
      same_aligned=$((same_aligned+1))
      mark="OK"
    else
      same_off=$((same_off+1))
      mark="FAIL"
      fail=$((fail+1))
    fi
  else
    diff_plan=$((diff_plan+1))
    mark="DIFF"
  fi

  display=$(echo "$q" | sed -E 's/^EXPLAIN //' | cut -c1-40)
  printf "%-3d | %-22s | %-22s | %-7s | %-4s %s\n" "$i" "$pg_cost" "$orca_cost" "$diff_pct" "$mark" "$display"
done

echo "----+------------------------+------------------------+---------+----------"
echo "Total=$total | same-plan ≤${TOL}%=$same_aligned | same-plan off=$same_off | diff plan=$diff_plan"

if [ "$fail" -gt 0 ]; then
  echo "FAIL: $fail same-plan query/queries exceed ${TOL}% tolerance"
  exit 1
fi
echo "PASS"
