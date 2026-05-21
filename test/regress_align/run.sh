#!/bin/bash
# Cost-alignment driver for the modified PG-regress SQL files in this dir.
#
# Each *.sql is a copy of postgres/src/test/regress/sql/*.sql with every
# `EXPLAIN (COSTS OFF)` rewritten to `EXPLAIN (COSTS ON)` so the planner
# emits cost estimates.  This driver:
#   1. preprocesses the file to insert `\echo ===Q<n>===` markers before
#      each EXPLAIN statement;
#   2. runs the preprocessed file twice in a transaction (rolled back at
#      the end) — once under PG-native, once under ORCA cost_model=pg;
#   3. compares the cost on each EXPLAIN whose plan shape matches.
#
# Required env (defaults in parens):
#   PGHOST (/tmp), PGDATABASE (pgcost_align)
#   TOL (1.0)            — percent tolerance for same-plan cost
#   TIMEOUT (60)         — per-file timeout (seconds)

set -u
PGHOST=${PGHOST:-/tmp}
PGDATABASE=${PGDATABASE:-pgcost_align}
PSQL=${PSQL:-/home/administrator/workspace/install/bin/psql}
TOL=${TOL:-1.0}
TIMEOUT=${TIMEOUT:-60}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PGHOST PGDATABASE
unset PGPORT PGUSER PGOPTIONS

filter_plan() {
  awk '
    function strip(line) { gsub(/^[[:space:]]*->[[:space:]]*/, "", line); return line }
    function get_cost(line) {
      if (match(line, /cost=[0-9.]+\.\.[0-9.]+/)) return substr(line, RSTART, RLENGTH)
      return ""
    }
    {
      if (saved != "") {
        cleaned = strip($0)
        if (cleaned ~ /^[A-Z]/ && get_cost($0) == saved_cost) {
          print $0; saved=""; saved_cost=""; next
        }
        print saved; saved=""; saved_cost=""
      }
      cleaned = strip($0)
      if (cleaned ~ /^Result[[:space:]]+\(cost=/) {
        saved=$0; saved_cost=get_cost($0); next
      }
      print $0
    }
    END { if (saved != "") print saved }
  ' \
  | sed -E 's/[[:space:]]*\(cost.*//' \
  | sed -E 's/^[[:space:]]*->[[:space:]]*//; s/^[[:space:]]+//' \
  | grep -E "^[A-Z]" \
  | grep -vE "^(SET|RESET|BEGIN|ROLLBACK|COMMIT|Optimizer|Output|Index Cond|Recheck Cond|Sort Key|Group Key|Hash Cond|Merge Cond|Filter|Join Filter|One-Time Filter|Subplans Removed|Workers|Heap Fetches|Window|InitPlan|SubPlan|CTE|Function Call|Cache Key|Cache Mode|Memory Usage|Memory|Result$)" \
  | sed -E 's/[[:space:]]+(using|on)[[:space:]]+[A-Za-z_][A-Za-z_0-9.]*([[:space:]]+[A-Za-z_][A-Za-z_0-9.]*)?//g; s/[[:space:]]+$//' \
  | xargs
}

# Insert a \echo sentinel before each EXPLAIN.  Multi-line EXPLAINs handled
# (ended by `;` on its own line or at end of line).
preprocess() {
  awk '
    BEGIN { qn = 0 }
    /^[[:space:]]*EXPLAIN/ && !/EXECUTE|PREPARE|ANALYZE|analyze/ {
      qn++
      printf "\\echo ===Q%d===\n", qn
    }
    { print }
  ' "$1"
}

run_mode() {
  # $1 = preprocessed file, $2 = "pg" or "orca"
  local f=$1 mode=$2
  local setup
  if [ "$mode" = "pg" ]; then
    setup="SET pg_orca.enable_orca=off;"
  else
    setup="SET pg_orca.enable_orca=on; SET pg_orca.cost_model=pg;"
  fi
  # Prepend a tiny header file with the BEGIN + GUC setup; psql -f doesn't
  # accept multi-statement -c with backslash meta-commands.  The header runs
  # BEGIN so all CREATE/INSERT in $f get rolled back at the end.
  local header; header=$(mktemp /tmp/cra_head.XXXX.sql)
  cat >"$header" <<HDR
\\set ON_ERROR_STOP off
\\set abs_srcdir '/home/administrator/workspace/postgres/src/test/regress'
BEGIN;
SET LOCAL max_parallel_workers_per_gather=0;
$setup
HDR
  local trailer; trailer=$(mktemp /tmp/cra_tail.XXXX.sql)
  echo "ROLLBACK;" >"$trailer"
  timeout "$TIMEOUT" "$PSQL" -d "$PGDATABASE" -X -At \
    -f "$header" -f "$f" -f "$trailer" 2>&1
  rm -f "$header" "$trailer"
}

TOTAL=0; OK=0; OFF=0; DIFF=0; ERR=0
declare -A TF TO TFF TD TE

# Iterate over the modified files (skip run.sh).
mapfile -t FILES < <(ls "$SCRIPT_DIR"/*.sql 2>/dev/null)
for src in "${FILES[@]}"; do
  tname=$(basename "$src" .sql)
  tmp=$(mktemp /tmp/cra_${tname}.XXXX.sql)
  preprocess "$src" > "$tmp"

  pg_out=$(run_mode "$tmp" pg)
  orca_out=$(run_mode "$tmp" orca)
  rm -f "$tmp"

  max_q=$(echo "$pg_out" | grep -oE "===Q[0-9]+===" | sed -E 's/===Q([0-9]+)===/\1/' | sort -un | tail -1)
  [ -z "$max_q" ] && { printf "  %-25s  no-explain\n" "$tname"; continue; }

  ft=0; fo=0; ff=0; fd=0; fe=0
  for q in $(seq 1 "$max_q"); do
    pg_block=$(echo "$pg_out" | awk -v m="===Q${q}===" -v n="===Q$((q+1))===" '
      $0 == m { in_blk=1; next } $0 == n { in_blk=0 } in_blk')
    orca_block=$(echo "$orca_out" | awk -v m="===Q${q}===" -v n="===Q$((q+1))===" '
      $0 == m { in_blk=1; next } $0 == n { in_blk=0 } in_blk')

    ft=$((ft+1))
    pg_cost=$(echo "$pg_block" | grep -oE "cost=[0-9.]+\.\.[0-9.]+" | head -1)
    orca_cost=$(echo "$orca_block" | grep -oE "cost=[0-9.]+\.\.[0-9.]+" | head -1)
    if [ -z "$pg_cost" ] || [ -z "$orca_cost" ]; then
      fe=$((fe+1)); continue
    fi
    pg_shape=$(echo "$pg_block" | filter_plan)
    orca_shape=$(echo "$orca_block" | filter_plan)
    pgt=$(echo "$pg_cost" | sed -E 's/cost=[0-9.]+\.\.([0-9.]+)/\1/')
    orct=$(echo "$orca_cost" | sed -E 's/cost=[0-9.]+\.\.([0-9.]+)/\1/')
    dp=$(awk -v a="$pgt" -v b="$orct" 'BEGIN { if (a==0) print 0; else printf "%.1f", (b-a)/a*100 }')
    ap=$(echo "$dp" | tr -d '-')
    if [ "$pg_shape" = "$orca_shape" ]; then
      if awk -v p="$ap" -v t="$TOL" 'BEGIN { exit (p+0 <= t+0) ? 0 : 1 }'; then
        fo=$((fo+1))
      else
        ff=$((ff+1))
      fi
    else
      fd=$((fd+1))
    fi
  done

  TOTAL=$((TOTAL+ft)); OK=$((OK+fo)); OFF=$((OFF+ff)); DIFF=$((DIFF+fd)); ERR=$((ERR+fe))
  printf "  %-25s %4d total | OK %3d | off %2d | diff %3d | err %3d\n" \
    "$tname" "$ft" "$fo" "$ff" "$fd" "$fe"
done

echo "================================================================"
printf "TOTAL %d | OK %d | off %d | DIFF %d | err %d\n" \
  "$TOTAL" "$OK" "$OFF" "$DIFF" "$ERR"
[ "$((OK + OFF))" -gt 0 ] && awk -v ok="$OK" -v off="$OFF" \
  'BEGIN { printf "same-plan alignment rate: %.1f%%\n", ok / (ok + off) * 100 }'
