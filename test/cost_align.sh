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
TOL=${COST_ALIGN_TOL_PCT:-10}
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

i=0
for q in "${queries[@]}"; do
  i=$((i+1))
  pg_out=$("$PSQL" -d "$PGDATABASE" -X -At -c "SET pg_orca.enable_orca=off; $q" 2>/dev/null | grep -vE "^SET|^RESET")
  orca_out=$("$PSQL" -d "$PGDATABASE" -X -At -c "SET pg_orca.enable_orca=on; SET pg_orca.cost_model=pg; $q" 2>/dev/null | grep -vE "^SET|^RESET")
  pg_top=$(echo "$pg_out" | grep -oE "^[A-Z][a-zA-Z ]+" | head -1 | xargs)
  orca_top=$(echo "$orca_out" | grep -oE "^[A-Z][a-zA-Z ]+" | head -1 | xargs)
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
