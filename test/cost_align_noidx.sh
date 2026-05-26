#!/bin/bash
# Re-run cost_align with enable_indexscan/indexonlyscan/bitmapscan=off
# to force both PG and ORCA into the same plan space.
set -u
PGHOST=/tmp
PGDATABASE=postgres
PSQL=/home/administrator/workspace/install/bin/psql
SQL_FILE=/home/administrator/workspace/pgorca/test/sql/cost_align.sql
TOL=${1:-2}
export PGHOST PGDATABASE

# Setup (everything before first EXPLAIN)
setup=$(awk '/^EXPLAIN/{exit} {print}' "$SQL_FILE")
"$PSQL" -X -At >/dev/null 2>&1 <<<"$setup"

# Install pg_orca and arm session_preload at the database level so each
# per-query psql invocation below auto-loads pg_orca.
"$PSQL" -X -c "CREATE EXTENSION IF NOT EXISTS pg_orca;" >/dev/null 2>&1
"$PSQL" -X -c "ALTER DATABASE \"$PGDATABASE\" SET session_preload_libraries = 'pg_orca';" >/dev/null

DISABLE='SET enable_indexscan=off; SET enable_indexonlyscan=off; SET enable_bitmapscan=off;'

mapfile -t queries < <(grep -E "^EXPLAIN " "$SQL_FILE")

same_ok=0; same_off=0; diff_plan=0; total=0; fail=0
printf "%-3s | %-22s | %-22s | %-7s | %s\n" "#" "PG native" "ORCA pg" "diff%" "Query"
echo "----+------------------------+------------------------+---------+----------"
i=0
for q in "${queries[@]}"; do
  i=$((i+1))
  pg_out=$("$PSQL" -X -At -c "$DISABLE SET pg_orca.enable_orca=off; $q" 2>/dev/null | grep -vE "^SET|^RESET")
  orca_out=$("$PSQL" -X -At -c "$DISABLE SET pg_orca.enable_orca=on; SET pg_orca.cost_model=pg; $q" 2>/dev/null | grep -vE "^SET|^RESET")

  filter_plan() {
    sed -E 's/[[:space:]]*\(cost.*//' \
    | sed -E 's/^[[:space:]]*->[[:space:]]*//; s/^[[:space:]]+//' \
    | grep -E "^[A-Z]" \
    | grep -vE "^(Optimizer|Output|Index Cond|Recheck Cond|Sort Key|Group Key|Hash Cond|Merge Cond|Filter|Join Filter|One-Time Filter|Subplans Removed|Workers|Heap Fetches)" \
    | sed -E 's/[[:space:]]+(using|on)[[:space:]]+[A-Za-z_][A-Za-z_0-9.]*//g; s/[[:space:]]+$//' \
    | xargs
  }
  pg_top=$(echo "$pg_out" | filter_plan)
  orca_top=$(echo "$orca_out" | filter_plan)
  pg_cost=$(echo "$pg_out" | grep -oE "cost=[0-9.]+\.\.[0-9.]+" | head -1)
  orca_cost=$(echo "$orca_out" | grep -oE "cost=[0-9.]+\.\.[0-9.]+" | head -1)
  if [ -z "$pg_cost" ] || [ -z "$orca_cost" ]; then continue; fi

  total=$((total+1))
  pgt=$(echo "$pg_cost" | sed -E 's/cost=[0-9.]+\.\.([0-9.]+)/\1/')
  orct=$(echo "$orca_cost" | sed -E 's/cost=[0-9.]+\.\.([0-9.]+)/\1/')
  diff_pct=$(awk -v a="$pgt" -v b="$orct" 'BEGIN { if (a==0) print 0; else printf "%.1f", (b-a)/a*100 }')
  abs_pct=$(echo "$diff_pct" | tr -d '-')

  mark=""
  if [ "$pg_top" = "$orca_top" ]; then
    if awk -v p="$abs_pct" -v t="$TOL" 'BEGIN { exit (p+0 <= t+0) ? 0 : 1 }'; then
      same_ok=$((same_ok+1)); mark="OK"
    else
      same_off=$((same_off+1)); mark="FAIL"; fail=$((fail+1))
    fi
  else
    diff_plan=$((diff_plan+1)); mark="DIFF"
  fi
  display=$(echo "$q" | sed -E 's/^EXPLAIN //' | cut -c1-40)
  printf "%-3d | %-22s | %-22s | %-7s | %-4s %s\n" "$i" "$pg_cost" "$orca_cost" "$diff_pct" "$mark" "$display"
done
echo "----+------------------------+------------------------+---------+----------"
echo "Total=$total | same-plan ≤${TOL}%=$same_ok | same-plan off=$same_off | diff plan=$diff_plan"
