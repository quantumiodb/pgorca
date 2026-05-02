#!/usr/bin/env bash
# TPC-H benchmark: ORCA vs PG planner.
# Usage: tpch_bench.sh <db> <runs>
#   db    target database (must already have pg_tpch + data loaded)
#   runs  number of iterations per query per planner (median is reported)
set -euo pipefail

DB=${1:-tpch_sf10}
RUNS=${2:-3}

PG_CONFIG=${PG_CONFIG:-/home/administrator/workspace/install/bin/pg_config}
PGBIN=$("$PG_CONFIG" --bindir)
PSQL="$PGBIN/psql -h /tmp -p 5432 -d $DB -X -q -A -t -v ON_ERROR_STOP=1"

OUTDIR="$(dirname "$0")/results"
mkdir -p "$OUTDIR"

# Force pg_orca to be loaded (it is preloaded if shared_preload_libraries is set;
# otherwise LOAD via session-level LOAD).
$PSQL -c "LOAD 'pg_orca';" >/dev/null

# Pull all 22 query texts into per-file copies once.
QDIR="$OUTDIR/queries-$DB"
mkdir -p "$QDIR"
for q in $(seq 1 22); do
  $PSQL -c "SELECT query FROM tpch_queries($q);" > "$QDIR/q$q.sql"
done

# Run a single query with EXPLAIN ANALYZE, capture execution time only.
# We use EXPLAIN (ANALYZE, TIMING OFF, BUFFERS OFF) and parse "Execution Time".
run_one() {
    local mode=$1   # "orca" or "pg"
    local qfile=$2
    local set_orca
    if [[ "$mode" == "orca" ]]; then
        set_orca="set pg_orca.enable_orca = on;"
    else
        set_orca="set pg_orca.enable_orca = off;"
    fi
    local sql
    sql=$(cat "$qfile")
    # Wrap in EXPLAIN ANALYZE so we get authoritative server-side timing
    # (psql \timing includes round-trip and result formatting; for queries that
    # return many rows that overhead is non-trivial).
    $PSQL <<SQL 2>&1
LOAD 'pg_orca';
set max_parallel_workers_per_gather = 0;
set statement_timeout = '900s';
$set_orca
EXPLAIN (ANALYZE, TIMING OFF, BUFFERS OFF, SUMMARY ON, COSTS OFF)
$sql
SQL
}

# Extract execution time in ms from EXPLAIN ANALYZE output.
extract_ms() {
    # Match "Execution Time: 1234.567 ms"
    grep -oE 'Execution Time: [0-9.]+ ms' | tail -1 | awk '{print $3}'
}

declare -A T_ORCA T_PG
declare -A FALLBACK

CSV="$OUTDIR/tpch_${DB}.csv"
echo "query,orca_ms,pg_ms,speedup,fallback" > "$CSV"

for q in $(seq 1 22); do
    qfile="$QDIR/q$q.sql"
    # Warm cache once (ORCA on)
    run_one orca "$qfile" > "$OUTDIR/_warm.log" 2>&1 || true

    orca_times=()
    pg_times=()
    fb=0
    for r in $(seq 1 "$RUNS"); do
        out_orca=$(run_one orca "$qfile" || true)
        if echo "$out_orca" | grep -qi "Falling back to Postgres"; then
            fb=1
        fi
        ms=$(echo "$out_orca" | extract_ms || true)
        if [[ -n "${ms:-}" ]]; then orca_times+=("$ms"); fi

        out_pg=$(run_one pg "$qfile" || true)
        ms=$(echo "$out_pg" | extract_ms || true)
        if [[ -n "${ms:-}" ]]; then pg_times+=("$ms"); fi
    done

    # Median (using sort -n + middle index). For 3 runs, that's the 2nd value.
    median() {
        printf '%s\n' "$@" | sort -n | awk -v n=$# 'NR == int((n+1)/2)'
    }

    if [[ ${#orca_times[@]} -eq 0 ]]; then o_med=NA; else o_med=$(median "${orca_times[@]}"); fi
    if [[ ${#pg_times[@]}   -eq 0 ]]; then p_med=NA; else p_med=$(median "${pg_times[@]}"); fi

    if [[ "$o_med" != NA && "$p_med" != NA ]]; then
        speedup=$(awk -v o="$o_med" -v p="$p_med" 'BEGIN{printf "%.2f", p/o}')
    else
        speedup=NA
    fi
    printf "Q%-2d  orca=%10s ms  pg=%10s ms  speedup=%6s  fallback=%d\n" \
        "$q" "$o_med" "$p_med" "$speedup" "$fb"
    echo "$q,$o_med,$p_med,$speedup,$fb" >> "$CSV"
done

echo
echo "Results CSV: $CSV"
