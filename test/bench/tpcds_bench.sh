#!/usr/bin/env bash
# TPC-DS benchmark: ORCA vs PG planner.
# Usage: tpcds_bench.sh [db [timeout_sec]]
#   db           target database (default: tpcds_sf1)
#   timeout_sec  per-query timeout in seconds (default: 120)
set -euo pipefail

DB=${1:-tpcds_sf1}
TIMEOUT=${2:-120}

PG_CONFIG=${PG_CONFIG:-/home/administrator/workspace/install/bin/pg_config}
PGBIN=$("$PG_CONFIG" --bindir)
PSQL="$PGBIN/psql -h /tmp -p 5432 -d $DB -X -A -t"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

OUTDIR="$SCRIPT_DIR/results"
mkdir -p "$OUTDIR"

ORCA_CSV="$OUTDIR/tpcds_orca.csv"
PG_CSV="$OUTDIR/tpcds_pg.csv"
COMPARE_CSV="$OUTDIR/tpcds_${DB}_compare.csv"

echo "=== TPC-DS Benchmark on '$DB' (timeout=${TIMEOUT}s) ==="
echo ""

# Idempotent setup: install extension and arm session_preload at the
# database level so every new connection opened below auto-loads pg_orca.
$PSQL -q -c "CREATE EXTENSION IF NOT EXISTS pg_orca;" >/dev/null 2>&1
$PSQL -q -c "ALTER DATABASE $DB SET session_preload_libraries = 'pg_orca';" >/dev/null \
    || { echo "ERROR: could not ALTER DATABASE $DB SET session_preload_libraries"; exit 1; }

# ── ANALYZE ────────────────────────────────────────────────────────────────
echo "[0/3] ANALYZE all tpcds tables (disable parallelism first)..."
$PSQL -q <<SQL
SET max_parallel_workers_per_gather = 0;
ANALYZE tpcds.store_sales, tpcds.catalog_sales, tpcds.web_sales,
        tpcds.store_returns, tpcds.catalog_returns, tpcds.web_returns,
        tpcds.inventory, tpcds.customer, tpcds.customer_address,
        tpcds.customer_demographics, tpcds.date_dim, tpcds.item,
        tpcds.store, tpcds.call_center, tpcds.catalog_page,
        tpcds.household_demographics, tpcds.income_band, tpcds.promotion,
        tpcds.reason, tpcds.ship_mode, tpcds.time_dim,
        tpcds.warehouse, tpcds.web_page, tpcds.web_site;
SQL
echo "ANALYZE done."
echo ""

# ── ORCA pass ──────────────────────────────────────────────────────────────
echo "[1/3] ORCA pass — enable_orca = on, parallelism off"
$PSQL 2>&1 <<SQL
SET pg_orca.enable_orca          = on;
SET max_parallel_workers_per_gather = 0;
SET jit                          = off;
SELECT tpcds.bench(timeout_sec := $TIMEOUT);
SQL

$PSQL -q -c \
    "COPY (SELECT query_id, status, duration_ms, rows_returned
           FROM tpcds.bench_summary ORDER BY query_id)
     TO STDOUT WITH (FORMAT csv, HEADER)" \
    > "$ORCA_CSV"
echo ""

# ── PG planner pass ────────────────────────────────────────────────────────
echo "[2/3] PG planner pass — enable_orca = off, parallelism off, timeout=${TIMEOUT}s"
python3 "$SCRIPT_DIR/tpcds_pg_pass.py" "$TIMEOUT" "$PG_CSV" "$DB"
echo ""

# ── Comparison report ──────────────────────────────────────────────────────
echo "[3/3] Generating comparison → $COMPARE_CSV"
python3 - "$ORCA_CSV" "$PG_CSV" "$COMPARE_CSV" <<'PYEOF'
import sys, csv, math

def read_csv(path):
    rows = {}
    with open(path) as f:
        for r in csv.DictReader(f):
            rows[int(r['query_id'])] = r
    return rows

orca_file, pg_file, out_file = sys.argv[1], sys.argv[2], sys.argv[3]
orca = read_csv(orca_file)
pg   = read_csv(pg_file)

def pg_status(s):
    if s == 'OK':          return 'OK'
    if 'cancel' in s.lower() or 'timeout' in s.lower(): return 'TIMEOUT'
    return 'ERROR'

rows_out = []
speedups = []
orca_total = pg_total = 0.0
pg_timeout_qs = []

for qid in range(1, 100):
    o = orca.get(qid, {})
    p = pg.get(qid, {})
    o_ms = float(o.get('duration_ms', 0))
    p_ms = float(p.get('duration_ms', 0))
    o_ok = o.get('status') == 'OK'
    p_st = pg_status(p.get('status', 'MISSING'))

    sp = ''
    if o_ok and p_st == 'OK' and o_ms > 0 and p_ms > 0:
        sp = round(p_ms / o_ms, 3)
        speedups.append(sp)
        orca_total += o_ms
        pg_total   += p_ms
    elif o_ok and p_st == 'TIMEOUT':
        pg_timeout_qs.append(qid)
        orca_total += o_ms

    rows_out.append({
        'query': qid,
        'orca_ms': round(o_ms, 1),
        'pg_ms':   round(p_ms, 1) if p_st == 'OK' else p_st,
        'speedup': sp,
        'orca_status': 'OK' if o_ok else 'ERROR',
        'pg_status':   p_st,
    })

with open(out_file, 'w', newline='') as f:
    w = csv.DictWriter(f, fieldnames=['query','orca_ms','pg_ms','speedup','orca_status','pg_status'])
    w.writeheader()
    w.writerows(rows_out)

gm = math.exp(sum(math.log(s) for s in speedups) / len(speedups)) if speedups else 0
med = sorted(speedups)[len(speedups)//2] if speedups else 0

print(f"\n{'='*65}")
print(f"TPC-DS Results  (ORCA bench plan+exec / PG EXPLAIN exec-only)")
print(f"{'='*65}")
orca_ok = sum(1 for r in orca.values() if r.get('status') == 'OK')
pg_ok   = sum(1 for p in pg.values() if pg_status(p.get('status','')) == 'OK')
print(f"  ORCA: {orca_ok}/99 OK   total = {orca_total/1000:.1f}s")
print(f"  PG:   {pg_ok}/99 OK   {len(pg_timeout_qs)} TIMEOUT ({' '.join(f'Q{q:02d}' for q in pg_timeout_qs)})")
print(f"  Comparable (both OK): {len(speedups)}")
if speedups:
    print(f"    Geomean speedup:  {gm:.2f}x   (>1 = ORCA faster)")
    print(f"    Median speedup:   {med:.2f}x")
    print(f"    ORCA faster >5%: {sum(1 for s in speedups if s>1.05)} queries")
    print(f"    ORCA slower >5%: {sum(1 for s in speedups if s<0.95)} queries")
print(f"{'='*65}")

print(f"\n{'Q':>4}  {'ORCA(ms)':>10}  {'PG':>12}  {'Speedup':>8}")
print("-" * 42)
for r in rows_out:
    sp = f"{r['speedup']:.2f}x" if isinstance(r['speedup'], float) else ''
    flag = '  <<< PG TIMEOUT' if r['pg_status'] == 'TIMEOUT' else ''
    pg_str = str(r['pg_ms']) if r['pg_status'] == 'OK' else r['pg_status']
    print(f"{r['query']:>4}  {r['orca_ms']:>10.1f}  {pg_str:>12}  {sp:>8}{flag}")
PYEOF

echo ""
echo "Done. Results:"
echo "  ORCA:    $ORCA_CSV"
echo "  PG:      $PG_CSV"
echo "  Compare: $COMPARE_CSV"
