#!/usr/bin/env bash
set -euo pipefail

# P1-001-D: exercise a real Daft two-source HashJoin under bounded queues,
# repeated execution and cancellation. This is an incremental runtime gate;
# it never rebuilds PostgreSQL, pg_orca or Daft.

PGD_WORKSPACE=${PGD_WORKSPACE:-/home/qiumc/data/code/cpp}
PGD_INSTALL_ROOT=${PGD_INSTALL_ROOT:-$PGD_WORKSPACE/.install/pg19-pgorca-daft}
PGD_INTEGRATION_ROOT=${PGD_INTEGRATION_ROOT:-$PGD_WORKSPACE/integration/pg19-pgorca-daft}
PGD_REPORTS_DIR=${PGD_REPORTS_DIR:-$PGD_INTEGRATION_ROOT/reports}
PGD_HOST=${PGHOST:-127.0.0.1}
PGD_PORT=${PGPORT:-${PGD_PORT:-55419}}
PGD_DATABASE=${PGDATABASE:-postgres}
PGD_REPEAT_COUNT=${PGD_REPEAT_COUNT:-20}
PGD_RSS_GROWTH_LIMIT_KB=${PGD_RSS_GROWTH_LIMIT_KB:-65536}
PGD_PSQL=$PGD_INSTALL_ROOT/bin/psql
PGD_REPEAT_CLIENT_PID=
PGD_CANCEL_CLIENT_PID=
PGD_DISCONNECT_CLIENT_PID=
PGD_TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/pgd-p1-join-stress.XXXXXX")

if [[ ! -x "$PGD_PSQL" ]]; then
    echo "psql not found: $PGD_PSQL" >&2
    exit 2
fi

psql_cmd=("$PGD_PSQL" -X -q -v ON_ERROR_STOP=1 -h "$PGD_HOST" -p "$PGD_PORT" -d "$PGD_DATABASE")

cleanup() {
    if [[ -n "$PGD_REPEAT_CLIENT_PID" ]]; then
        kill "$PGD_REPEAT_CLIENT_PID" >/dev/null 2>&1 || true
        wait "$PGD_REPEAT_CLIENT_PID" >/dev/null 2>&1 || true
    fi
    if [[ -n "$PGD_CANCEL_CLIENT_PID" ]]; then
        kill "$PGD_CANCEL_CLIENT_PID" >/dev/null 2>&1 || true
        wait "$PGD_CANCEL_CLIENT_PID" >/dev/null 2>&1 || true
    fi
    if [[ -n "$PGD_DISCONNECT_CLIENT_PID" ]]; then
        kill "$PGD_DISCONNECT_CLIENT_PID" >/dev/null 2>&1 || true
        wait "$PGD_DISCONNECT_CLIENT_PID" >/dev/null 2>&1 || true
    fi
    "${psql_cmd[@]}" >/dev/null 2>&1 <<'SQL' || true
DROP FUNCTION IF EXISTS pgd_p1_join_slow_int(integer);
DROP TABLE IF EXISTS pgd_p1_join_stress_r;
DROP TABLE IF EXISTS pgd_p1_join_stress_l;
SQL
    case "$PGD_TMP_DIR" in
        "${TMPDIR:-/tmp}"/pgd-p1-join-stress.*)
            cmake -E remove_directory "$PGD_TMP_DIR"
            ;;
        *)
            echo "refusing to remove unexpected temporary path: $PGD_TMP_DIR" >&2
            ;;
    esac
}
trap cleanup EXIT

wait_for_application() {
    local application_name=$1
    local attempts=${2:-300}
    local pid=
    for ((i = 0; i < attempts; i++)); do
        pid=$("${psql_cmd[@]}" -Atc \
            "SELECT pid FROM pg_stat_activity WHERE application_name = '$application_name' ORDER BY pid LIMIT 1" || true)
        if [[ "$pid" =~ ^[0-9]+$ ]]; then
            printf '%s\n' "$pid"
            return 0
        fi
        sleep 0.1
    done
    echo "timed out waiting for application_name=$application_name" >&2
    return 1
}

wait_for_active_application() {
    local application_name=$1
    local attempts=${2:-300}
    local pid=
    for ((i = 0; i < attempts; i++)); do
        pid=$("${psql_cmd[@]}" -Atc \
            "SELECT pid FROM pg_stat_activity WHERE application_name = '$application_name' AND state = 'active' ORDER BY pid LIMIT 1" || true)
        if [[ "$pid" =~ ^[0-9]+$ ]]; then
            printf '%s\n' "$pid"
            return 0
        fi
        sleep 0.1
    done
    echo "timed out waiting for active fault-injection client" >&2
    return 1
}

backend_rss_kb() {
    local pid=$1
    awk '/^VmRSS:/ {print $2; found=1} END {if (!found) exit 1}' "/proc/$pid/status"
}

"${psql_cmd[@]}" <<'SQL'
DROP FUNCTION IF EXISTS pgd_p1_join_slow_int(integer);
DROP TABLE IF EXISTS pgd_p1_join_stress_r;
DROP TABLE IF EXISTS pgd_p1_join_stress_l;
CREATE TABLE pgd_p1_join_stress_l(lid integer, k integer, payload integer);
CREATE TABLE pgd_p1_join_stress_r(rid integer, k integer, payload integer);
SET default_statistics_target=10000;
INSERT INTO pgd_p1_join_stress_l
SELECT i, CASE WHEN i % 17 = 0 THEN NULL ELSE i % 2000 END, i * 3
FROM generate_series(1, 30000) AS g(i);
INSERT INTO pgd_p1_join_stress_r
SELECT i, CASE WHEN i % 19 = 0 THEN NULL ELSE i % 2000 END, i * 5
FROM generate_series(1, 8000) AS g(i);
ANALYZE pgd_p1_join_stress_l;
ANALYZE pgd_p1_join_stress_r;
CREATE FUNCTION pgd_p1_join_slow_int(value integer)
RETURNS integer
LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE
AS $$
BEGIN
    PERFORM pg_sleep(0.0002);
    RETURN value;
END
$$;
SQL

expected_rows=$("${psql_cmd[@]}" -Atc \
    "SET pg_orca.enable_orca=on; SET pg_orca.daft_enable='off'; SELECT count(l.lid + r.rid) FROM pgd_p1_join_stress_l l JOIN pgd_p1_join_stress_r r ON l.k=r.k" | tail -n 1)
if [[ ! "$expected_rows" =~ ^[0-9]+$ ]] || [[ "$expected_rows" -le 0 ]]; then
    echo "invalid PostgreSQL baseline row count: $expected_rows" >&2
    exit 1
fi

daft_settings="SET pg_orca.enable_orca=on; SET pg_orca.daft_enable='force'; SET pg_orca.daft_prefer_plan=on; SET pg_orca.daft_trace=on; SET pg_orca.daft_cost_calibration_profile='test_prefer_daft_v1'; SET pg_orca.cost_model='pg'; SET effective_cache_size='4GB'; SET work_mem='4MB'; SET hash_mem_multiplier=2.0;"
join_query="SELECT count(l.lid + r.rid) FROM pgd_p1_join_stress_l l JOIN pgd_p1_join_stress_r r ON l.k=r.k;"

plan=$("${psql_cmd[@]}" -Atc "$daft_settings EXPLAIN (COSTS OFF) SELECT l.lid, r.rid FROM pgd_p1_join_stress_l l JOIN pgd_p1_join_stress_r r ON l.k=r.k")
if [[ "$plan" != *"Custom Scan (DaftExec)"* ]] || [[ "$plan" != *"PG Sources: 2"* ]]; then
    echo "stress query did not select a two-source DaftExec plan" >&2
    printf '%s\n' "$plan" >&2
    exit 1
fi

# Warm one persistent backend, measure its RSS, execute the same two-source
# Daft fragment repeatedly, then measure the same process again.
{
    printf '%s\n' "$daft_settings"
    printf '%s\n' "$join_query"
    printf '%s\n' "SET application_name='pgd_p1_join_memory_baseline'; SELECT pg_sleep(3);"
    for ((i = 0; i < PGD_REPEAT_COUNT; i++)); do
        printf '%s\n' "$join_query"
    done
    printf '%s\n' "SET application_name='pgd_p1_join_memory_final'; SELECT pg_sleep(3);"
} | "${psql_cmd[@]}" -At >"$PGD_TMP_DIR/repeat.out" 2>"$PGD_TMP_DIR/repeat.err" &
PGD_REPEAT_CLIENT_PID=$!

repeat_backend_pid=$(wait_for_application pgd_p1_join_memory_baseline)
rss_before_kb=$(backend_rss_kb "$repeat_backend_pid")
final_backend_pid=$(wait_for_application pgd_p1_join_memory_final 1200)
if [[ "$final_backend_pid" != "$repeat_backend_pid" ]]; then
    echo "repeat workload changed backend pid" >&2
    exit 1
fi
rss_after_kb=$(backend_rss_kb "$repeat_backend_pid")
if ! wait "$PGD_REPEAT_CLIENT_PID"; then
    cat "$PGD_TMP_DIR/repeat.err" >&2
    exit 1
fi
PGD_REPEAT_CLIENT_PID=

observed_results=$(awk -v expected="$expected_rows" '$0 == expected {count++} END {print count + 0}' "$PGD_TMP_DIR/repeat.out")
expected_results=$((PGD_REPEAT_COUNT + 1))
if [[ "$observed_results" -ne "$expected_results" ]]; then
    echo "repeat result count mismatch: expected=$expected_results observed=$observed_results" >&2
    cat "$PGD_TMP_DIR/repeat.out" >&2
    exit 1
fi
rss_growth_kb=$((rss_after_kb - rss_before_kb))
if [[ "$rss_growth_kb" -gt "$PGD_RSS_GROWTH_LIMIT_KB" ]]; then
    echo "backend RSS grew too much: before=${rss_before_kb}kB after=${rss_after_kb}kB growth=${rss_growth_kb}kB" >&2
    exit 1
fi

# Keep the cost profile's frozen bounded queue depth of 2 and put a slow PG
# consumer above DaftExec. EXPLAIN ANALYZE must finish and expose a real,
# non-zero backpressure wait.
backpressure_plan=$(timeout 45s "${psql_cmd[@]}" -Atc \
    "$daft_settings EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF) SELECT pgd_p1_join_slow_int(l.lid) FROM pgd_p1_join_stress_l l JOIN pgd_p1_join_stress_r r ON l.k=r.k LIMIT 20000")
if [[ "$backpressure_plan" != *"Custom Scan (DaftExec)"* ]]; then
    echo "backpressure workload did not execute DaftExec" >&2
    exit 1
fi
backpressure_wait_ms=$(printf '%s\n' "$backpressure_plan" | awk -F': ' '/Backpressure Wait ms:/ {print $2; exit}')
if [[ -z "$backpressure_wait_ms" ]] || ! awk -v value="$backpressure_wait_ms" 'BEGIN {exit !(value + 0 > 0)}'; then
    echo "backpressure workload did not report a positive wait: ${backpressure_wait_ms:-missing}" >&2
    exit 1
fi

# Run the same slow-consumer shape without LIMIT, cancel the backend, require
# prompt termination, and then prove that a fresh Daft query still succeeds.
{
    printf '%s\n' "$daft_settings"
    printf '%s\n' "SET application_name='pgd_p1_join_cancel'; SET statement_timeout='60s';"
    printf '%s\n' "SELECT sum(pgd_p1_join_slow_int(l.lid)::bigint) FROM pgd_p1_join_stress_l l JOIN pgd_p1_join_stress_r r ON l.k=r.k;"
} | "${psql_cmd[@]}" -At >"$PGD_TMP_DIR/cancel.out" 2>"$PGD_TMP_DIR/cancel.err" &
PGD_CANCEL_CLIENT_PID=$!
cancel_backend_pid=$(wait_for_application pgd_p1_join_cancel)
sleep 1
cancel_result=$("${psql_cmd[@]}" -Atc "SELECT pg_cancel_backend($cancel_backend_pid)")
if [[ "$cancel_result" != "t" ]]; then
    echo "pg_cancel_backend returned: $cancel_result" >&2
    exit 1
fi

cancel_exited=false
for ((i = 0; i < 150; i++)); do
    if ! kill -0 "$PGD_CANCEL_CLIENT_PID" >/dev/null 2>&1; then
        cancel_exited=true
        break
    fi
    sleep 0.1
done
if [[ "$cancel_exited" != true ]]; then
    echo "cancelled Daft Join did not terminate within 15 seconds" >&2
    exit 1
fi
set +e
wait "$PGD_CANCEL_CLIENT_PID"
cancel_status=$?
set -e
PGD_CANCEL_CLIENT_PID=
if [[ "$cancel_status" -eq 0 ]]; then
    echo "cancelled query unexpectedly succeeded" >&2
    exit 1
fi
if ! awk '/canceling statement due to user request/ {found=1} END {exit !found}' "$PGD_TMP_DIR/cancel.err"; then
    echo "cancelled query did not return PostgreSQL cancellation" >&2
    cat "$PGD_TMP_DIR/cancel.err" >&2
    exit 1
fi

recovery_rows=$(timeout 30s "${psql_cmd[@]}" -Atc "$daft_settings $join_query" | tail -n 1)
if [[ "$recovery_rows" != "$expected_rows" ]]; then
    echo "post-cancel recovery mismatch: expected=$expected_rows actual=$recovery_rows" >&2
    exit 1
fi

# A slow PG consumer above a real Daft two-source fragment keeps the backend
# active long enough to close the client socket. The backend must observe the
# disconnect, tear down the Bridge handle, and disappear without postmaster
# failure or a leaked active query.
{
    printf '%s\n' "$daft_settings"
    printf '%s\n' "SET application_name='pgd_p1_join_disconnect'; SET statement_timeout='60s';"
    printf '%s\n' "SELECT pgd_p1_join_slow_int(l.lid) FROM pgd_p1_join_stress_l l JOIN pgd_p1_join_stress_r r ON l.k=r.k;"
} | "${psql_cmd[@]}" -At >"$PGD_TMP_DIR/disconnect.out" 2>"$PGD_TMP_DIR/disconnect.err" &
PGD_DISCONNECT_CLIENT_PID=$!
disconnect_backend_pid=$(wait_for_active_application pgd_p1_join_disconnect)
kill "$PGD_DISCONNECT_CLIENT_PID"
set +e
wait "$PGD_DISCONNECT_CLIENT_PID"
set -e
PGD_DISCONNECT_CLIENT_PID=

disconnect_cleaned=false
for ((i = 0; i < 150; i++)); do
    remaining=$("${psql_cmd[@]}" -Atc \
        "SELECT count(*) FROM pg_stat_activity WHERE pid = $disconnect_backend_pid" || true)
    if [[ "$remaining" == "0" ]]; then
        disconnect_cleaned=true
        break
    fi
    sleep 0.1
done
if [[ "$disconnect_cleaned" != true ]]; then
    echo "disconnected Daft query backend did not terminate within 15 seconds" >&2
    exit 1
fi

disconnect_recovery_rows=$(timeout 30s "${psql_cmd[@]}" -Atc "$daft_settings $join_query" | tail -n 1)
if [[ "$disconnect_recovery_rows" != "$expected_rows" ]]; then
    echo "post-disconnect recovery result mismatch" >&2
    exit 1
fi

mkdir -p "$PGD_REPORTS_DIR"
{
    echo "report_version: 1"
    echo "status: PASS"
    echo "two_source_join_differential: PASS"
    echo "repeat_execution_cases: $PGD_REPEAT_COUNT"
    echo "rss_growth_kb: $rss_growth_kb"
    echo "rss_growth_limit_kb: $PGD_RSS_GROWTH_LIMIT_KB"
    echo "bounded_backpressure: PASS"
    echo "statement_cancel_cleanup: PASS"
    echo "client_disconnect_cleanup: PASS"
    echo "post_fault_recovery: PASS"
    echo "result_differences: 0"
    echo "skipped_cases: 0"
} >"$PGD_REPORTS_DIR/p1_fault_report.txt"

echo "P1 Join stress PASS"
echo "P1 fault matrix PASS (cancel, disconnect, recovery, RSS, backpressure)"
