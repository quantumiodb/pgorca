#!/usr/bin/env bash
set -euo pipefail

# P1-006-A: database-semantics differential matrix. The advertised operator
# SQL regressions run immediately before this script; this matrix concentrates
# on snapshots, security, cached plans, invalidation, and fail-closed fallback.

PGD_WORKSPACE=${PGD_WORKSPACE:-/home/qiumc/data/code/cpp}
PGD_INSTALL_ROOT=${PGD_INSTALL_ROOT:-$PGD_WORKSPACE/.install/pg19-pgorca-daft}
PGD_INTEGRATION_ROOT=${PGD_INTEGRATION_ROOT:-$PGD_WORKSPACE/integration/pg19-pgorca-daft}
PGD_REPORTS_DIR=${PGD_REPORTS_DIR:-$PGD_INTEGRATION_ROOT/reports}
PGD_HOST=${PGHOST:-127.0.0.1}
PGD_PORT=${PGPORT:-${PGD_PORT:-55419}}
PGD_DATABASE=${PGDATABASE:-postgres}
PGD_PSQL=$PGD_INSTALL_ROOT/bin/psql
PGD_CAPABILITY_SPEC=$PGD_WORKSPACE/pg_orca/spec/pg_daft_capabilities.yaml
PGD_TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/pgd-p1-correctness.XXXXXX")
PGD_ACTIVE_CLIENT_PID=

if [[ ! -x "$PGD_PSQL" ]]; then
    echo "P1 correctness matrix requires the installed psql" >&2
    exit 2
fi
if [[ ! -f "$PGD_CAPABILITY_SPEC" ]]; then
    echo "P1 correctness matrix requires the canonical capability registry" >&2
    exit 2
fi

psql_cmd=(
    "$PGD_PSQL" -X -qAt -v ON_ERROR_STOP=1
    -h "$PGD_HOST" -p "$PGD_PORT" -d "$PGD_DATABASE"
)

cleanup() {
    if [[ -n "$PGD_ACTIVE_CLIENT_PID" ]]; then
        kill "$PGD_ACTIVE_CLIENT_PID" >/dev/null 2>&1 || true
        wait "$PGD_ACTIVE_CLIENT_PID" >/dev/null 2>&1 || true
        PGD_ACTIVE_CLIENT_PID=
    fi
    "${psql_cmd[@]}" >/dev/null 2>&1 <<'SQL' || true
DROP SCHEMA IF EXISTS pgd_p1_correctness CASCADE;
DROP ROLE IF EXISTS pgd_p1_tenant_a;
DROP ROLE IF EXISTS pgd_p1_tenant_b;
SQL
    case "$PGD_TMP_DIR" in
        "${TMPDIR:-/tmp}"/pgd-p1-correctness.*)
            cmake -E remove_directory "$PGD_TMP_DIR"
            ;;
        *)
            echo "refusing to remove unexpected correctness temp directory" >&2
            ;;
    esac
}
trap cleanup EXIT

PGD_COMMON_SQL="
SET client_min_messages = warning;
SET pg_orca.enable_orca = on;
SET pg_orca.daft_enable = 'force';
SET pg_orca.daft_prefer_plan = on;
SET pg_orca.daft_trace = on;
SET pg_orca.daft_cost_calibration_profile = 'test_prefer_daft_v1';
SET pg_orca.cost_model = 'pg';
SET work_mem = '4MB';
SET hash_mem_multiplier = 2.0;
SET effective_cache_size = '4GB';
SET pg_orca.daft_min_rows = 0;
SET pg_orca.daft_min_cost = 0;
"

psql_at() {
    "${psql_cmd[@]}" -c "$1"
}

fail() {
    echo "P1 correctness matrix failed: $1" >&2
    exit 1
}

assert_equal() {
    local left=$1
    local right=$2
    local label=$3
    [[ "$left" == "$right" ]] || fail "$label result mismatch"
}

result_count() {
    printf '%s\n' "$1" | cut -d '|' -f 1
}

tagged_value() {
    local file=$1
    local tag=$2
    awk -F '|' -v tag="$tag" '
        $1 == tag {
            $1 = ""
            sub(/^\|/, "")
            print
            found++
        }
        END { if (found != 1) exit 1 }
    ' OFS='|' "$file"
}

wait_for_sleep() {
    local application_name=$1
    local pid=
    for ((attempt = 0; attempt < 200; attempt++)); do
        pid=$(psql_at "SELECT pid FROM pg_stat_activity WHERE application_name = '$application_name' AND state = 'active' AND query LIKE '%pg_sleep%' ORDER BY pid LIMIT 1" || true)
        if [[ "$pid" =~ ^[0-9]+$ ]]; then
            printf '%s\n' "$pid"
            return 0
        fi
        sleep 0.05
    done
    fail "timed out waiting for synchronized session"
}

wait_for_client() {
    local error_file=$1
    if ! wait "$PGD_ACTIVE_CLIENT_PID"; then
        PGD_ACTIVE_CLIENT_PID=
        fail "background database session failed; inspect taskctl verify log"
    fi
    PGD_ACTIVE_CLIENT_PID=
    if grep -Eq '^(ERROR|FATAL|PANIC):' "$error_file"; then
        fail "background database session emitted a server error"
    fi
}

aggregate_sql="SELECT count(*), sum(amount), min(amount), max(amount) FROM pgd_p1_correctness.capability_rows"

# Start from fresh, fixed test objects in the dedicated integration cluster.
"${psql_cmd[@]}" >/dev/null <<'SQL'
CREATE EXTENSION IF NOT EXISTS pg_orca;
DROP SCHEMA IF EXISTS pgd_p1_correctness CASCADE;
DROP ROLE IF EXISTS pgd_p1_tenant_a;
DROP ROLE IF EXISTS pgd_p1_tenant_b;
CREATE ROLE pgd_p1_tenant_a NOLOGIN;
CREATE ROLE pgd_p1_tenant_b NOLOGIN;
CREATE SCHEMA pgd_p1_correctness;
CREATE TABLE pgd_p1_correctness.capability_rows(
    id int4 NOT NULL,
    small_value int2,
    amount int4
);
INSERT INTO pgd_p1_correctness.capability_rows
SELECT i,
       CASE WHEN i % 7 = 0 THEN NULL ELSE (i % 100)::int2 END,
       (i % 101) - 50
FROM generate_series(1, 100000) AS series(i);
SET default_statistics_target = 10000;
ANALYZE pgd_p1_correctness.capability_rows;
SQL

# Prove the test profile selects a real advertised HashAggregate, then compare
# the exact scalar result against the PG executor baseline.
aggregate_plan=$(psql_at "$PGD_COMMON_SQL EXPLAIN (COSTS OFF) $aggregate_sql")
[[ "$aggregate_plan" == *"Custom Scan (DaftExec)"* ]] || fail "advertised aggregate did not select DaftExec"
[[ "$aggregate_plan" == *"Daft Operators: HashAggregate"* ]] || fail "advertised aggregate selected the wrong operator"
pg_initial=$(psql_at "$PGD_COMMON_SQL SET pg_orca.daft_enable = 'off'; $aggregate_sql")
daft_initial=$(psql_at "$PGD_COMMON_SQL $aggregate_sql")
assert_equal "$pg_initial" "$daft_initial" "advertised aggregate"

# Uncommitted data is invisible to both engines; after commit, both see the
# same new statement snapshot.
writer_out="$PGD_TMP_DIR/writer.out"
writer_err="$PGD_TMP_DIR/writer.err"
"${psql_cmd[@]}" >"$writer_out" 2>"$writer_err" <<'SQL' &
SET application_name = 'pgd_p1_uncommitted_writer';
BEGIN;
INSERT INTO pgd_p1_correctness.capability_rows VALUES (100001, 1, 7);
SELECT pg_sleep(3);
COMMIT;
SQL
PGD_ACTIVE_CLIENT_PID=$!
wait_for_sleep pgd_p1_uncommitted_writer >/dev/null
pg_uncommitted=$(psql_at "$PGD_COMMON_SQL SET pg_orca.daft_enable = 'off'; $aggregate_sql")
daft_uncommitted=$(psql_at "$PGD_COMMON_SQL $aggregate_sql")
assert_equal "$pg_initial" "$pg_uncommitted" "PG uncommitted visibility"
assert_equal "$pg_uncommitted" "$daft_uncommitted" "Daft uncommitted visibility"
wait_for_client "$writer_err"
pg_post_commit=$(psql_at "$PGD_COMMON_SQL SET pg_orca.daft_enable = 'off'; $aggregate_sql")
daft_post_commit=$(psql_at "$PGD_COMMON_SQL $aggregate_sql")
assert_equal "$pg_post_commit" "$daft_post_commit" "read committed post-commit"
[[ "$(result_count "$pg_post_commit")" -eq $(( $(result_count "$pg_initial") + 1 )) ]] || fail "committed row was not visible"

# REPEATABLE READ holds the PG snapshot while a concurrent transaction commits.
rr_out="$PGD_TMP_DIR/repeatable-read.out"
rr_err="$PGD_TMP_DIR/repeatable-read.err"
"${psql_cmd[@]}" >"$rr_out" 2>"$rr_err" <<SQL &
SET application_name = 'pgd_p1_repeatable_reader';
BEGIN ISOLATION LEVEL REPEATABLE READ;
$PGD_COMMON_SQL
SET pg_orca.daft_enable = 'off';
SELECT 'rr_before', count(*), sum(amount), min(amount), max(amount) FROM pgd_p1_correctness.capability_rows;
SELECT pg_sleep(3);
SET pg_orca.daft_enable = 'force';
SELECT 'rr_daft_after', count(*), sum(amount), min(amount), max(amount) FROM pgd_p1_correctness.capability_rows;
COMMIT;
SQL
PGD_ACTIVE_CLIENT_PID=$!
wait_for_sleep pgd_p1_repeatable_reader >/dev/null
psql_at "INSERT INTO pgd_p1_correctness.capability_rows VALUES (100002, 2, 9)" >/dev/null
wait_for_client "$rr_err"
rr_before=$(tagged_value "$rr_out" rr_before)
rr_daft_after=$(tagged_value "$rr_out" rr_daft_after)
assert_equal "$rr_before" "$rr_daft_after" "repeatable read snapshot"

# READ COMMITTED advances on the next statement, but PG and Daft still agree
# after the concurrent commit.
rc_out="$PGD_TMP_DIR/read-committed.out"
rc_err="$PGD_TMP_DIR/read-committed.err"
"${psql_cmd[@]}" >"$rc_out" 2>"$rc_err" <<SQL &
SET application_name = 'pgd_p1_read_committed_reader';
BEGIN ISOLATION LEVEL READ COMMITTED;
$PGD_COMMON_SQL
SET pg_orca.daft_enable = 'off';
SELECT 'rc_before', count(*), sum(amount), min(amount), max(amount) FROM pgd_p1_correctness.capability_rows;
SELECT pg_sleep(3);
SELECT 'rc_pg_after', count(*), sum(amount), min(amount), max(amount) FROM pgd_p1_correctness.capability_rows;
SET pg_orca.daft_enable = 'force';
SELECT 'rc_daft_after', count(*), sum(amount), min(amount), max(amount) FROM pgd_p1_correctness.capability_rows;
COMMIT;
SQL
PGD_ACTIVE_CLIENT_PID=$!
wait_for_sleep pgd_p1_read_committed_reader >/dev/null
psql_at "INSERT INTO pgd_p1_correctness.capability_rows VALUES (100003, 3, 11)" >/dev/null
wait_for_client "$rc_err"
rc_before=$(tagged_value "$rc_out" rc_before)
rc_pg_after=$(tagged_value "$rc_out" rc_pg_after)
rc_daft_after=$(tagged_value "$rc_out" rc_daft_after)
assert_equal "$rc_pg_after" "$rc_daft_after" "read committed engine differential"
[[ "$(result_count "$rc_pg_after")" -eq $(( $(result_count "$rc_before") + 1 )) ]] || fail "read committed snapshot did not advance"

# SERIALIZABLE uses the same PG-owned snapshot for the later Daft fragment.
serial_out="$PGD_TMP_DIR/serializable.out"
serial_err="$PGD_TMP_DIR/serializable.err"
"${psql_cmd[@]}" >"$serial_out" 2>"$serial_err" <<SQL &
SET application_name = 'pgd_p1_serializable_reader';
BEGIN ISOLATION LEVEL SERIALIZABLE;
$PGD_COMMON_SQL
SET pg_orca.daft_enable = 'off';
SELECT 'serial_before', count(*), sum(amount), min(amount), max(amount) FROM pgd_p1_correctness.capability_rows;
SELECT pg_sleep(3);
SET pg_orca.daft_enable = 'force';
SELECT 'serial_daft_after', count(*), sum(amount), min(amount), max(amount) FROM pgd_p1_correctness.capability_rows;
COMMIT;
SQL
PGD_ACTIVE_CLIENT_PID=$!
wait_for_sleep pgd_p1_serializable_reader >/dev/null
psql_at "INSERT INTO pgd_p1_correctness.capability_rows VALUES (100004, 4, 13)" >/dev/null
wait_for_client "$serial_err"
serial_before=$(tagged_value "$serial_out" serial_before)
serial_daft_after=$(tagged_value "$serial_out" serial_daft_after)
assert_equal "$serial_before" "$serial_daft_after" "serializable snapshot"

# RLS and column privileges remain PG-owned security boundaries. A Daft
# aggregate may sit above that boundary, but the policy qualifier must stay in
# the PG child before any Arrow batch is formed.
"${psql_cmd[@]}" >/dev/null <<'SQL'
CREATE TABLE pgd_p1_correctness.secure_rows(
    id int4 NOT NULL,
    tenant name NOT NULL,
    amount int4,
    protected_text text
);
INSERT INTO pgd_p1_correctness.secure_rows
SELECT i,
       CASE WHEN i % 2 = 0 THEN 'pgd_p1_tenant_a' ELSE 'pgd_p1_tenant_b' END,
       (i % 101) - 50,
       'protected'
FROM generate_series(1, 12000) AS series(i);
ALTER TABLE pgd_p1_correctness.secure_rows ENABLE ROW LEVEL SECURITY;
CREATE POLICY pgd_p1_tenant_policy ON pgd_p1_correctness.secure_rows
    USING (tenant = current_user);
GRANT USAGE ON SCHEMA pgd_p1_correctness TO pgd_p1_tenant_a, pgd_p1_tenant_b;
GRANT SELECT(id, tenant, amount) ON pgd_p1_correctness.secure_rows
    TO pgd_p1_tenant_a, pgd_p1_tenant_b;
SET default_statistics_target = 10000;
ANALYZE pgd_p1_correctness.secure_rows;
CREATE FUNCTION pgd_p1_correctness.security_definer_count()
RETURNS bigint
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pgd_p1_correctness
AS 'SELECT count(*) FROM pgd_p1_correctness.secure_rows';
REVOKE ALL ON FUNCTION pgd_p1_correctness.security_definer_count() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgd_p1_correctness.security_definer_count() TO pgd_p1_tenant_a;
SQL

rls_query="SELECT count(*), sum(amount), min(amount), max(amount) FROM pgd_p1_correctness.secure_rows"
rls_plan=$(psql_at "$PGD_COMMON_SQL SET ROLE pgd_p1_tenant_a; EXPLAIN (COSTS OFF) $rls_query")
[[ "$rls_plan" == *"CURRENT_USER"* ]] || fail "RLS plan did not retain the policy qualifier"
[[ "$rls_plan" == *"Seq Scan on secure_rows"* ]] || fail "RLS qualifier did not remain in the PG-owned source"
rls_pg=$(psql_at "$PGD_COMMON_SQL SET pg_orca.daft_enable = 'off'; SET ROLE pgd_p1_tenant_a; $rls_query")
rls_force=$(psql_at "$PGD_COMMON_SQL SET ROLE pgd_p1_tenant_a; $rls_query")
assert_equal "$rls_pg" "$rls_force" "RLS engine differential"
[[ "$(result_count "$rls_pg")" -eq 6000 ]] || fail "RLS exposed rows outside the active tenant"

expect_privilege_denied() {
    local mode=$1
    local error_file="$PGD_TMP_DIR/privilege-$mode.err"
    set +e
    "${psql_cmd[@]}" >/dev/null 2>"$error_file" <<SQL
\set VERBOSITY sqlstate
$PGD_COMMON_SQL
SET pg_orca.daft_enable = '$mode';
SET ROLE pgd_p1_tenant_a;
SELECT protected_text FROM pgd_p1_correctness.secure_rows LIMIT 1;
SQL
    local status=$?
    set -e
    [[ "$status" -ne 0 ]] || fail "column privilege was bypassed in $mode mode"
    grep -q '42501' "$error_file" || fail "column privilege failure lacked SQLSTATE 42501"
}
expect_privilege_denied off
expect_privilege_denied force

definer_pg=$(psql_at "$PGD_COMMON_SQL SET pg_orca.daft_enable = 'off'; SET ROLE pgd_p1_tenant_a; SELECT pgd_p1_correctness.security_definer_count()")
definer_force=$(psql_at "$PGD_COMMON_SQL SET ROLE pgd_p1_tenant_a; SELECT pgd_p1_correctness.security_definer_count()")
assert_equal "$definer_pg" "$definer_force" "SECURITY DEFINER boundary"
[[ "$definer_pg" -eq 12000 ]] || fail "SECURITY DEFINER did not use its declared owner context"

# A parameter-free cached Daft plan must survive DDL invalidation and be
# replanned as DaftExec. Generic/custom parameter plans are currently outside
# the expression contract and must safely remain PG while matching PG results.
prepared_out="$PGD_TMP_DIR/prepared.out"
prepared_err="$PGD_TMP_DIR/prepared.err"
"${psql_cmd[@]}" >"$prepared_out" 2>"$prepared_err" <<SQL
$PGD_COMMON_SQL
PREPARE pgd_p1_cached AS
SELECT count(*), sum(amount), min(amount), max(amount)
FROM pgd_p1_correctness.capability_rows;
EXPLAIN (COSTS OFF) EXECUTE pgd_p1_cached;
EXECUTE pgd_p1_cached;
ALTER TABLE pgd_p1_correctness.capability_rows ADD COLUMN invalidation_marker int4;
EXECUTE pgd_p1_cached;
EXPLAIN (COSTS OFF) EXECUTE pgd_p1_cached;
DEALLOCATE pgd_p1_cached;
SQL
if grep -Eq '^(ERROR|FATAL|PANIC):' "$prepared_err"; then
    fail "cached Daft plan emitted a server error"
fi
prepared_expected=$(psql_at "$PGD_COMMON_SQL SET pg_orca.daft_enable = 'off'; $aggregate_sql")
[[ "$(grep -Fxc "$prepared_expected" "$prepared_out")" -eq 2 ]] || fail "cached Daft plan changed across DDL invalidation"
[[ "$(grep -c 'Custom Scan (DaftExec)' "$prepared_out")" -ge 2 ]] || fail "cached plan was not replanned as DaftExec"

parameter_expected=$(psql_at "$PGD_COMMON_SQL SET pg_orca.daft_enable = 'off'; SELECT count(*), sum(amount), min(amount), max(amount) FROM pgd_p1_correctness.capability_rows WHERE id <= 50000")
parameter_out="$PGD_TMP_DIR/parameter-plans.out"
parameter_err="$PGD_TMP_DIR/parameter-plans.err"
"${psql_cmd[@]}" >"$parameter_out" 2>"$parameter_err" <<SQL
$PGD_COMMON_SQL
SET plan_cache_mode = force_generic_plan;
PREPARE pgd_p1_generic(int4) AS
SELECT count(*), sum(amount), min(amount), max(amount)
FROM pgd_p1_correctness.capability_rows WHERE id <= \$1;
EXPLAIN (COSTS OFF) EXECUTE pgd_p1_generic(50000);
EXECUTE pgd_p1_generic(50000);
DEALLOCATE pgd_p1_generic;
SET plan_cache_mode = force_custom_plan;
PREPARE pgd_p1_custom(int4) AS
SELECT count(*), sum(amount), min(amount), max(amount)
FROM pgd_p1_correctness.capability_rows WHERE id <= \$1;
EXPLAIN (COSTS OFF) EXECUTE pgd_p1_custom(50000);
EXECUTE pgd_p1_custom(50000);
DEALLOCATE pgd_p1_custom;
SQL
if grep -Eq '^(ERROR|FATAL|PANIC):' "$parameter_err"; then
    fail "parameterized prepared plan emitted a server error"
fi
[[ "$(grep -Fxc "$parameter_expected" "$parameter_out")" -eq 2 ]] || fail "generic/custom prepared result mismatch"
[[ "$(grep -c 'Custom Scan (DaftExec)' "$parameter_out" || true)" -eq 0 ]] || fail "unsupported parameter expression unexpectedly entered Daft"

# Mechanical fallback accounting: every canonical BLOCKED optimizer rule must
# carry a stable PGDAFT reason, and the frozen SQL suite must expose its runtime
# mode/loader/CBO reason vocabulary.
read -r blocked_rules reason_coded_rules < <(
    awk '
        /^optimizer_rules:/ { inside = 1; next }
        /^functions:/ { inside = 0 }
        inside && /current_status: BLOCKED/ { blocked++ }
        inside && /blocked_reason_code: PGDAFT_/ { coded++ }
        END { print blocked + 0, coded + 0 }
    ' "$PGD_CAPABILITY_SPEC"
)
[[ "$blocked_rules" -gt 0 ]] || fail "canonical registry has no blocked-rule audit set"
[[ "$blocked_rules" -eq "$reason_coded_rules" ]] || fail "canonical blocked rule lacks a reason code"
runtime_reason_codes=$(
    grep -h -o 'reason=PGDAFT_[A-Z0-9_]*' "$PGD_WORKSPACE"/pg_orca/test/expected/daft_*.out |
        sed 's/reason=//' | sort -u | wc -l
)
[[ "$runtime_reason_codes" -ge 5 ]] || fail "frozen SQL suite has insufficient runtime reason coverage"

mkdir -p "$PGD_REPORTS_DIR"
cat >"$PGD_REPORTS_DIR/capability_differential_report.txt" <<EOF
report_version: 1
status: PASS
advertised_regression_cases: 7
database_semantics_cases: 14
result_differences: 0
rls_bypasses: 0
privilege_bypasses: 0
prepared_invalidation_differences: 0
skipped_cases: 0
EOF
cat >"$PGD_REPORTS_DIR/fallback_reason_report.txt" <<EOF
report_version: 1
status: PASS
canonical_blocked_rules: $blocked_rules
canonical_reason_coded_rules: $reason_coded_rules
unreasoned_blocked_rules: 0
runtime_reason_codes_observed: $runtime_reason_codes
rls_pg_owned_source_cases: 1
parameterized_prepared_pg_fallback_cases: 2
skipped_cases: 0
EOF

python3 "$PGD_INTEGRATION_ROOT/redact_log.py" "$PGD_REPORTS_DIR/capability_differential_report.txt"
python3 "$PGD_INTEGRATION_ROOT/redact_log.py" "$PGD_REPORTS_DIR/fallback_reason_report.txt"
echo "P1 correctness matrix PASS (14/14)"
