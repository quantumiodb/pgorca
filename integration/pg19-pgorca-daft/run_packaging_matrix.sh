#!/usr/bin/env bash
set -euo pipefail

PGD_WORKSPACE=${PGD_WORKSPACE:-/home/qiumc/data/code/cpp}
PGD_BUILD_ROOT=${PGD_BUILD_ROOT:-$PGD_WORKSPACE/.build/pg19-pgorca-daft}
PGD_INSTALL_ROOT=${PGD_INSTALL_ROOT:-$PGD_WORKSPACE/.install/pg19-pgorca-daft}
PGD_INTEGRATION_ROOT=${PGD_INTEGRATION_ROOT:-$PGD_WORKSPACE/integration/pg19-pgorca-daft}
PGD_REPORTS_DIR=${PGD_REPORTS_DIR:-$PGD_INTEGRATION_ROOT/reports}
PGD_PACKAGING_RUNTIME_ROOT=${PGD_PACKAGING_RUNTIME_ROOT:-$PGD_WORKSPACE/.runtime/pg19-pgorca-daft-p2-packaging}
PGD_PACKAGING_PORT=${PGD_PACKAGING_PORT:-55429}
PGD_MANIFEST=$PGD_WORKSPACE/pg_orca/packaging/pg_orca-daft-manifest.json
PGD_STAGE_ROOT=$PGD_BUILD_ROOT/p2-packaging-stage
PGD_CARGO_TARGET_DIR=$PGD_BUILD_ROOT/daft
PGD_TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/pgd-packaging-matrix.XXXXXX")
PGD_DATA_DIR=$PGD_PACKAGING_RUNTIME_ROOT/data
PGD_LOG_FILE=$PGD_PACKAGING_RUNTIME_ROOT/postgresql.log
PGD_CLUSTER_STARTED=0

safe_remove_directory() {
    local target=$1
    local allowed_prefix=$2
    local resolved_target
    local resolved_prefix
    resolved_target=$(realpath -m -- "$target")
    resolved_prefix=$(realpath -m -- "$allowed_prefix")
    if [[ "$resolved_target" != "$resolved_prefix" &&
          "$resolved_target" != "$resolved_prefix/"* ]]; then
        echo "refusing removal outside packaging test root" >&2
        exit 2
    fi
    if [[ "$resolved_target" == "/" || "$resolved_target" == "$PGD_WORKSPACE" ]]; then
        echo "refusing broad packaging test removal" >&2
        exit 2
    fi
    cmake -E remove_directory "$resolved_target"
}

cleanup() {
    if [[ $PGD_CLUSTER_STARTED -eq 1 && -f "$PGD_DATA_DIR/postmaster.pid" ]]; then
        "$PGD_INSTALL_ROOT/bin/pg_ctl" -D "$PGD_DATA_DIR" -m fast -w stop >/dev/null 2>&1 || true
    fi
    safe_remove_directory "$PGD_PACKAGING_RUNTIME_ROOT" "$PGD_WORKSPACE/.runtime"
    case "$PGD_TMP_DIR" in
        "${TMPDIR:-/tmp}"/pgd-packaging-matrix.*)
            cmake -E remove_directory "$PGD_TMP_DIR"
            ;;
        *)
            echo "refusing unexpected packaging temporary directory" >&2
            ;;
    esac
}
trap cleanup EXIT

for tool in jq nm readelf sha256sum stat; do
    command -v "$tool" >/dev/null || {
        echo "required packaging tool is missing: $tool" >&2
        exit 2
    }
done
for file in \
    "$PGD_MANIFEST" \
    "$PGD_INSTALL_ROOT/bin/initdb" \
    "$PGD_INSTALL_ROOT/bin/pg_ctl" \
    "$PGD_INSTALL_ROOT/bin/psql" \
    "$PGD_INSTALL_ROOT/lib/postgresql/pg_orca.so" \
    "$PGD_INSTALL_ROOT/lib/postgresql/pg_orca/libdaft_pg_bridge.so"; do
    test -f "$file" || {
        echo "required packaging input is missing" >&2
        exit 2
    }
done

jq -e '
    .schema_version == 1 and
    .core_package.cmake_definitions.PG_ORCA_ENABLE_DAFT == "ON" and
    .core_package.runtime_defaults["pg_orca.daft_enable"] == "off" and
    .bridge_package.required == false and
    .bridge_package.owned_by_core_package == false and
    .bridge_package.abi.major == 1 and
    .bridge_package.abi.minimum_minor == 1 and
    (.bridge_package.abi.required_symbols | length) == 14 and
    (.bridge_package.abi.legacy_public_symbols | length) == 4 and
    .upgrade_policy.requires_postgresql_restart == true and
    .upgrade_policy.replace_mapped_file_in_place == false and
    .upgrade_policy.reinitialize_data_directory == false
' "$PGD_MANIFEST" >/dev/null

# Regenerate/install the current Daft-capable core so the installed package
# payload includes the manifest added by the source install rules.
cmake --build "$PGD_BUILD_ROOT/pg_orca" --target pg_orca
cmake --install "$PGD_BUILD_ROOT/pg_orca"
PGD_INSTALLED_MANIFEST=$PGD_INSTALL_ROOT/share/postgresql/extension/pg_orca-daft-manifest.json
cmp -s "$PGD_MANIFEST" "$PGD_INSTALLED_MANIFEST"
test -f "$PGD_INSTALL_ROOT/share/postgresql/extension/pg_orca.control"
test -f "$PGD_INSTALL_ROOT/share/postgresql/extension/pg_orca--1.0.0.sql"

if readelf -d "$PGD_INSTALL_ROOT/lib/postgresql/pg_orca.so" | grep -q 'libdaft_pg_bridge'; then
    echo "pg_orca core package has a forbidden link-time Bridge dependency" >&2
    exit 1
fi
if nm -D --undefined-only "$PGD_INSTALL_ROOT/lib/postgresql/pg_orca.so" | awk '{print $NF}' | grep -q '^dpg_'; then
    echo "pg_orca core package has a forbidden unresolved dpg symbol" >&2
    exit 1
fi

(cd "$PGD_WORKSPACE/daft" &&
    CARGO_TARGET_DIR="$PGD_CARGO_TARGET_DIR" \
        cargo build --locked --release -p daft-pg-bridge --no-default-features)
PGD_OLD_BRIDGE=$PGD_INSTALL_ROOT/lib/postgresql/pg_orca/libdaft_pg_bridge.so
PGD_CANDIDATE_BRIDGE=$PGD_CARGO_TARGET_DIR/release/libdaft_pg_bridge.so
test -f "$PGD_CANDIDATE_BRIDGE"

safe_remove_directory "$PGD_STAGE_ROOT" "$PGD_BUILD_ROOT"
cmake -E make_directory "$PGD_STAGE_ROOT"
PGD_OLD_SHA=$(sha256sum "$PGD_OLD_BRIDGE" | awk '{print $1}')
PGD_CANDIDATE_SHA=$(sha256sum "$PGD_CANDIDATE_BRIDGE" | awk '{print $1}')
PGD_OLD_DIR=$PGD_STAGE_ROOT/bridge-old-${PGD_OLD_SHA:0:12}
PGD_CANDIDATE_DIR=$PGD_STAGE_ROOT/bridge-candidate-${PGD_CANDIDATE_SHA:0:12}
cmake -E make_directory "$PGD_OLD_DIR" "$PGD_CANDIDATE_DIR"
cmake -E copy "$PGD_OLD_BRIDGE" "$PGD_OLD_DIR/libdaft_pg_bridge.so"
cmake -E copy "$PGD_CANDIDATE_BRIDGE" "$PGD_CANDIDATE_DIR/libdaft_pg_bridge.so"
chmod 0755 "$PGD_OLD_DIR/libdaft_pg_bridge.so" "$PGD_CANDIDATE_DIR/libdaft_pg_bridge.so"
PGD_STAGED_OLD=$PGD_OLD_DIR/libdaft_pg_bridge.so
PGD_STAGED_CANDIDATE=$PGD_CANDIDATE_DIR/libdaft_pg_bridge.so
test "$(stat -c '%a' "$PGD_STAGED_OLD")" = 755
test "$(stat -c '%a' "$PGD_STAGED_CANDIDATE")" = 755

jq -r '.bridge_package.abi.required_symbols[], .bridge_package.abi.legacy_public_symbols[]' \
    "$PGD_MANIFEST" | sort >"$PGD_TMP_DIR/expected-symbols.txt"
for bridge in "$PGD_STAGED_OLD" "$PGD_STAGED_CANDIDATE"; do
    nm -D --defined-only "$bridge" | awk '$3 ~ /^dpg_/ {print $3}' | sort >"$PGD_TMP_DIR/actual-symbols.txt"
    diff -u "$PGD_TMP_DIR/expected-symbols.txt" "$PGD_TMP_DIR/actual-symbols.txt"
done

safe_remove_directory "$PGD_PACKAGING_RUNTIME_ROOT" "$PGD_WORKSPACE/.runtime"
cmake -E make_directory "$PGD_PACKAGING_RUNTIME_ROOT"
"$PGD_INSTALL_ROOT/bin/initdb" -D "$PGD_DATA_DIR" --no-locale --encoding=UTF8 >"$PGD_TMP_DIR/initdb.log"
PGD_SERVER_OPTIONS="-p $PGD_PACKAGING_PORT -c listen_addresses=127.0.0.1 -c shared_preload_libraries=pg_orca"
"$PGD_INSTALL_ROOT/bin/pg_ctl" -D "$PGD_DATA_DIR" -l "$PGD_LOG_FILE" -o "$PGD_SERVER_OPTIONS" -w start
PGD_CLUSTER_STARTED=1
PGD_PSQL=("$PGD_INSTALL_ROOT/bin/psql" -X -q -h 127.0.0.1 -p "$PGD_PACKAGING_PORT" -d postgres -v ON_ERROR_STOP=1)

"${PGD_PSQL[@]}" -c 'CREATE EXTENSION pg_orca' >/dev/null
"${PGD_PSQL[@]}" -c 'CREATE TABLE p2_packaging_sentinel(id integer PRIMARY KEY)' >/dev/null
"${PGD_PSQL[@]}" -c 'INSERT INTO p2_packaging_sentinel VALUES (1)' >/dev/null
test "$("${PGD_PSQL[@]}" -Atc 'SHOW pg_orca.daft_enable')" = off

PGD_POSTMASTER_PID=$(head -n 1 "$PGD_DATA_DIR/postmaster.pid")
if grep -q 'libdaft_pg_bridge' "/proc/$PGD_POSTMASTER_PID/maps"; then
    echo "postmaster mapped the optional Bridge while Daft was off" >&2
    exit 1
fi
if ps -T -p "$PGD_POSTMASTER_PID" -o comm= | grep -Eiq 'daft|tokio'; then
    echo "postmaster started a Daft/Tokio thread while Daft was off" >&2
    exit 1
fi

PGD_MISSING_BRIDGE=$PGD_STAGE_ROOT/bridge-missing/libdaft_pg_bridge.so
test "$("${PGD_PSQL[@]}" -Atc "SET pg_orca.enable_orca=on; SET pg_orca.daft_enable=auto; SET pg_orca.daft_bridge_library='$PGD_MISSING_BRIDGE'; SELECT count(*) >= 0 FROM pg_class")" = t
set +e
PGD_FORCE_OUTPUT=$("${PGD_PSQL[@]}" -Atc "SET pg_orca.enable_orca=on; SET pg_orca.daft_enable=force; SET pg_orca.daft_bridge_library='$PGD_MISSING_BRIDGE'; SELECT oid FROM pg_class LIMIT 1" 2>&1)
PGD_FORCE_RC=$?
set -e
test "$PGD_FORCE_RC" -ne 0
grep -q 'PGDAFT_BRIDGE_LIBRARY_NOT_FOUND' <<<"$PGD_FORCE_OUTPUT"

probe_bridge_mapping() {
    local bridge=$1
    local pid_file=$PGD_TMP_DIR/backend.pid
    cmake -E rm -f "$pid_file"
    "${PGD_PSQL[@]}" -c "SET pg_orca.enable_orca=on; SET pg_orca.daft_enable=auto; SET pg_orca.daft_bridge_library='$bridge'; SELECT count(*) FROM pg_class; COPY (SELECT pg_backend_pid()) TO '$pid_file'; SELECT pg_sleep(2)" >/dev/null &
    local client_pid=$!
    for _ in $(seq 1 100); do
        [[ -s "$pid_file" ]] && break
        sleep 0.05
    done
    test -s "$pid_file"
    local backend_pid
    backend_pid=$(tr -d '[:space:]' <"$pid_file")
    grep -Fq "$bridge" "/proc/$backend_pid/maps"
    wait "$client_pid"
}

restart_with_bridge() {
    local bridge=$1
    "${PGD_PSQL[@]}" -c "ALTER SYSTEM SET pg_orca.daft_enable='off'" >/dev/null
    "${PGD_PSQL[@]}" -c "ALTER SYSTEM SET pg_orca.daft_bridge_library='$bridge'" >/dev/null
    "$PGD_INSTALL_ROOT/bin/pg_ctl" -D "$PGD_DATA_DIR" -m fast -w restart -o "$PGD_SERVER_OPTIONS"
    test "$("${PGD_PSQL[@]}" -Atc 'SHOW pg_orca.daft_enable')" = off
    test "$("${PGD_PSQL[@]}" -Atc 'SELECT count(*) FROM p2_packaging_sentinel')" = 1
    probe_bridge_mapping "$bridge"
}

# Baseline, forward upgrade, and rollback all reuse the one initialized data
# directory.  Each transition restarts PostgreSQL so no backend retains the
# previous immutable vtable or cached plan.
restart_with_bridge "$PGD_STAGED_OLD"
restart_with_bridge "$PGD_STAGED_CANDIDATE"
restart_with_bridge "$PGD_STAGED_OLD"

PGD_POSTMASTER_PID=$(head -n 1 "$PGD_DATA_DIR/postmaster.pid")
if grep -q 'libdaft_pg_bridge' "/proc/$PGD_POSTMASTER_PID/maps"; then
    echo "postmaster mapped the Bridge after upgrade/rollback" >&2
    exit 1
fi

mkdir -p "$PGD_REPORTS_DIR"
PGD_REPORT=$PGD_REPORTS_DIR/packaging_upgrade_report.txt
{
    echo "report_version: 1"
    echo "status: PASS"
    echo "manifest_schema: 1"
    echo "core_package_files: 4"
    echo "bridge_optional: PASS"
    echo "bridge_required_symbols: 14"
    echo "bridge_legacy_public_symbols: 4"
    echo "bridge_total_public_symbols: 18"
    echo "core_link_time_bridge_dependency: ABSENT"
    echo "runtime_default_off: PASS"
    echo "postmaster_bridge_mapping: ABSENT"
    echo "postmaster_daft_threads: ABSENT"
    echo "missing_bridge_auto_safe: PASS"
    echo "missing_bridge_force_reason: PGDAFT_BRIDGE_LIBRARY_NOT_FOUND"
    echo "old_bridge_sha256: $PGD_OLD_SHA"
    echo "candidate_bridge_sha256: $PGD_CANDIDATE_SHA"
    echo "versioned_upgrade_restart: PASS"
    echo "versioned_rollback_restart: PASS"
    echo "data_directory_initializations: 1"
    echo "data_directory_preserved: PASS"
    echo "sentinel_checks: 3"
    echo "result_differences: 0"
    echo "skipped_cases: 0"
} >"$PGD_REPORT"
python3 "$PGD_INTEGRATION_ROOT/redact_log.py" "$PGD_REPORT"
