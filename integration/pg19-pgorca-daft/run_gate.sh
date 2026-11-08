#!/usr/bin/env bash
set -euo pipefail

PGD_WORKSPACE=${PGD_WORKSPACE:-/home/qiumc/data/code/cpp}
PGD_BUILD_ROOT=${PGD_BUILD_ROOT:-$PGD_WORKSPACE/.build/pg19-pgorca-daft}
PGD_INSTALL_ROOT=${PGD_INSTALL_ROOT:-$PGD_WORKSPACE/.install/pg19-pgorca-daft}
PGD_INTEGRATION_ROOT=${PGD_INTEGRATION_ROOT:-$PGD_WORKSPACE/integration/pg19-pgorca-daft}
PGD_REPORTS_DIR=${PGD_REPORTS_DIR:-$PGD_INTEGRATION_ROOT/reports}
PGD_GATE=${1:-}
PGD_REQUIRED_CASES=0
PGD_PASS_CASES=0
PGD_FAIL_CASES=0
PGD_SKIP_CASES=0
export PG_CONFIG="$PGD_INSTALL_ROOT/bin/pg_config"
export PG_REGRESS_SQL="$PGD_WORKSPACE/postgres19/src/test/regress"
export PG_ORCA_TEST_BUILD_DIR="$PGD_BUILD_ROOT/pg_orca-tests"

require_file() {
    test -f "$1" || {
        echo "required gate input missing: $1" >&2
        exit 2
    }
}

run_orca_tests() {
    require_file "$PGD_WORKSPACE/pg_orca/test/test.sh"
    (
        cd "$PGD_WORKSPACE/pg_orca"
        test/test.sh --orca-tests "$@"
    )
}

# Write a version + feature + SHA report (deliverable: redacted SHA report;
# acceptance #3).  No SQL literals, tuple data or credentials are emitted --
# only build identity, which redact_log.py then scans.
write_report() {
    local gate=$1
    local status=$2
    mkdir -p "$PGD_REPORTS_DIR"
    local report="$PGD_REPORTS_DIR/gate_report.txt"
    {
        echo "gate: $gate"
        echo "status: $status"
        echo "required_cases: $PGD_REQUIRED_CASES"
        echo "pass_cases: $PGD_PASS_CASES"
        echo "fail_cases: $PGD_FAIL_CASES"
        echo "skip_cases: $PGD_SKIP_CASES"
        if [[ -x "$PGD_INSTALL_ROOT/bin/pg_config" ]]; then
            echo "postgres_version: $("$PGD_INSTALL_ROOT/bin/pg_config" --version)"
        else
            echo "postgres_version: (install not present)"
        fi
        echo "pg_orca_feature: PG_ORCA_ENABLE_DAFT=ON"
        echo "postgres_sha: $(git -C "$PGD_WORKSPACE/postgres19" rev-parse HEAD 2>/dev/null || echo unknown)"
        echo "pg_orca_sha:   $(git -C "$PGD_WORKSPACE/pg_orca" rev-parse HEAD 2>/dev/null || echo unknown)"
        echo "daft_sha:      $(git -C "$PGD_WORKSPACE/daft" rev-parse HEAD 2>/dev/null || echo unknown)"
        echo "bridge_library: libdaft_pg_bridge.so"
    } >"$report"
    echo "wrote report: $report"
}

case "$PGD_GATE" in
    P0)
        require_file "$PGD_INTEGRATION_ROOT/build_clean.sh"
        require_file "$PGD_INTEGRATION_ROOT/cluster.sh"
        bash "$PGD_INTEGRATION_ROOT/build_clean.sh"
        bash "$PGD_INTEGRATION_ROOT/cluster.sh" reset
        bash "$PGD_INTEGRATION_ROOT/cluster.sh" init
        trap 'bash "$PGD_INTEGRATION_ROOT/cluster.sh" stop >/dev/null 2>&1 || true' EXIT
        bash "$PGD_INTEGRATION_ROOT/cluster.sh" start
        "$PGD_INSTALL_ROOT/bin/psql" -X -At -p "${PGD_PORT:-55419}" \
            -d postgres -c "SELECT current_setting('pg_orca.daft_enable');"
        # One pg_regress invocation keeps the same backend/database semantics
        # used by the extension suite.  A zero exit means all ten mandatory
        # cases passed; pg_regress has no SKIP path for these named SQL files.
        PGD_REQUIRED_CASES=10
        run_orca_tests daft_loader daft_arrow daft_memo daft_plancache \
            daft_basic daft_fallback daft_cancel daft_errors daft_explain \
            daft_parallel
        PGD_PASS_CASES=$PGD_REQUIRED_CASES
        ;;
    P1-correctness)
        run_orca_tests daft_filter daft_join daft_aggregate daft_sort_topn daft_rules daft_explain daft_plancache
        require_file "$PGD_INTEGRATION_ROOT/cluster.sh"
        bash "$PGD_INTEGRATION_ROOT/cluster.sh" reset
        bash "$PGD_INTEGRATION_ROOT/cluster.sh" init
        trap 'bash "$PGD_INTEGRATION_ROOT/cluster.sh" stop >/dev/null 2>&1 || true' EXIT
        bash "$PGD_INTEGRATION_ROOT/cluster.sh" start
        require_file "$PGD_INTEGRATION_ROOT/run_correctness_matrix.sh"
        bash "$PGD_INTEGRATION_ROOT/run_correctness_matrix.sh"
        require_file "$PGD_INTEGRATION_ROOT/run_p1_join_stress.sh"
        bash "$PGD_INTEGRATION_ROOT/run_p1_join_stress.sh"
        # Seven advertised operator regressions, fourteen database-semantics
        # cases, and five join/fault/resource cases. None has a SKIP path.
        PGD_REQUIRED_CASES=26
        PGD_PASS_CASES=$PGD_REQUIRED_CASES
        ;;
    P1-performance)
        require_file "$PGD_INTEGRATION_ROOT/run_tpch.sh"
        bash "$PGD_INTEGRATION_ROOT/run_tpch.sh" --correctness-sf 0.01 --performance-sf 1
        # Twenty-two three-way SF0.01 result checks and three SF1 selected
        # join/aggregate workloads. run_tpch.sh has no SKIP path.
        PGD_REQUIRED_CASES=25
        PGD_PASS_CASES=$PGD_REQUIRED_CASES
        ;;
    P1)
        bash "$0" P1-correctness
        bash "$0" P1-performance
        ;;
    spill)
        require_file "$PGD_INTEGRATION_ROOT/run_fault_matrix.sh"
        bash "$PGD_INTEGRATION_ROOT/run_fault_matrix.sh" spill
        PGD_REQUIRED_CASES=3
        PGD_PASS_CASES=3
        ;;
    observability)
        run_orca_tests daft_explain
        require_file "$PGD_INTEGRATION_ROOT/run_fault_matrix.sh"
        bash "$PGD_INTEGRATION_ROOT/run_fault_matrix.sh" observability
        PGD_REQUIRED_CASES=8
        PGD_PASS_CASES=8
        ;;
    P2-faults)
        require_file "$PGD_INTEGRATION_ROOT/run_fault_matrix.sh"
        bash "$PGD_INTEGRATION_ROOT/run_fault_matrix.sh" all
        # Five feature-gated fault/fuzz cases, seven normal Bridge execution
        # cases, and five PostgreSQL recovery regressions. None has a SKIP path.
        PGD_REQUIRED_CASES=17
        PGD_PASS_CASES=$PGD_REQUIRED_CASES
        ;;
    P2-packaging)
        require_file "$PGD_INTEGRATION_ROOT/run_packaging_matrix.sh"
        bash "$PGD_INTEGRATION_ROOT/run_packaging_matrix.sh"
        PGD_REQUIRED_CASES=12
        PGD_PASS_CASES=$PGD_REQUIRED_CASES
        ;;
    P2)
        require_file "$PGD_INTEGRATION_ROOT/run_mixed_workload.sh"
        PGD_P2_DURATION_SECONDS=${PGD_P2_DURATION_SECONDS:-259200}
        if [[ "$PGD_P2_DURATION_SECONDS" -lt 259200 ]]; then
            echo "P2 production gate requires at least 259200 seconds" >&2
            exit 2
        fi
        bash "$PGD_INTEGRATION_ROOT/run_mixed_workload.sh" "$PGD_P2_DURATION_SECONDS"
        ;;
    *)
        echo "usage: run_gate.sh P0|P1-correctness|P1-performance|P1|spill|observability|P2-faults|P2-packaging|P2" >&2
        exit 2
        ;;
esac

write_report "$PGD_GATE" PASS

if [[ -f "$PGD_INTEGRATION_ROOT/redact_log.py" ]]; then
    python3 "$PGD_INTEGRATION_ROOT/redact_log.py" "$PGD_REPORTS_DIR"
else
    echo "required redaction scanner is missing" >&2
    exit 2
fi
