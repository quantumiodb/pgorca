#!/usr/bin/env bash
set -euo pipefail

# Reproducible P1 TPC-H correctness/performance evidence gate. Generated data
# and the pinned dbgen checkout live under .build; only redacted aggregate
# evidence is written under integration/reports.

PGD_WORKSPACE=${PGD_WORKSPACE:-/home/qiumc/data/code/cpp}
PGD_BUILD_ROOT=${PGD_BUILD_ROOT:-$PGD_WORKSPACE/.build/pg19-pgorca-daft}
PGD_INSTALL_ROOT=${PGD_INSTALL_ROOT:-$PGD_WORKSPACE/.install/pg19-pgorca-daft}
PGD_INTEGRATION_ROOT=${PGD_INTEGRATION_ROOT:-$PGD_WORKSPACE/integration/pg19-pgorca-daft}
PGD_REPORTS_DIR=${PGD_REPORTS_DIR:-$PGD_INTEGRATION_ROOT/reports}
PGD_PORT=${PGD_PORT:-55419}
PGD_CORRECTNESS_SF=
PGD_PERFORMANCE_SF=

PGD_DBGEN_COMMIT=32f1c1b92d1664dba542e927d23d86ffa57aa253
PGD_DBGEN_REPOSITORY=https://github.com/electrum/tpch-dbgen.git
PGD_DBGEN_ROOT=$PGD_BUILD_ROOT/tpch-dbgen
PGD_TPCH_DATA_ROOT=$PGD_BUILD_ROOT/tpch-data

while [[ $# -gt 0 ]]; do
    case "$1" in
        --correctness-sf)
            PGD_CORRECTNESS_SF=${2:-}
            shift 2
            ;;
        --performance-sf)
            PGD_PERFORMANCE_SF=${2:-}
            shift 2
            ;;
        *)
            echo "usage: run_tpch.sh --correctness-sf SF --performance-sf SF" >&2
            exit 2
            ;;
    esac
done

if [[ "$PGD_CORRECTNESS_SF" != "0.01" || "$PGD_PERFORMANCE_SF" != "1" ]]; then
    echo "P1 gate requires correctness SF0.01 and performance SF1" >&2
    exit 2
fi

for required in \
    "$PGD_INSTALL_ROOT/bin/psql" \
    "$PGD_INTEGRATION_ROOT/cluster.sh" \
    "$PGD_INTEGRATION_ROOT/tpch_benchmark.py" \
    "$PGD_WORKSPACE/daft/benchmarking/tpch/queries/01.sql"; do
    if [[ ! -e "$required" ]]; then
        echo "required TPC-H input missing: $required" >&2
        exit 2
    fi
done

mkdir -p "$PGD_BUILD_ROOT" "$PGD_TPCH_DATA_ROOT" "$PGD_REPORTS_DIR"

if [[ ! -d "$PGD_DBGEN_ROOT/.git" ]]; then
    git clone --no-checkout "$PGD_DBGEN_REPOSITORY" "$PGD_DBGEN_ROOT"
fi
git -C "$PGD_DBGEN_ROOT" checkout --detach "$PGD_DBGEN_COMMIT"
if [[ "$(git -C "$PGD_DBGEN_ROOT" rev-parse HEAD)" != "$PGD_DBGEN_COMMIT" ]]; then
    echo "dbgen checkout does not match the frozen commit" >&2
    exit 2
fi
if [[ ! -x "$PGD_DBGEN_ROOT/dbgen" ]]; then
    make -C "$PGD_DBGEN_ROOT" -j"${PGD_BUILD_JOBS:-8}"
fi

validate_data_path() {
    local resolved
    local allowed
    resolved=$(realpath -m -- "$1")
    allowed=$(realpath -m -- "$PGD_TPCH_DATA_ROOT")
    if [[ "$resolved" != "$allowed/"* ]]; then
        echo "TPC-H data path escapes its build root: $resolved" >&2
        exit 2
    fi
}

ensure_dataset() {
    local sf=$1
    local data_dir=$PGD_TPCH_DATA_ROOT/sf$sf
    local marker=$data_dir/.pgd-complete
    validate_data_path "$data_dir"

    if [[ -f "$marker" ]] &&
       grep -Fxq "dbgen_commit=$PGD_DBGEN_COMMIT" "$marker" &&
       grep -Fxq "scale_factor=$sf" "$marker"; then
        echo "reusing deterministic TPC-H SF$sf data"
        return
    fi

    cmake -E remove_directory "$data_dir"
    mkdir -p "$data_dir"
    (
        cd "$data_dir"
        "$PGD_DBGEN_ROOT/dbgen" -q -f -s "$sf" \
            -b "$PGD_DBGEN_ROOT/dists.dss"
        chmod -R u+rwX .
        for table_file in ./*.tbl; do
            sed -i -e 's/|$//' "$table_file"
        done
    )
    {
        echo "dbgen_commit=$PGD_DBGEN_COMMIT"
        echo "scale_factor=$sf"
        echo "command=dbgen -q -f -s $sf -b dists.dss"
        echo "seed_policy=TPC-H dbgen deterministic built-in seeds"
    } >"$marker"
}

ensure_dataset "$PGD_CORRECTNESS_SF"
ensure_dataset "$PGD_PERFORMANCE_SF"

bash "$PGD_INTEGRATION_ROOT/cluster.sh" reset

# Performance evidence must never compare PostgreSQL against a Rust Debug
# bridge left behind by a narrow correctness test. Rebuild the current locked
# sources into the same isolated Release target used by build_clean.sh, then
# prove the installed artifact is byte-identical before starting PostgreSQL.
(
    cd "$PGD_WORKSPACE/daft"
    CARGO_TARGET_DIR="$PGD_BUILD_ROOT/daft" \
        cargo build --locked --release -p daft-pg-bridge --no-default-features
)
PGD_RELEASE_BRIDGE=$PGD_BUILD_ROOT/daft/release/libdaft_pg_bridge.so
PGD_INSTALLED_BRIDGE=$PGD_INSTALL_ROOT/lib/postgresql/pg_orca/libdaft_pg_bridge.so
test -f "$PGD_RELEASE_BRIDGE"
cmake -E make_directory "$PGD_INSTALL_ROOT/lib/postgresql/pg_orca"
cmake -E copy_if_different "$PGD_RELEASE_BRIDGE" "$PGD_INSTALLED_BRIDGE"
if [[ "$(sha256sum "$PGD_RELEASE_BRIDGE" | awk '{print $1}')" != \
      "$(sha256sum "$PGD_INSTALLED_BRIDGE" | awk '{print $1}')" ]]; then
    echo "installed Daft Bridge does not match the Release artifact" >&2
    exit 2
fi

bash "$PGD_INTEGRATION_ROOT/cluster.sh" init
trap 'bash "$PGD_INTEGRATION_ROOT/cluster.sh" stop >/dev/null 2>&1 || true' EXIT
bash "$PGD_INTEGRATION_ROOT/cluster.sh" start

python3 "$PGD_INTEGRATION_ROOT/tpch_benchmark.py" \
    --workspace "$PGD_WORKSPACE" \
    --install-root "$PGD_INSTALL_ROOT" \
    --data-root "$PGD_TPCH_DATA_ROOT" \
    --query-root "$PGD_WORKSPACE/daft/benchmarking/tpch/queries" \
    --reports-dir "$PGD_REPORTS_DIR" \
    --host 127.0.0.1 \
    --port "$PGD_PORT" \
    --correctness-sf "$PGD_CORRECTNESS_SF" \
    --performance-sf "$PGD_PERFORMANCE_SF" \
    --dbgen-commit "$PGD_DBGEN_COMMIT"

python3 "$PGD_INTEGRATION_ROOT/redact_log.py" "$PGD_REPORTS_DIR"
