#!/usr/bin/env bash
set -euo pipefail

PGD_WORKSPACE=${PGD_WORKSPACE:-/home/qiumc/data/code/cpp}
PGD_INSTALL_ROOT=${PGD_INSTALL_ROOT:-$PGD_WORKSPACE/.install/pg19-pgorca-daft}
PGD_RUNTIME_ROOT=${PGD_RUNTIME_ROOT:-$PGD_WORKSPACE/.runtime/pg19-pgorca-daft}
PGD_DATA_DIR=${PGD_DATA_DIR:-$PGD_RUNTIME_ROOT/data}
PGD_LOG_FILE=${PGD_LOG_FILE:-$PGD_RUNTIME_ROOT/postgresql.log}
PGD_PORT=${PGD_PORT:-55419}

validate_runtime_path() {
    local resolved
    local allowed
    resolved=$(realpath -m -- "$1")
    allowed=$(realpath -m -- "$PGD_WORKSPACE/.runtime/pg19-pgorca-daft")
    if [[ "$resolved" != "$allowed" && "$resolved" != "$allowed/"* ]]; then
        echo "runtime path escapes allowed root: $resolved" >&2
        exit 2
    fi
}

validate_runtime_path "$PGD_RUNTIME_ROOT"
validate_runtime_path "$PGD_DATA_DIR"

action=${1:-}
case "$action" in
    reset)
        if [[ -f "$PGD_DATA_DIR/postmaster.pid" ]]; then
            "$PGD_INSTALL_ROOT/bin/pg_ctl" -D "$PGD_DATA_DIR" -m fast -w stop
        fi
        cmake -E remove_directory "$PGD_RUNTIME_ROOT"
        ;;
    init)
        if [[ -e "$PGD_DATA_DIR/PG_VERSION" ]]; then
            echo "cluster already initialized: $PGD_DATA_DIR" >&2
            exit 2
        fi
        cmake -E make_directory "$PGD_RUNTIME_ROOT"
        "$PGD_INSTALL_ROOT/bin/initdb" -D "$PGD_DATA_DIR" --no-locale --encoding=UTF8
        {
            echo "shared_preload_libraries = 'pg_orca'"
            echo "port = $PGD_PORT"
            echo "listen_addresses = '127.0.0.1'"
            echo "max_connections = 20"
            echo "pg_orca.enable_orca = on"
            echo "pg_orca.daft_enable = off"
            echo "pg_orca.daft_bridge_library = '$PGD_INSTALL_ROOT/lib/postgresql/pg_orca/libdaft_pg_bridge.so'"
        } >>"$PGD_DATA_DIR/postgresql.conf"
        ;;
    start)
        "$PGD_INSTALL_ROOT/bin/pg_ctl" -D "$PGD_DATA_DIR" -l "$PGD_LOG_FILE" -w start
        ;;
    stop)
        "$PGD_INSTALL_ROOT/bin/pg_ctl" -D "$PGD_DATA_DIR" -m fast -w stop
        ;;
    status)
        "$PGD_INSTALL_ROOT/bin/pg_ctl" -D "$PGD_DATA_DIR" status
        ;;
    *)
        echo "usage: cluster.sh reset|init|start|stop|status" >&2
        exit 2
        ;;
esac
