#!/usr/bin/env bash
set -euo pipefail

PGD_WORKSPACE=${PGD_WORKSPACE:-/home/qiumc/data/code/cpp}
PGD_BUILD_ROOT=${PGD_BUILD_ROOT:-$PGD_WORKSPACE/.build/pg19-pgorca-daft}
PGD_INSTALL_ROOT=${PGD_INSTALL_ROOT:-$PGD_WORKSPACE/.install/pg19-pgorca-daft}
PGD_JOBS=${PGD_JOBS:-8}

safe_remove_directory() {
    local target=$1
    local allowed_prefix=$2
    local resolved_target
    local resolved_prefix
    resolved_target=$(realpath -m -- "$target")
    resolved_prefix=$(realpath -m -- "$allowed_prefix")
    if [[ "$resolved_target" == "$resolved_prefix" || "$resolved_target" == "$resolved_prefix/"* ]]; then
        if [[ "$resolved_target" == "/" || "$resolved_target" == "$PGD_WORKSPACE" ]]; then
            echo "refusing broad removal: $resolved_target" >&2
            exit 2
        fi
        cmake -E remove_directory "$resolved_target"
    else
        echo "refusing removal outside $resolved_prefix: $resolved_target" >&2
        exit 2
    fi
}

for source_dir in postgres19 pg_orca daft; do
    test -d "$PGD_WORKSPACE/$source_dir" || {
        echo "missing source directory: $PGD_WORKSPACE/$source_dir" >&2
        exit 2
    }
done

safe_remove_directory "$PGD_BUILD_ROOT" "$PGD_WORKSPACE/.build/pg19-pgorca-daft"
safe_remove_directory "$PGD_INSTALL_ROOT" "$PGD_WORKSPACE/.install/pg19-pgorca-daft"
cmake -E make_directory "$PGD_BUILD_ROOT/postgres19" "$PGD_INSTALL_ROOT"

cd "$PGD_BUILD_ROOT/postgres19"
"$PGD_WORKSPACE/postgres19/configure" \
    --prefix="$PGD_INSTALL_ROOT" \
    --enable-debug \
    --enable-cassert
make -j"$PGD_JOBS" world-bin
make install-world-bin

cd "$PGD_WORKSPACE/daft"
export CARGO_TARGET_DIR="$PGD_BUILD_ROOT/daft"
cargo build --locked --release -p daft-pg-bridge --no-default-features
test -f "$CARGO_TARGET_DIR/release/libdaft_pg_bridge.so"
cmake -E make_directory "$PGD_INSTALL_ROOT/lib/postgresql/pg_orca"
cmake -E copy_if_different \
    "$CARGO_TARGET_DIR/release/libdaft_pg_bridge.so" \
    "$PGD_INSTALL_ROOT/lib/postgresql/pg_orca/libdaft_pg_bridge.so"

cmake -S "$PGD_WORKSPACE/pg_orca" \
    -B "$PGD_BUILD_ROOT/pg_orca" \
    -G Ninja \
    -DPG_CONFIG="$PGD_INSTALL_ROOT/bin/pg_config" \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_INSTALL_PREFIX="$PGD_INSTALL_ROOT" \
    -DPG_ORCA_ENABLE_DAFT=ON
cmake --build "$PGD_BUILD_ROOT/pg_orca" -j"$PGD_JOBS"
cmake --install "$PGD_BUILD_ROOT/pg_orca"

"$PGD_INSTALL_ROOT/bin/pg_config" --version
git -C "$PGD_WORKSPACE/postgres19" rev-parse HEAD
git -C "$PGD_WORKSPACE/pg_orca" rev-parse HEAD
git -C "$PGD_WORKSPACE/daft" rev-parse HEAD
ldd "$PGD_INSTALL_ROOT/lib/postgresql/pg_orca.so"
