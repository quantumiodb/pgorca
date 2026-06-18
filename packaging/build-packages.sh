#!/usr/bin/env bash
# Build .deb and/or .rpm packages for pg_orca.
#
# Usage:
#   packaging/build-packages.sh [--deb] [--rpm] [--all]
#                               [--pg-config PATH] [--build-dir DIR]
#                               [--build-type Release|Debug]
#
# Defaults: --all, build-dir=build-pkg, build-type=Release.
# Requires: cmake, ninja (or make), and dpkg-deb (for --deb) / rpmbuild (for --rpm).
#
# The PostgreSQL pointed to by --pg-config (or $PG_CONFIG, or `pg_config` on
# PATH) determines the install paths baked into the package, so use the
# pg_config of the *target* PostgreSQL — typically a system/PGDG install
# such as /usr/lib/postgresql/18/bin/pg_config (Debian) or
# /usr/pgsql-18/bin/pg_config (RHEL).
set -euo pipefail

build_deb=0
build_rpm=0
build_dir="build-pkg"
build_type="Release"
pg_config="${PG_CONFIG:-}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --deb) build_deb=1 ;;
        --rpm) build_rpm=1 ;;
        --all) build_deb=1; build_rpm=1 ;;
        --pg-config) pg_config="$2"; shift ;;
        --build-dir) build_dir="$2"; shift ;;
        --build-type) build_type="$2"; shift ;;
        -h|--help)
            sed -n '2,16p' "$0"; exit 0 ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
    shift
done

if [[ $build_deb -eq 0 && $build_rpm -eq 0 ]]; then
    build_deb=1; build_rpm=1
fi

if [[ -z "$pg_config" ]]; then
    pg_config="$(command -v pg_config || true)"
fi
if [[ -z "$pg_config" || ! -x "$pg_config" ]]; then
    echo "error: pg_config not found. Pass --pg-config or set \$PG_CONFIG." >&2
    exit 1
fi

generators=()
[[ $build_deb -eq 1 ]] && generators+=("DEB")
[[ $build_rpm -eq 1 ]] && generators+=("RPM")
gens="$(IFS=';'; echo "${generators[*]}")"

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

echo "==> Configuring ($build_type) with PG_CONFIG=$pg_config"
# PG_ORCA_BUNDLED_XERCES=ON is required for release packages so xerces-c
# is statically linked into pg_orca.so (no runtime libxerces-c dep).
cmake -S . -B "$build_dir" \
    -DCMAKE_BUILD_TYPE="$build_type" \
    -DPG_CONFIG="$pg_config" \
    -DPG_ORCA_BUNDLED_XERCES=ON \
    -GNinja

echo "==> Building"
cmake --build "$build_dir" -j

echo "==> Packaging ($gens)"
( cd "$build_dir" && cpack -G "$gens" )

echo "==> Done. Artifacts:"
find "$build_dir" -maxdepth 2 -type f \( -name '*.deb' -o -name '*.rpm' \) -print
