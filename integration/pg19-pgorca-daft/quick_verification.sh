#!/usr/bin/env bash
#===----------------------------------------------------------------------===#
# quick_verification.sh - Quick verification test for pg_orca
#
# This script performs a quick verification test using the existing pg_orca
# build without rebuilding PostgreSQL 19 from scratch.
#
#===----------------------------------------------------------------------===#

set -euo pipefail

echo "============================================"
echo "Quick Verification Test"
echo "============================================"
echo "Purpose: Verify pg_orca functionality without full rebuild"
echo "Duration: ~2-3 minutes"
echo "============================================"
echo ""

# Configuration
PGD_WORKSPACE=${PGD_WORKSPACE:-/home/qiumc/data/code/cpp}
PGD_EXISTING_BUILD=${PGD_EXISTING_BUILD:-$PGD_WORKSPACE/pg_orca/build}
PGD_TEST_INSTALL=${PGD_TEST_INSTALL:-$PGD_WORKSPACE/.install/pg19-pgorca-daft-test}
PGD_TEST_RUNTIME=${PGD_TEST_RUNTIME:-$PGD_WORKSPACE/.runtime/pg19-pgorca-daft-test}
PGD_TEST_PORT=${PGD_TEST_PORT:-55420}

echo "[1/6] Checking existing build..."
if [[ ! -f "$PGD_EXISTING_BUILD/pg_orca.so" ]]; then
    echo "ERROR: pg_orca.so not found at $PGD_EXISTING_BUILD" >&2
    echo "       Please build pg_orca first or run full P0 gate" >&2
    exit 1
fi
echo "  ✓ Found: $PGD_EXISTING_BUILD/pg_orca.so"

# Check for existing PostgreSQL installation
if [[ ! -f "$PGD_WORKSPACE/postgres19/src/backend/postgres" ]]; then
    echo "ERROR: PostgreSQL 19 not compiled" >&2
    echo "       Run: cd $PGD_WORKSPACE/postgres19 && ./configure && make -j8" >&2
    exit 1
fi
echo "  ✓ Found: PostgreSQL 19 compiled"

echo ""
echo "[2/6] Setting up test installation..."
cmake -E remove_directory "$PGD_TEST_INSTALL"
cmake -E remove_directory "$PGD_TEST_RUNTIME"
cmake -E make_directory "$PGD_TEST_INSTALL/lib/postgresql"

# Copy PostgreSQL binaries (if available from a previous install)
if [[ -d "$PGD_WORKSPACE/.install/pg19-pgorca-daft" ]]; then
    echo "  Using existing PostgreSQL installation..."
    cp -r "$PGD_WORKSPACE/.install/pg19-pgorca-daft"/* "$PGD_TEST_INSTALL/"
elif [[ -f "$PGD_WORKSPACE/postgres19/tmp_install/usr/local/pgsql/bin/postgres" ]]; then
    echo "  Using postgres19/tmp_install..."
    cp -r "$PGD_WORKSPACE/postgres19/tmp_install/usr/local/pgsql"/* "$PGD_TEST_INSTALL/"
else
    echo "  Installing PostgreSQL from source tree..."
    (
        cd "$PGD_WORKSPACE/postgres19"
        if [[ ! -f "config.status" ]]; then
            ./configure --prefix="$PGD_TEST_INSTALL" --enable-debug --enable-cassert
        fi
        make install -j8
    )
fi

# Copy pg_orca.so
cp "$PGD_EXISTING_BUILD/pg_orca.so" "$PGD_TEST_INSTALL/lib/postgresql/"
echo "  ✓ Test installation ready"

echo ""
echo "[3/6] Initializing test cluster..."
cmake -E make_directory "$PGD_TEST_RUNTIME"
"$PGD_TEST_INSTALL/bin/initdb" -D "$PGD_TEST_RUNTIME/data" --no-locale --encoding=UTF8

# Configure
{
    echo "shared_preload_libraries = 'pg_orca'"
    echo "port = $PGD_TEST_PORT"
    echo "listen_addresses = '127.0.0.1'"
    echo "max_connections = 10"
    echo "pg_orca.enable_orca = on"
    echo "pg_orca.daft_enable = off"
} >> "$PGD_TEST_RUNTIME/data/postgresql.conf"

echo "  ✓ Cluster initialized (port $PGD_TEST_PORT)"

echo ""
echo "[4/6] Starting test cluster..."
"$PGD_TEST_INSTALL/bin/pg_ctl" -D "$PGD_TEST_RUNTIME/data" \
    -l "$PGD_TEST_RUNTIME/postgresql.log" -w start

echo "  ✓ Cluster started"

# Wait a moment for startup
sleep 2

echo ""
echo "[5/6] Running verification tests..."

# Helper function to run SQL
run_test_sql() {
    local test_name=$1
    local sql=$2
    echo ""
    echo "  Test: $test_name"
    if "$PGD_TEST_INSTALL/bin/psql" -h 127.0.0.1 -p "$PGD_TEST_PORT" \
        -d postgres -c "$sql" > /dev/null 2>&1; then
        echo "    ✓ PASS"
        return 0
    else
        echo "    ✗ FAIL"
        return 1
    fi
}

tests_passed=0
tests_failed=0

# Test 1: Extension loads
if run_test_sql "Extension loading" "CREATE EXTENSION IF NOT EXISTS pg_orca;"; then
    ((tests_passed++))
else
    ((tests_failed++))
fi

# Test 2: Basic query
if run_test_sql "Basic SELECT" "SELECT 1 AS test;"; then
    ((tests_passed++))
else
    ((tests_failed++))
fi

# Test 3: Create table
if run_test_sql "CREATE TABLE" "CREATE TABLE test_quick (id int4, name text);"; then
    ((tests_passed++))
else
    ((tests_failed++))
fi

# Test 4: Insert data
if run_test_sql "INSERT" "INSERT INTO test_quick VALUES (1, 'Alice'), (2, 'Bob');"; then
    ((tests_passed++))
else
    ((tests_failed++))
fi

# Test 5: Select with data
echo ""
echo "  Test: SELECT with data"
result=$("$PGD_TEST_INSTALL/bin/psql" -h 127.0.0.1 -p "$PGD_TEST_PORT" \
    -d postgres -t -c "SELECT COUNT(*) FROM test_quick;")
if [[ "$result" =~ 2 ]]; then
    echo "    ✓ PASS (returned $result rows)"
    ((tests_passed++))
else
    echo "    ✗ FAIL (expected 2, got $result)"
    ((tests_failed++))
fi

# Test 6: EXPLAIN with ORCA
if run_test_sql "EXPLAIN with ORCA" "SET pg_orca.enable_orca = on; EXPLAIN SELECT * FROM test_quick;"; then
    ((tests_passed++))
else
    ((tests_failed++))
fi

# Test 7: Type conversion test (Arrow)
if run_test_sql "Type conversion" \
    "SELECT 42::int4, 'test'::text, 3.14::float8, '2024-01-01'::date, true::bool;"; then
    ((tests_passed++))
else
    ((tests_failed++))
fi

# Test 8: NULL handling
if run_test_sql "NULL handling" \
    "SELECT NULL::int4, NULL::text;"; then
    ((tests_passed++))
else
    ((tests_failed++))
fi

# Cleanup test data
"$PGD_TEST_INSTALL/bin/psql" -h 127.0.0.1 -p "$PGD_TEST_PORT" \
    -d postgres -c "DROP TABLE IF EXISTS test_quick;" > /dev/null 2>&1 || true

echo ""
echo "[6/6] Stopping test cluster..."
"$PGD_TEST_INSTALL/bin/pg_ctl" -D "$PGD_TEST_RUNTIME/data" -m fast -w stop
echo "  ✓ Cluster stopped"

echo ""
echo "============================================"
echo "Test Results"
echo "============================================"
echo "Tests passed: $tests_passed / $((tests_passed + tests_failed))"
echo "Tests failed: $tests_failed / $((tests_passed + tests_failed))"
echo ""

if [[ $tests_failed -eq 0 ]]; then
    echo "✓ SUCCESS: All tests passed"
    echo ""
    echo "Environment details:"
    echo "  PostgreSQL: $("$PGD_TEST_INSTALL/bin/pg_config" --version 2>/dev/null || echo 'N/A')"
    echo "  pg_orca.so: $(stat -c%s "$PGD_EXISTING_BUILD/pg_orca.so" 2>/dev/null || echo '?') bytes"
    echo "  Test port: $PGD_TEST_PORT"
    echo ""
    echo "Next steps:"
    echo "  - Run full P0 gate: ./run_gate.sh P0"
    echo "  - Test Daft modes: ./run_gate.sh P0-daft-modes"
    echo "  - Run correctness tests: ./run_gate.sh P1-correctness"
    exit 0
else
    echo "✗ FAILURE: Some tests failed"
    echo ""
    echo "Check logs at: $PGD_TEST_RUNTIME/postgresql.log"
    echo ""
    exit 1
fi
