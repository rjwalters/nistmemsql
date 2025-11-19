#!/usr/bin/env bash
#
# Test basic ODBC connection to VibeSQL
# Requires: isql (unixODBC), psqlODBC driver, VibeSQL server running
#

set -e

echo "=== ODBC Connection Test ==="
echo

# Check if isql is available
if ! command -v isql &> /dev/null; then
    echo "Error: isql not found. Install unixODBC first."
    echo "  Ubuntu/Debian: sudo apt-get install unixodbc"
    echo "  macOS: brew install unixodbc"
    exit 1
fi

# Check if server is running
if ! lsof -i :5432 &> /dev/null; then
    echo "Warning: No process listening on port 5432"
    echo "Make sure vibesql-server is running"
    exit 1
fi

# Test connection using DSN (if configured)
echo "Testing with DSN (VibeSQL)..."
if echo "SELECT 1 as test;" | isql -v VibeSQL 2>&1 | grep -q "test"; then
    echo "✓ DSN connection successful"
else
    echo "✗ DSN connection failed (you may need to configure ~/.odbc.ini)"
fi

echo

# Test connection using connection string
echo "Testing with connection string..."
CONNECTION_STRING="Driver={PostgreSQL Unicode};Server=localhost;Port=5432;Database=testdb;Uid=testuser;Pwd=;SSLMode=disable;"

if echo "SELECT 1 as test;" | isql "$CONNECTION_STRING" 2>&1 | grep -q "test"; then
    echo "✓ Connection string test successful"
else
    echo "✗ Connection string test failed"
    echo "  Check if PostgreSQL ODBC driver is installed"
    exit 1
fi

echo
echo "=== All ODBC connection tests passed ==="
