#!/bin/bash
# Quick benchmark script that skips DuckDB for faster iteration
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Quick Benchmark (SQLite vs vibesql only) ==="
echo ""

# Run benchmarks without DuckDB
.venv/bin/pytest . \
    --benchmark-only \
    --no-duckdb \
    --benchmark-columns=min,max,mean,median,ops \
    -v

echo ""
echo "=== Quick Benchmark Complete ==="
echo "Tip: Run '../scripts/run_benchmarks.sh' for full benchmarks including DuckDB"
