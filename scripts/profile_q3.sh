#!/bin/bash
# Profile TPC-H Q3 execution with join debugging enabled

set -e

echo "=== TPC-H Q3 Profiling ==="
echo ""
echo "Running Q3 with JOIN_REORDER_VERBOSE=1 to see join ordering decisions"
echo ""

# Build the profiling benchmark
echo "Building benchmark..."
cargo build --release -p vibesql-executor --bench tpch_profiling --features benchmark-comparison 2>&1 | grep -E "(Compiling|Finished)" || true
echo ""

# Run Q3 with join debugging enabled
# The tpch_profiling benchmark runs all queries, so we filter for Q3 output
echo "Running Q3 profiling..."
QUERY_TIMEOUT_SECS=60 JOIN_REORDER_VERBOSE=1 ./target/release/deps/tpch_profiling-* 2>&1 | \
    awk '/^=== Q3 ===/,/^=== Q[0-9]+ ===/ {print} /^=== Q3 ===/ {found=1} found && /TOTAL:/ {exit}'

echo ""
echo "✓ Q3 profiling complete"
echo ""
echo "The output above shows:"
echo "  - Join ordering decisions from the query planner"
echo "  - Parse, executor creation, and execution timing"
echo "  - Total query execution time"
