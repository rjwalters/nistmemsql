#!/usr/bin/env bash
#
# Direct Parallel Cargo Test Runner
#
# Spawns multiple cargo test processes in parallel using bash job control.
# Each worker gets a unique WORKER_ID to partition test files.
#
# Usage:
#   ./scripts/run_parallel_cargo.sh [WORKERS] [TIME_BUDGET]
#
# Examples:
#   ./scripts/run_parallel_cargo.sh 64 3600    # 64 workers, 1 hour each
#   ./scripts/run_parallel_cargo.sh 128 1800   # 128 workers, 30 min each

set -euo pipefail

WORKERS=${1:-64}
TIME_BUDGET=${2:-3600}
SEED=${3:-$(date +%s)}

echo "=== Direct Parallel Cargo Test Runner ==="
echo "Workers: $WORKERS"
echo "Time budget: ${TIME_BUDGET}s"
echo "Shared seed: $SEED"
echo ""

# Create results directory
RESULTS_DIR="/tmp/sqllogictest_results_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"
echo "Results: $RESULTS_DIR"
echo ""

# Function to run a single worker
run_worker() {
    local worker_id=$1
    local total_workers=$2
    local seed=$3
    local time_budget=$4
    local results_dir=$5

    local log_file="$results_dir/worker_${worker_id}.log"

    echo "Starting worker $worker_id/$total_workers..." >> "$log_file"

    # Find the test binary
    local test_binary=$(find target/release/deps -name "sqllogictest_suite-*" -type f -executable | head -1)

    if [ -z "$test_binary" ]; then
        echo "Error: test binary not found" >> "$log_file"
        return 1
    fi

    # Run test binary directly (bypasses cargo lock)
    timeout $((time_budget + 30)) \
        env SQLLOGICTEST_SEED=$seed \
            SQLLOGICTEST_WORKER_ID=$worker_id \
            SQLLOGICTEST_TOTAL_WORKERS=$total_workers \
            SQLLOGICTEST_TIME_BUDGET=$time_budget \
        "$test_binary" run_sqllogictest_suite --nocapture >> "$log_file" 2>&1

    local exit_code=$?
    echo "Worker $worker_id finished with exit code $exit_code" >> "$log_file"

    # Use flock to safely copy the analysis JSON file (workers may finish simultaneously)
    (
        flock -x 200
        if [ -f target/sqllogictest_analysis.json ]; then
            cp target/sqllogictest_analysis.json "$results_dir/analysis_${worker_id}.json"
        fi
    ) 200>/tmp/sqllogictest_analysis_lock
}

export -f run_worker

# Pre-compile test binary
echo "Pre-compiling test binary..."
cargo test --test sqllogictest_suite --release --no-run
echo "✓ Test binary compiled"
echo ""

# Launch all workers in parallel using GNU parallel or xargs
echo "Launching $WORKERS workers in parallel..."
echo ""

# Set file descriptor limit for parallel execution
ulimit -n 65536

if command -v parallel &> /dev/null; then
    # Use GNU parallel if available (better load balancing)
    seq 1 $WORKERS | parallel -j $WORKERS run_worker {} $WORKERS $SEED $TIME_BUDGET $RESULTS_DIR
else
    # Fallback: Use bash background jobs
    for worker_id in $(seq 1 $WORKERS); do
        run_worker $worker_id $WORKERS $SEED $TIME_BUDGET $RESULTS_DIR &
    done

    # Wait for all background jobs
    echo "Waiting for all $WORKERS workers to complete..."
    wait
fi

echo ""
echo "=== All Workers Complete ==="
echo ""

# Aggregate results
echo "Aggregating results from worker logs..."
if [ -f scripts/aggregate_worker_results.py ]; then
    python3 scripts/aggregate_worker_results.py "$RESULTS_DIR" target/sqllogictest_cumulative.json
    echo ""
    echo "=== Final Results ==="
    jq '.summary' target/sqllogictest_cumulative.json 2>/dev/null || echo "Could not parse summary"
else
    echo "Warning: aggregation script not found, results not aggregated"
fi

echo ""
echo "✓ Complete"
echo "Results directory: $RESULTS_DIR"
