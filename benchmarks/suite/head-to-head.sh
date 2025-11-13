#!/bin/bash
#
# Head-to-head benchmark: VibeSQL vs SQLite3
#
# Features:
# - Interleaved execution (alternates engines to control for system load)
# - Adaptive repetitions (runs fast tests more times for stable measurements)
# - Target: At least 1 second total execution time per test file
# - Fair comparison using same Rust test harness
#
# Usage:
#   ./scripts/benchmark_head_to_head.sh [--sample N] [--categories "cat1,cat2"] [--min-time SECS]

set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

# Trap handler to ensure JSON is properly closed on interruption
cleanup() {
    if [ -f "$RESULTS_FILE" ] && [ -n "$RESULTS_FILE" ]; then
        # Check if JSON needs closing (doesn't end with })
        if ! tail -c 2 "$RESULTS_FILE" | grep -q '}'; then
            echo "" >> "$RESULTS_FILE"
            echo "  ]" >> "$RESULTS_FILE"
            echo "}" >> "$RESULTS_FILE"
            echo ""
            echo "⚠️  Script interrupted - JSON file closed gracefully"
        fi
    fi
}
trap cleanup EXIT INT TERM

# Parse arguments
SAMPLE=""
CATEGORIES=""
MIN_TIME=1.0  # Minimum 1 second total execution per test file
while [[ $# -gt 0 ]]; do
    case $1 in
        --sample)
            SAMPLE="$2"
            shift 2
            ;;
        --categories)
            CATEGORIES="$2"
            shift 2
            ;;
        --min-time)
            MIN_TIME="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--sample N] [--categories 'cat1,cat2'] [--min-time SECS]"
            exit 1
            ;;
    esac
done

# Find test files
TEST_DIR="third_party/sqllogictest/test"
if [ -n "$CATEGORIES" ]; then
    # Specific categories
    IFS=',' read -ra CATS <<< "$CATEGORIES"
    TEST_FILES=()
    for cat in "${CATS[@]}"; do
        while IFS= read -r file; do
            TEST_FILES+=("$file")
        done < <(find "$TEST_DIR/$cat" -name "*.test" -type f 2>/dev/null || true)
    done
else
    # All files
    TEST_FILES=()
    while IFS= read -r file; do
        TEST_FILES+=("$file")
    done < <(find "$TEST_DIR" -name "*.test" -type f | sort)
fi

TOTAL_FILES=${#TEST_FILES[@]}
echo "Found $TOTAL_FILES test files"

# Sample if requested
if [ -n "$SAMPLE" ] && [ "$SAMPLE" -lt "$TOTAL_FILES" ]; then
    # Shuffle and take first N
    SAMPLED=()
    while IFS= read -r file; do
        SAMPLED+=("$file")
    done < <(printf '%s\n' "${TEST_FILES[@]}" | shuf | head -n "$SAMPLE")
    TEST_FILES=("${SAMPLED[@]}")
    echo "Sampling $SAMPLE random files"
    TOTAL_FILES=$SAMPLE
fi

# Ensure release binary exists
if [ ! -f "target/release/vibesql" ]; then
    echo "Building VibeSQL in release mode..."
    cargo build --release
fi

# Create output directory
mkdir -p target/benchmarks

# Results file
RESULTS_FILE="target/benchmarks/head_to_head_$(date +%Y%m%d_%H%M%S).json"
echo "{" > "$RESULTS_FILE"
echo "  \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"," >> "$RESULTS_FILE"
echo "  \"total_files\": $TOTAL_FILES," >> "$RESULTS_FILE"
echo "  \"min_time_target\": $MIN_TIME," >> "$RESULTS_FILE"
echo "  \"results\": [" >> "$RESULTS_FILE"

echo ""
echo "================================"
echo "Head-to-Head Benchmark"
echo "VibeSQL vs SQLite3"
echo "================================"
echo "Total files: $TOTAL_FILES"
echo "Min time target: ${MIN_TIME}s per file"
echo "Output: $RESULTS_FILE"
echo ""

# Progress tracking
COUNT=0
VIBESQL_PASS=0
VIBESQL_FAIL=0
SQLITE_PASS=0
SQLITE_FAIL=0
TOTAL_VIBESQL_TIME=0
TOTAL_SQLITE_TIME=0

# Helper function to run a single test with timing
run_test() {
    local ENGINE="$1"
    local REL_PATH="$2"  # Relative path from TEST_DIR (e.g., "evidence/in1.test")
    local TIMEOUT=30

    START_TIME=$(date +%s.%N)
    if [ "$ENGINE" = "vibesql" ]; then
        # Pass only the relative path - the test runner will prepend "third_party/sqllogictest/test/"
        if timeout $TIMEOUT env SQLLOGICTEST_FILE="$REL_PATH" cargo test -p vibesql --test sqllogictest_runner run_single_test_file >/dev/null 2>&1; then
            END_TIME=$(date +%s.%N)
            DURATION=$(echo "$END_TIME - $START_TIME" | bc)
            echo "$DURATION:success"
        else
            END_TIME=$(date +%s.%N)
            DURATION=$(echo "$END_TIME - $START_TIME" | bc)
            echo "$DURATION:failure"
        fi
    else
        # SQLite - use cargo test with the benchmark infrastructure
        # For now, we'll use a placeholder - this would need actual SQLite runner
        # This is a simplified version; full implementation would use the Rust harness
        END_TIME=$(date +%s.%N)
        DURATION=$(echo "$END_TIME - $START_TIME" | bc)
        echo "$DURATION:skipped"
    fi
}

# Calculate adaptive repetition count
calculate_reps() {
    local INITIAL_TIME="$1"
    # Target at least MIN_TIME seconds total
    # Start with 3 reps minimum
    if (( $(echo "$INITIAL_TIME < 0.001" | bc -l) )); then
        # Very fast test (< 1ms) - cap at reasonable max
        echo "100"
    elif (( $(echo "$INITIAL_TIME * 3 < $MIN_TIME" | bc -l) )); then
        # Calculate how many more reps needed
        NEEDED=$(echo "scale=0; ($MIN_TIME / $INITIAL_TIME) + 0.5" | bc)
        # Cap at 100 to avoid excessive runtime on very fast tests
        if (( $(echo "$NEEDED > 100" | bc -l) )); then
            echo "100"
        elif (( $(echo "$NEEDED < 3" | bc -l) )); then
            echo "3"
        else
            echo "$NEEDED"
        fi
    else
        echo "3"
    fi
}

# Benchmark each file
for TEST_FILE in "${TEST_FILES[@]}"; do
    COUNT=$((COUNT + 1))
    REL_PATH="${TEST_FILE#$TEST_DIR/}"

    printf "[%3d/%3d] %-60s " "$COUNT" "$TOTAL_FILES" "$REL_PATH"

    # Get category
    CATEGORY=$(echo "$REL_PATH" | cut -d'/' -f1)

    # Do initial runs to determine adaptive repetition count
    VIBESQL_INITIAL=$(run_test "vibesql" "$REL_PATH")
    VIBESQL_INITIAL_TIME=$(echo "$VIBESQL_INITIAL" | cut -d':' -f1)
    VIBESQL_INITIAL_STATUS=$(echo "$VIBESQL_INITIAL" | cut -d':' -f2)

    # Skip if initial run failed
    if [ "$VIBESQL_INITIAL_STATUS" != "success" ]; then
        VIBESQL_FAIL=$((VIBESQL_FAIL + 1))
        printf "✗ vibesql failed\n"

        [ $COUNT -gt 1 ] && echo "," >> "$RESULTS_FILE"
        cat >> "$RESULTS_FILE" <<EOF
    {
      "file": "$REL_PATH",
      "category": "$CATEGORY",
      "vibesql": { "success": false },
      "sqlite": { "success": false, "skipped": true }
    }
EOF
        continue
    fi

    # Calculate adaptive repetitions
    REPS=$(calculate_reps "$VIBESQL_INITIAL_TIME")
    # Ensure REPS is an integer for the loop
    REPS=$(printf "%.0f" "$REPS")

    # Run interleaved benchmarks
    # Pattern: V S V S V S... alternating to control for system effects
    VIBESQL_RUNS=()
    SQLITE_RUNS=()

    for ((i=0; i<REPS; i++)); do
        # Run VibeSQL
        VS_RESULT=$(run_test "vibesql" "$REL_PATH")
        VS_TIME=$(echo "$VS_RESULT" | cut -d':' -f1)
        VS_STATUS=$(echo "$VS_RESULT" | cut -d':' -f2)

        if [ "$VS_STATUS" = "success" ]; then
            VIBESQL_RUNS+=("$VS_TIME")
        fi

        # Run SQLite (interleaved)
        # NOTE: This is a placeholder until we integrate the Rust SQLite benchmark
        # For now, we'll skip SQLite runs as they require the proper test harness
        # SQLITE_RESULT=$(run_test "sqlite" "$REL_PATH")
        # SQLITE_TIME=$(echo "$SQLITE_RESULT" | cut -d':' -f1)
        # SQLITE_RUNS+=("$SQLITE_TIME")
    done

    # Calculate statistics
    if [ ${#VIBESQL_RUNS[@]} -gt 0 ]; then
        # Sort runs to get min/max
        IFS=$'\n' SORTED_VS=($(sort -n <<<"${VIBESQL_RUNS[*]}"))
        unset IFS

        VS_MIN="${SORTED_VS[0]}"
        # Get last element (max) - compatible with bash 3.x
        VS_MAX="${SORTED_VS[${#SORTED_VS[@]}-1]}"

        # Calculate average
        VS_SUM=0
        for run in "${VIBESQL_RUNS[@]}"; do
            VS_SUM=$(echo "$VS_SUM + $run" | bc)
        done
        VS_AVG=$(echo "scale=6; $VS_SUM / ${#VIBESQL_RUNS[@]}" | bc)

        VIBESQL_PASS=$((VIBESQL_PASS + 1))
        TOTAL_VIBESQL_TIME=$(echo "$TOTAL_VIBESQL_TIME + $VS_AVG" | bc)

        printf "✓ %dx%.3fs (min=%.3f max=%.3f avg=%.3f)\n" \
            "$REPS" "$VS_AVG" "$VS_MIN" "$VS_MAX" "$VS_AVG"

        # Write result to JSON
        [ $COUNT -gt 1 ] && echo "," >> "$RESULTS_FILE"

        # Format runs array for JSON
        RUNS_JSON=$(printf "%s," "${VIBESQL_RUNS[@]}")
        RUNS_JSON="[${RUNS_JSON%,}]"

        cat >> "$RESULTS_FILE" <<EOF
    {
      "file": "$REL_PATH",
      "category": "$CATEGORY",
      "repetitions": $REPS,
      "vibesql": {
        "success": true,
        "runs": $RUNS_JSON,
        "min_secs": $VS_MIN,
        "max_secs": $VS_MAX,
        "avg_secs": $VS_AVG
      },
      "sqlite": {
        "success": false,
        "skipped": true,
        "note": "SQLite benchmarking requires Rust test harness integration"
      }
    }
EOF
    else
        VIBESQL_FAIL=$((VIBESQL_FAIL + 1))
        printf "✗ failed\n"

        [ $COUNT -gt 1 ] && echo "," >> "$RESULTS_FILE"
        cat >> "$RESULTS_FILE" <<EOF
    {
      "file": "$REL_PATH",
      "category": "$CATEGORY",
      "vibesql": { "success": false },
      "sqlite": { "success": false, "skipped": true }
    }
EOF
    fi
done

# Close JSON
echo "" >> "$RESULTS_FILE"
echo "  ]" >> "$RESULTS_FILE"
echo "}" >> "$RESULTS_FILE"

# Print summary
AVG_TIME=0
if [ $VIBESQL_PASS -gt 0 ]; then
    AVG_TIME=$(echo "scale=3; $TOTAL_VIBESQL_TIME / $VIBESQL_PASS" | bc)
fi

echo ""
echo "================================"
echo "BENCHMARK SUMMARY"
echo "================================"
echo "Total files:    $TOTAL_FILES"
echo ""
echo "VibeSQL:"
echo "  Passed:       $VIBESQL_PASS"
echo "  Failed:       $VIBESQL_FAIL"
echo "  Pass rate:    $(echo "scale=1; $VIBESQL_PASS * 100 / $TOTAL_FILES" | bc)%"
echo "  Total time:   ${TOTAL_VIBESQL_TIME}s"
echo "  Average:      ${AVG_TIME}s per file"
echo ""
echo "SQLite:"
echo "  Status:       Not yet integrated"
echo "  Note:         Requires Rust test harness"
echo ""
echo "Results saved to: $RESULTS_FILE"
echo "================================"
echo ""
echo "Next steps:"
echo "  - Integrate SqliteDB from tests/sqllogictest_benchmark.rs"
echo "  - Add proper interleaved execution"
echo "  - Compare performance metrics"
