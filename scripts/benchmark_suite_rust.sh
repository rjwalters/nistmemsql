#!/bin/bash
#
# Run head-to-head benchmarks: VibeSQL vs SQLite3
# Using the Rust benchmark harness for accurate timing
#
# Usage:
#   ./scripts/benchmark_suite_rust.sh [--sample N] [--categories "cat1,cat2"]

set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
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
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--sample N] [--categories 'cat1,cat2']"
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

# Create output directory
mkdir -p target/benchmarks

# Results file
RESULTS_FILE="target/benchmarks/comparison_$(date +%Y%m%d_%H%M%S).json"
echo "{" > "$RESULTS_FILE"
echo "  \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"," >> "$RESULTS_FILE"
echo "  \"total_files\": $TOTAL_FILES," >> "$RESULTS_FILE"
echo "  \"results\": [" >> "$RESULTS_FILE"

echo ""
echo "================================"
echo "SQLLogicTest Benchmark Suite"
echo "VibeSQL vs SQLite3"
echo "================================"
echo "Total files: $TOTAL_FILES"
echo "Output: $RESULTS_FILE"
echo ""

# Progress tracking
COUNT=0
VIBESQL_PASS=0
VIBESQL_FAIL=0
TOTAL_VIBESQL_TIME=0

# Benchmark each file
for TEST_FILE in "${TEST_FILES[@]}"; do
    COUNT=$((COUNT + 1))
    REL_PATH="${TEST_FILE#$TEST_DIR/}"

    printf "[%3d/%3d] %-60s " "$COUNT" "$TOTAL_FILES" "$REL_PATH"

    # Get category
    CATEGORY=$(echo "$REL_PATH" | cut -d'/' -f1)

    # Benchmark VibeSQL (3 runs for better statistics)
    RUNS=()
    ALL_PASSED=true

    for RUN in 1 2 3; do
        START_TIME=$(date +%s.%N)
        if timeout 30 "./scripts/sqllogictest" test "$REL_PATH" >/dev/null 2>&1; then
            END_TIME=$(date +%s.%N)
            DURATION=$(echo "$END_TIME - $START_TIME" | bc)
            RUNS+=("$DURATION")
        else
            ALL_PASSED=false
            END_TIME=$(date +%s.%N)
            DURATION=$(echo "$END_TIME - $START_TIME" | bc)
            RUNS+=("$DURATION")
        fi
    done

    # Calculate statistics from 3 runs
    MIN_TIME=$(printf '%s\n' "${RUNS[@]}" | sort -n | head -1)
    MAX_TIME=$(printf '%s\n' "${RUNS[@]}" | sort -n | tail -1)
    AVG_RUN=$(echo "scale=6; (${RUNS[0]} + ${RUNS[1]} + ${RUNS[2]}) / 3" | bc)

    if $ALL_PASSED; then
        VIBESQL_PASS=$((VIBESQL_PASS + 1))
        TOTAL_VIBESQL_TIME=$(echo "$TOTAL_VIBESQL_TIME + $AVG_RUN" | bc)
        printf "✓ %.3fs (min=%.3f max=%.3f)" "$AVG_RUN" "$MIN_TIME" "$MAX_TIME"

        # Write result to JSON
        [ $COUNT -gt 1 ] && echo "," >> "$RESULTS_FILE"
        cat >> "$RESULTS_FILE" <<EOF
    {
      "file": "$REL_PATH",
      "category": "$CATEGORY",
      "vibesql": {
        "success": true,
        "runs": [${RUNS[0]}, ${RUNS[1]}, ${RUNS[2]}],
        "min_secs": $MIN_TIME,
        "max_secs": $MAX_TIME,
        "avg_secs": $AVG_RUN
      }
    }
EOF
    else
        VIBESQL_FAIL=$((VIBESQL_FAIL + 1))
        printf "✗ failed"

        # Write result to JSON
        [ $COUNT -gt 1 ] && echo "," >> "$RESULTS_FILE"
        cat >> "$RESULTS_FILE" <<EOF
    {
      "file": "$REL_PATH",
      "category": "$CATEGORY",
      "vibesql": {
        "success": false,
        "runs": [${RUNS[0]}, ${RUNS[1]}, ${RUNS[2]}]
      }
    }
EOF
    fi

    echo ""
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
echo "Passed:         $VIBESQL_PASS"
echo "Failed:         $VIBESQL_FAIL"
echo "Pass rate:      $(echo "scale=1; $VIBESQL_PASS * 100 / $TOTAL_FILES" | bc)%"
echo "Total time:     ${TOTAL_VIBESQL_TIME}s"
echo "Average time:   ${AVG_TIME}s per file"
echo ""
echo "Results saved to: $RESULTS_FILE"
echo "================================"
