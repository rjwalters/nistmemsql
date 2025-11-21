#!/bin/bash
# TPC-H Benchmark Runner
#
# Quick and easy way to run TPC-H benchmarks during performance sprints
#
# Usage:
#   ./scripts/bench-tpch.sh              # Run with 30s timeout (default)
#   ./scripts/bench-tpch.sh 60           # Run with custom timeout
#   ./scripts/bench-tpch.sh 30 summary   # Show summary only

set -e

# Configuration
TIMEOUT_SECS=${1:-30}
MODE=${2:-full}
OUTPUT_FILE="/tmp/tpch_results.txt"
BENCH_NAME="tpch_profiling"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== TPC-H Benchmark Runner ===${NC}"
echo "Timeout: ${TIMEOUT_SECS}s per query"
echo "Output: ${OUTPUT_FILE}"
echo ""

# Build benchmark
echo -e "${YELLOW}Building benchmark...${NC}"
cargo build --release -p vibesql-executor --bench ${BENCH_NAME} --features benchmark-comparison 2>&1 | grep -E "(Compiling|Finished)" || true
echo -e "${GREEN}✓ Build complete${NC}"
echo ""

# Run benchmark
echo -e "${YELLOW}Running benchmark...${NC}"
QUERY_TIMEOUT_SECS=${TIMEOUT_SECS} timeout 300 ./target/release/deps/${BENCH_NAME}-* > ${OUTPUT_FILE} 2>&1
echo -e "${GREEN}✓ Benchmark complete${NC}"
echo ""

# Show results
if [ "$MODE" = "summary" ]; then
    # Summary mode - just show query totals
    echo -e "${BLUE}=== Results Summary ===${NC}"
    grep -E "^===" ${OUTPUT_FILE} | while read line; do
        query=$(echo "$line" | grep -oP 'Q\d+')
        if [ -n "$query" ]; then
            # Get the result for this query
            result=$(awk "/^=== $query ===/{flag=1; next} /^==={next}//{flag=0} flag && /TOTAL:/{print; exit}" ${OUTPUT_FILE})

            if echo "$result" | grep -q "TIMEOUT"; then
                echo -e "  $query: ${YELLOW}TIMEOUT (>${TIMEOUT_SECS}s)${NC}"
            elif echo "$result" | grep -q "ERROR"; then
                error=$(echo "$result" | grep -oP 'ERROR: \K.*')
                echo -e "  $query: ${RED}ERROR${NC} - $(echo "$error" | cut -c1-50)"
            elif [ -n "$result" ]; then
                time=$(echo "$result" | grep -oP 'TOTAL:\s+\K[0-9.]+[µmn]?s')
                echo -e "  $query: ${GREEN}$time${NC}"
            fi
        fi
    done
else
    # Full mode - show complete output
    cat ${OUTPUT_FILE}
fi

echo ""
echo -e "${BLUE}=== Quick Stats ===${NC}"
total=$(grep -c "^=== Q" ${OUTPUT_FILE} || echo "0")
passed=$(grep -E "Execute:.*rows\)" ${OUTPUT_FILE} | wc -l | tr -d ' ')
timeout=$(grep -c "TIMEOUT" ${OUTPUT_FILE} || echo "0")
errors=$(grep -E "Execute:.*ERROR" ${OUTPUT_FILE} | wc -l | tr -d ' ')

echo -e "Total queries: $total"
echo -e "${GREEN}Passed: $passed${NC}"
echo -e "${YELLOW}Timeout: $timeout${NC}"
echo -e "${RED}Errors: $errors${NC}"

echo ""
echo "Full results: ${OUTPUT_FILE}"
