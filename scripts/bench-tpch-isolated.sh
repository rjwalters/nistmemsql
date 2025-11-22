#!/bin/bash
# TPC-H Isolated Benchmark Runner
#
# Runs each TPC-H query in a separate subprocess to prevent cascading failures
# from memory issues or crashes.
#
# Usage:
#   ./scripts/bench-tpch-isolated.sh [timeout_secs] [output_file]
#
# Examples:
#   ./scripts/bench-tpch-isolated.sh 30 /tmp/tpch_isolated.txt
#   ./scripts/bench-tpch-isolated.sh              # Uses defaults

set -e

# Configuration
TIMEOUT_SECS=${1:-30}
OUTPUT_FILE=${2:-/tmp/tpch_isolated_results.txt}
BENCH_NAME="tpch_profiling"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== TPC-H Isolated Benchmark Runner ===${NC}"
echo "Timeout: ${TIMEOUT_SECS}s per query"
echo "Output: ${OUTPUT_FILE}"
echo ""

# Build benchmark binary
echo -e "${YELLOW}Building benchmark...${NC}"
cargo build --release -p vibesql-executor --bench ${BENCH_NAME} --features benchmark-comparison 2>&1 | grep -E "(Compiling|Finished)" || true
BENCH_BIN=$(find target/release/deps -name "${BENCH_NAME}-*" -type f -perm +111 | head -1)

if [ -z "$BENCH_BIN" ]; then
    echo -e "${RED}Error: Benchmark binary not found${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Build complete: $BENCH_BIN${NC}"
echo ""

# Initialize output file
cat > "$OUTPUT_FILE" << EOF
=== TPC-H Isolated Benchmark Results ===
Timeout: ${TIMEOUT_SECS}s per query
Timestamp: $(date -u +"%Y-%m-%d %H:%M:%S UTC")
Binary: $BENCH_BIN

EOF

# Query list
QUERIES=(Q1 Q2 Q3 Q4 Q5 Q6 Q7 Q8 Q9 Q10 Q11 Q12 Q13 Q14 Q15 Q16 Q17 Q18 Q19 Q20 Q21 Q22)

# Counters
PASSED=0
TIMEOUT=0
CRASHED=0
TOTAL=${#QUERIES[@]}

echo -e "${YELLOW}Running ${TOTAL} queries in isolation...${NC}"
echo ""

# Run each query in isolation
for query in "${QUERIES[@]}"; do
    echo -ne "  ${query}: "

    # Run in subprocess with timeout
    QUERY_OUTPUT=$(mktemp)

    if timeout ${TIMEOUT_SECS} env QUERY_TIMEOUT_SECS=${TIMEOUT_SECS} "$BENCH_BIN" "$query" 2>&1 | \
       tee "$QUERY_OUTPUT" | \
       grep -q "TOTAL:"; then
        # Extract timing
        TIMING=$(grep "TOTAL:" "$QUERY_OUTPUT" | head -1 | awk '{print $2}')
        if echo "$TIMING" | grep -q "TIMEOUT"; then
            echo -e "${YELLOW}TIMEOUT${NC}"
            ((TIMEOUT++))
        else
            echo -e "${GREEN}${TIMING}${NC}"
            ((PASSED++))
        fi
    else
        # Check exit status
        EXIT_CODE=$?
        if [ $EXIT_CODE -eq 124 ]; then
            echo -e "${YELLOW}TIMEOUT (wallclock)${NC}"
            ((TIMEOUT++))
        else
            echo -e "${RED}CRASHED (exit $EXIT_CODE)${NC}"
            ((CRASHED++))
        fi
    fi

    # Append to results
    cat "$QUERY_OUTPUT" >> "$OUTPUT_FILE"
    echo "" >> "$OUTPUT_FILE"

    rm -f "$QUERY_OUTPUT"
done

# Summary
echo ""
echo -e "${BLUE}=== Summary ===${NC}"
echo -e "Total queries: $TOTAL"
echo -e "${GREEN}Passed: $PASSED${NC}"
echo -e "${YELLOW}Timeout: $TIMEOUT${NC}"
echo -e "${RED}Crashed: $CRASHED${NC}"
echo ""
echo "Full results: $OUTPUT_FILE"

# Add summary to output file
cat >> "$OUTPUT_FILE" << EOF

=== Summary ===
Total queries: $TOTAL
Passed: $PASSED
Timeout: $TIMEOUT
Crashed: $CRASHED
EOF
