#!/usr/bin/env bash
#
# Run index/between and index/commute tests to validate PR #1442 fix
#

set -euo pipefail

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

RESULTS_DIR="validation_results"
mkdir -p "$RESULTS_DIR"

echo "========================================="
echo "Index Ordering Validation (Issue #1449)"
echo "Testing PR #1442 fix"
echo "========================================="
echo ""

# Function to run a single test file
run_test() {
    local test_file="$1"
    local category="$2"

    local test_name=$(basename "$test_file")
    local subdir=$(dirname "$test_file" | sed 's|.*index/[^/]*/||')

    echo -n "Testing $category/$subdir/$test_name... "

    if timeout 60 cargo test --package vibesql --test sqllogictest_runner -- \
        --test-file="$test_file" --nocapture > "$RESULTS_DIR/${category}_${subdir}_${test_name}.log" 2>&1; then
        echo -e "${GREEN}PASS${NC}"
        echo "PASS" > "$RESULTS_DIR/${category}_${subdir}_${test_name}.result"
        return 0
    else
        echo -e "${RED}FAIL${NC}"
        echo "FAIL" > "$RESULTS_DIR/${category}_${subdir}_${test_name}.result"
        return 1
    fi
}

# Run BETWEEN tests
echo "=== Running BETWEEN Tests ==="
BETWEEN_FILES=$(find third_party/sqllogictest/test/index/between -name "*.test" | sort)
BETWEEN_PASSED=0
BETWEEN_FAILED=0

for test_file in $BETWEEN_FILES; do
    if run_test "$test_file" "between"; then
        ((BETWEEN_PASSED++)) || true
    else
        ((BETWEEN_FAILED++)) || true
    fi
done

echo ""
echo "=== Running COMMUTE Tests ==="
COMMUTE_FILES=$(find third_party/sqllogictest/test/index/commute -name "*.test" | sort)
COMMUTE_PASSED=0
COMMUTE_FAILED=0

for test_file in $COMMUTE_FILES; do
    if run_test "$test_file" "commute"; then
        ((COMMUTE_PASSED++)) || true
    else
        ((COMMUTE_FAILED++)) || true
    fi
done

# Calculate totals
TOTAL_TESTS=$((BETWEEN_PASSED + BETWEEN_FAILED + COMMUTE_PASSED + COMMUTE_FAILED))
TOTAL_PASSED=$((BETWEEN_PASSED + COMMUTE_PASSED))
TOTAL_FAILED=$((BETWEEN_FAILED + COMMUTE_FAILED))

if [ $TOTAL_TESTS -gt 0 ]; then
    PASS_RATE=$((TOTAL_PASSED * 100 / TOTAL_TESTS))
else
    PASS_RATE=0
fi

# Summary
echo ""
echo "========================================="
echo "SUMMARY"
echo "========================================="
echo ""
echo "BETWEEN Tests:"
echo "  Passed: $BETWEEN_PASSED"
echo "  Failed: $BETWEEN_FAILED"
echo ""
echo "COMMUTE Tests:"
echo "  Passed: $COMMUTE_PASSED"
echo "  Failed: $COMMUTE_FAILED"
echo ""
echo "TOTAL:"
echo "  Passed: $TOTAL_PASSED / $TOTAL_TESTS"
echo "  Failed: $TOTAL_FAILED / $TOTAL_TESTS"
echo "  Pass Rate: ${PASS_RATE}%"
echo ""

# Save summary to file
cat > "$RESULTS_DIR/SUMMARY.md" << EOF
# Index Ordering Validation Results

**Issue**: #1449
**PR Tested**: #1442 (fix: Preserve index ordering in range_scan and multi_lookup)
**Date**: $(date)

## Summary

| Category | Passed | Failed | Total | Pass Rate |
|----------|--------|--------|-------|-----------|
| BETWEEN  | $BETWEEN_PASSED | $BETWEEN_FAILED | $((BETWEEN_PASSED + BETWEEN_FAILED)) | $((BETWEEN_PASSED * 100 / (BETWEEN_PASSED + BETWEEN_FAILED) ))% |
| COMMUTE  | $COMMUTE_PASSED | $COMMUTE_FAILED | $((COMMUTE_PASSED + COMMUTE_FAILED)) | $((COMMUTE_PASSED * 100 / (COMMUTE_PASSED + COMMUTE_FAILED) ))% |
| **TOTAL** | **$TOTAL_PASSED** | **$TOTAL_FAILED** | **$TOTAL_TESTS** | **${PASS_RATE}%** |

## Details

### BETWEEN Tests ($((BETWEEN_PASSED + BETWEEN_FAILED)) files)
EOF

# Add BETWEEN test results
for result_file in "$RESULTS_DIR"/between_*.result; do
    if [ -f "$result_file" ]; then
        test_name=$(basename "$result_file" .result | sed 's/between_//')
        status=$(cat "$result_file")
        if [ "$status" = "PASS" ]; then
            echo "- ✅ $test_name" >> "$RESULTS_DIR/SUMMARY.md"
        else
            echo "- ❌ $test_name" >> "$RESULTS_DIR/SUMMARY.md"
        fi
    fi
done

cat >> "$RESULTS_DIR/SUMMARY.md" << EOF

### COMMUTE Tests ($((COMMUTE_PASSED + COMMUTE_FAILED)) files)
EOF

# Add COMMUTE test results
for result_file in "$RESULTS_DIR"/commute_*.result; do
    if [ -f "$result_file" ]; then
        test_name=$(basename "$result_file" .result | sed 's/commute_//')
        status=$(cat "$result_file")
        if [ "$status" = "PASS" ]; then
            echo "- ✅ $test_name" >> "$RESULTS_DIR/SUMMARY.md"
        else
            echo "- ❌ $test_name" >> "$RESULTS_DIR/SUMMARY.md"
        fi
    fi
done

echo "Results saved to: $RESULTS_DIR/SUMMARY.md"
echo ""

# Exit with appropriate code
if [ $TOTAL_FAILED -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
    exit 0
else
    echo -e "${YELLOW}⚠ Some tests failed${NC}"
    exit 1
fi
