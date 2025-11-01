#!/bin/bash
set -e

# Check SQLLogicTest score and generate report
# This script runs the SQLLogicTest suite and displays results

RESULTS_FILE="target/sqllogictest_results.json"

echo "=================================================="
echo "  SQLLogicTest Conformance Check"
echo "=================================================="
echo ""

# Check if submodule is initialized
if [ ! -d "third_party/sqllogictest/test" ]; then
    echo "❌ SQLLogicTest submodule not initialized"
    echo ""
    echo "Please run:"
    echo "  git submodule update --init --recursive"
    echo ""
    exit 1
fi

# Run the test suite
echo "Running SQLLogicTest suite..."
echo "(This may take several minutes)"
echo ""

cargo test run_sqllogictest_suite -- --nocapture 2>&1 | tee /tmp/sqllogictest_output.txt

# Check if results file was created
if [ ! -f "$RESULTS_FILE" ]; then
    echo ""
    echo "❌ Results file not found: $RESULTS_FILE"
    exit 1
fi

# Parse and display results
echo ""
echo "=================================================="
echo "  SQLLogicTest Results"
echo "=================================================="
echo ""

if command -v jq &> /dev/null; then
    # Use jq for pretty output if available
    TOTAL=$(jq -r '.total' "$RESULTS_FILE")
    PASSED=$(jq -r '.passed' "$RESULTS_FILE")
    FAILED=$(jq -r '.failed' "$RESULTS_FILE")
    ERRORS=$(jq -r '.errors' "$RESULTS_FILE")
    SKIPPED=$(jq -r '.skipped' "$RESULTS_FILE")
    PASS_RATE=$(jq -r '.pass_rate' "$RESULTS_FILE")

    echo "Total Files:    $TOTAL"
    echo "Passed:         $PASSED"
    echo "Failed:         $FAILED"
    echo "Errors:         $ERRORS"
    echo "Skipped:        $SKIPPED (vendor-specific)"
    echo ""

    RELEVANT=$((TOTAL - SKIPPED))
    echo "Relevant Tests: $RELEVANT"
    printf "Pass Rate:      %.1f%%\n" "$PASS_RATE"
    echo ""

    # Category breakdown
    echo "By Category:"
    echo "  select:   $(jq -r '.categories.select.pass_rate // 0' "$RESULTS_FILE")% ($(jq -r '.categories.select.passed // 0' "$RESULTS_FILE")/$(jq -r '(.categories.select.total // 0) - (.categories.select.skipped // 0)' "$RESULTS_FILE"))"
    echo "  evidence: $(jq -r '.categories.evidence.pass_rate // 0' "$RESULTS_FILE")% ($(jq -r '.categories.evidence.passed // 0' "$RESULTS_FILE")/$(jq -r '(.categories.evidence.total // 0) - (.categories.evidence.skipped // 0)' "$RESULTS_FILE"))"
    echo "  index:    $(jq -r '.categories.index.pass_rate // 0' "$RESULTS_FILE")% ($(jq -r '.categories.index.passed // 0' "$RESULTS_FILE")/$(jq -r '(.categories.index.total // 0) - (.categories.index.skipped // 0)' "$RESULTS_FILE"))"
    echo "  random:   $(jq -r '.categories.random.pass_rate // 0' "$RESULTS_FILE")% ($(jq -r '.categories.random.passed // 0' "$RESULTS_FILE")/$(jq -r '(.categories.random.total // 0) - (.categories.random.skipped // 0)' "$RESULTS_FILE"))"
    echo "  ddl:      $(jq -r '.categories.ddl.pass_rate // 0' "$RESULTS_FILE")% ($(jq -r '.categories.ddl.passed // 0' "$RESULTS_FILE")/$(jq -r '(.categories.ddl.total // 0) - (.categories.ddl.skipped // 0)' "$RESULTS_FILE"))"
    echo ""

    # Status message based on pass rate
    if (( $(echo "$PASS_RATE >= 50" | bc -l 2>/dev/null || echo "0") )); then
        echo "✅ Status: Good progress!"
    elif (( $(echo "$PASS_RATE >= 10" | bc -l 2>/dev/null || echo "0") )); then
        echo "⚠️  Status: Making progress"
    else
        echo "🚧 Status: Early stage - many features still needed"
    fi
else
    # Fallback to basic grep if jq not available
    cat "$RESULTS_FILE"
fi

echo ""
echo "Results saved to: $RESULTS_FILE"
echo ""
