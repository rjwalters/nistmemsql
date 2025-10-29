#!/bin/bash
set -e

# Generate SQL:1999 Conformance Report

SQLTEST_RESULTS="target/sqltest_results.json"
OUTPUT="docs/SQL1999_CONFORMANCE.md"

# Ensure docs directory exists
mkdir -p docs

echo "# SQL:1999 Conformance Report" > "$OUTPUT"
echo "" >> "$OUTPUT"
echo "**Generated**: $(date)" >> "$OUTPUT"
echo "**Commit**: $(git rev-parse --short HEAD 2>/dev/null || echo 'unknown')" >> "$OUTPUT"
echo "" >> "$OUTPUT"

# Parse sqltest results if they exist
if [ -f "$SQLTEST_RESULTS" ]; then
    echo "## Summary" >> "$OUTPUT"
    echo "" >> "$OUTPUT"
    
    # Extract metrics from JSON using basic shell tools
    TOTAL=$(grep -o '"total":[[:space:]]*[0-9]*' "$SQLTEST_RESULTS" | sed 's/"total":[[:space:]]*//g')
    PASSED=$(grep -o '"passed":[[:space:]]*[0-9]*' "$SQLTEST_RESULTS" | sed 's/"passed":[[:space:]]*//g')
    FAILED=$(grep -o '"failed":[[:space:]]*[0-9]*' "$SQLTEST_RESULTS" | sed 's/"failed":[[:space:]]*//g')
    ERRORS=$(grep -o '"errors":[[:space:]]*[0-9]*' "$SQLTEST_RESULTS" | sed 's/"errors":[[:space:]]*//g')
    PASS_RATE_RAW=$(grep -o '"pass_rate":[[:space:]]*[0-9.]*' "$SQLTEST_RESULTS" | sed 's/"pass_rate":[[:space:]]*//g')
    # Round to 1 decimal place
    PASS_RATE=$(printf "%.1f" "$PASS_RATE_RAW")
    
    echo "| Metric | Value |" >> "$OUTPUT"
    echo "|--------|-------|" >> "$OUTPUT"
    echo "| Total Tests | $TOTAL |" >> "$OUTPUT"
    echo "| Passed | $PASSED ✅ |" >> "$OUTPUT"
    echo "| Failed | $FAILED ❌ |" >> "$OUTPUT"
    echo "| Errors | $ERRORS ⚠️ |" >> "$OUTPUT"
    echo "| Pass Rate | ${PASS_RATE}% |" >> "$OUTPUT"
    echo "" >> "$OUTPUT"
    
    echo "## Test Coverage" >> "$OUTPUT"
    echo "" >> "$OUTPUT"
    echo "Test suite from [sqltest](https://github.com/elliotchance/sqltest) - upstream-recommended SQL:1999 conformance tests." >> "$OUTPUT"
    echo "" >> "$OUTPUT"
    echo "Coverage includes:" >> "$OUTPUT"
    echo "" >> "$OUTPUT"
    echo "- **E011**: Numeric data types" >> "$OUTPUT"
    echo "- **E021**: Character string types" >> "$OUTPUT"
    echo "- **E031**: Identifiers" >> "$OUTPUT"
    echo "- **E051**: Basic query specification" >> "$OUTPUT"
    echo "- **E061**: Basic predicates and search conditions" >> "$OUTPUT"
    echo "- **E071**: Basic query expressions" >> "$OUTPUT"
    echo "- **E081**: Basic privileges" >> "$OUTPUT"
    echo "- **E091**: Set functions" >> "$OUTPUT"
    echo "- **E101**: Basic data manipulation" >> "$OUTPUT"
    echo "- **E111**: Single row SELECT statement" >> "$OUTPUT"
    echo "- **E121**: Basic cursor support" >> "$OUTPUT"
    echo "- **E131**: Null value support" >> "$OUTPUT"
    echo "- **E141**: Basic integrity constraints" >> "$OUTPUT"
    echo "- **E151**: Transaction support" >> "$OUTPUT"
    echo "- **E161**: SQL comments" >> "$OUTPUT"
    echo "- **F031**: Basic schema manipulation" >> "$OUTPUT"
    echo "- Plus additional features from the F-series" >> "$OUTPUT"
    echo "" >> "$OUTPUT"

    # Show sample of failing tests if there are errors
    ERROR_COUNT=$(echo "$ERRORS" | sed 's/^0$//')
    if [ -n "$ERROR_COUNT" ] && [ "$ERROR_COUNT" != "0" ]; then
        echo "## Sample Failing Tests" >> "$OUTPUT"
        echo "" >> "$OUTPUT"
        echo "The following tests are currently failing (showing first 10):" >> "$OUTPUT"
        echo "" >> "$OUTPUT"

        # Extract first 10 error tests from JSON
        if command -v jq >/dev/null 2>&1; then
            jq -r '.error_tests[:10] | .[] | "- **\(.id)**: \(.sql)\n  - Error: \(.error)"' "$SQLTEST_RESULTS" >> "$OUTPUT" 2>/dev/null || \
                echo "*Error details unavailable - install jq for detailed output*" >> "$OUTPUT"
        else
            echo "*Install jq to see detailed test failures*" >> "$OUTPUT"
        fi
        echo "" >> "$OUTPUT"
        echo "<details>" >> "$OUTPUT"
        echo "<summary>View all failing tests (click to expand)</summary>" >> "$OUTPUT"
        echo "" >> "$OUTPUT"
        echo "\`\`\`" >> "$OUTPUT"
        if command -v jq >/dev/null 2>&1; then
            jq -r '.error_tests | .[] | "\(.id): \(.sql)\n  Error: \(.error)\n"' "$SQLTEST_RESULTS" >> "$OUTPUT" 2>/dev/null || \
                echo "Error details unavailable" >> "$OUTPUT"
        else
            echo "Install jq to see detailed test failures" >> "$OUTPUT"
        fi
        echo "\`\`\`" >> "$OUTPUT"
        echo "" >> "$OUTPUT"
        echo "</details>" >> "$OUTPUT"
        echo "" >> "$OUTPUT"
    fi
else
    echo "⚠️ No test results found at $SQLTEST_RESULTS" >> "$OUTPUT"
    echo "" >> "$OUTPUT"
    echo "Run \`cargo test --test sqltest_conformance\` to generate results." >> "$OUTPUT"
    echo "" >> "$OUTPUT"
fi

echo "## Running Tests Locally" >> "$OUTPUT"
echo "" >> "$OUTPUT"
echo "\`\`\`bash" >> "$OUTPUT"
echo "# Run all conformance tests" >> "$OUTPUT"
echo "cargo test --test sqltest_conformance -- --nocapture" >> "$OUTPUT"
echo "" >> "$OUTPUT"
echo "# Generate coverage report" >> "$OUTPUT"
echo "cargo coverage" >> "$OUTPUT"
echo "# Open coverage report" >> "$OUTPUT"
echo "open target/llvm-cov/html/index.html" >> "$OUTPUT"
echo "\`\`\`" >> "$OUTPUT"
echo "" >> "$OUTPUT"

echo "✅ Conformance report generated: $OUTPUT"
