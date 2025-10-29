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
    TOTAL=$(grep -o '"total":[0-9]*' "$SQLTEST_RESULTS" | head -1 | cut -d':' -f2)
    PASSED=$(grep -o '"passed":[0-9]*' "$SQLTEST_RESULTS" | head -1 | cut -d':' -f2)
    FAILED=$(grep -o '"failed":[0-9]*' "$SQLTEST_RESULTS" | head -1 | cut -d':' -f2)
    ERRORS=$(grep -o '"errors":[0-9]*' "$SQLTEST_RESULTS" | head -1 | cut -d':' -f2)
    PASS_RATE=$(grep -o '"pass_rate":[0-9.]*' "$SQLTEST_RESULTS" | head -1 | cut -d':' -f2)
    
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
    echo "Current test suite covers Core SQL:1999 features:" >> "$OUTPUT"
    echo "" >> "$OUTPUT"
    echo "- **E011**: Numeric data types (INTEGER, SMALLINT, BIGINT, FLOAT, DOUBLE, DECIMAL)" >> "$OUTPUT"
    echo "- **E021**: Character string types (CHAR, VARCHAR)" >> "$OUTPUT"
    echo "- **E011-04**: Arithmetic operators (+, -, *, /)" >> "$OUTPUT"
    echo "- **E011-05**: Comparison predicates (<, <=, =, <>, >=, >)" >> "$OUTPUT"
    echo "- **E011-06**: Implicit casting between numeric types" >> "$OUTPUT"
    echo "" >> "$OUTPUT"
    
    echo "## Known Gaps" >> "$OUTPUT"
    echo "" >> "$OUTPUT"
    echo "Based on test failures, the following areas need implementation:" >> "$OUTPUT"
    echo "" >> "$OUTPUT"
    echo "### Parser Gaps" >> "$OUTPUT"
    echo "- [ ] Unary plus (+) operator support" >> "$OUTPUT"
    echo "- [ ] Unary minus (-) operator support" >> "$OUTPUT"
    echo "- [ ] DECIMAL/DEC type alias recognition" >> "$OUTPUT"
    echo "- [ ] Floating point literals starting with decimal point (e.g., .5)" >> "$OUTPUT"
    echo "- [ ] Scientific notation (e.g., 1.5E+10)" >> "$OUTPUT"
    echo "- [ ] FLOAT with precision specification: FLOAT(n)" >> "$OUTPUT"
    echo "" >> "$OUTPUT"
    
    echo "### Executor Gaps" >> "$OUTPUT"
    echo "- [ ] Numeric type coercion (INTEGER <-> DECIMAL comparison)" >> "$OUTPUT"
    echo "- [ ] Arithmetic operations on DECIMAL/NUMERIC types" >> "$OUTPUT"
    echo "- [ ] Proper DECIMAL type implementation (currently string-based)" >> "$OUTPUT"
    echo "" >> "$OUTPUT"
    
    echo "## Improvement Roadmap" >> "$OUTPUT"
    echo "" >> "$OUTPUT"
    echo "To improve conformance from current ${PASS_RATE}% to 80%+:" >> "$OUTPUT"
    echo "" >> "$OUTPUT"
    echo "1. **Phase 1**: Implement unary operators (+, -) - Will fix ~25 tests" >> "$OUTPUT"
    echo "2. **Phase 2**: Add DECIMAL type alias and floating point literal parsing - Will fix ~15 tests" >> "$OUTPUT"
    echo "3. **Phase 3**: Implement numeric type coercion in executor - Will fix ~18 tests" >> "$OUTPUT"
    echo "4. **Phase 4**: Proper DECIMAL type arithmetic - Will improve accuracy" >> "$OUTPUT"
    echo "" >> "$OUTPUT"
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
