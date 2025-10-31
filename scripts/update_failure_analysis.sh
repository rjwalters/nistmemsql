#!/bin/bash
# Update FAILURE_ANALYSIS.md from conformance test results
#
# This script analyzes target/sqltest_results.json and updates FAILURE_ANALYSIS.md
# with current test metrics and failure patterns.
#
# Usage:
#   ./scripts/update_failure_analysis.sh           # Use existing test results
#   ./scripts/update_failure_analysis.sh --run     # Run tests first, then analyze

set -e

RESULTS_FILE="target/sqltest_results.json"
OUTPUT_FILE="FAILURE_ANALYSIS.md"

# Check if --run flag is provided
if [ "$1" = "--run" ]; then
    echo "Running conformance tests..."
    cargo test --test sqltest_conformance --release -- --nocapture > /dev/null 2>&1
    echo "✅ Tests completed"
    echo ""
fi

# Check if results file exists
if [ ! -f "$RESULTS_FILE" ]; then
    echo "Error: $RESULTS_FILE not found. Run conformance tests first:"
    echo "  cargo test --test sqltest_conformance --release -- --nocapture"
    echo "Or run this script with --run flag:"
    echo "  ./scripts/update_failure_analysis.sh --run"
    exit 1
fi

# Extract metrics from JSON
TOTAL_TESTS=$(jq -r '.passed + .errors' "$RESULTS_FILE")
PASSED=$(jq -r '.passed' "$RESULTS_FILE")
ERRORS=$(jq -r '.errors' "$RESULTS_FILE")
PASS_RATE=$(jq -r '.pass_rate' "$RESULTS_FILE")
PASS_RATE_FORMATTED=$(printf "%.1f" "$PASS_RATE")

# Calculate tests needed for 90%
TESTS_FOR_90=$(echo "$TOTAL_TESTS * 0.9" | bc | awk '{print int($1+0.5)}')
TESTS_NEEDED=$(echo "$TESTS_FOR_90 - $PASSED" | bc)

# Get current date
CURRENT_DATE=$(date +%Y-%m-%d)

echo "Analyzing test results..."
echo "  Total: $TOTAL_TESTS"
echo "  Passed: $PASSED"
echo "  Errors: $ERRORS"
echo "  Pass Rate: $PASS_RATE_FORMATTED%"
echo "  Tests needed for 90%: $TESTS_NEEDED"

# Group errors by pattern
echo ""
echo "Grouping errors by pattern..."

# Extract error patterns
jq -r '.error_tests[] | "\(.id)|\(.error)"' "$RESULTS_FILE" > /tmp/error_list.txt

# Count SELECT * AS patterns (e051_07, e051_08)
SELECT_AS_COUNT=$(grep -c "e051_0[78]" /tmp/error_list.txt || true)

# Count GRANT/REVOKE patterns (f031_03, f031_19, e081)
GRANT_REVOKE_COUNT=$(grep -E "f031_03|f031_19|e081" /tmp/error_list.txt | wc -l | tr -d ' ')

# Count cursor patterns (e121)
CURSOR_COUNT=$(grep -c "e121" /tmp/error_list.txt || true)

# Count SET statement patterns (e152)
SET_COUNT=$(grep -c "e152" /tmp/error_list.txt || true)

# Count foreign key ON DELETE/UPDATE patterns (e141)
FK_ACTION_COUNT=$(grep "e141.*Expected UPDATE after ON" /tmp/error_list.txt | wc -l | tr -d ' ')

# Count aggregate ALL patterns (e091_06)
AGG_ALL_COUNT=$(grep -c "e091_06" /tmp/error_list.txt || true)

# Count TIMESTAMP WITHOUT TIME ZONE patterns (f051_05)
TIMESTAMP_COUNT=$(grep "f051_05.*WITHOUT TIME ZONE" /tmp/error_list.txt | wc -l | tr -d ' ')

# Count column privileges patterns (f031_03_03, f031_19_02)
COL_PRIV_COUNT=$(grep -E "f031_03_03|f031_19_02" /tmp/error_list.txt | wc -l | tr -d ' ')

echo "  SELECT * AS patterns: $SELECT_AS_COUNT"
echo "  GRANT/REVOKE advanced: $GRANT_REVOKE_COUNT"
echo "  Cursor operations: $CURSOR_COUNT"
echo "  SET statements: $SET_COUNT"
echo "  Foreign key actions: $FK_ACTION_COUNT"
echo "  Aggregate ALL: $AGG_ALL_COUNT"
echo "  TIMESTAMP WITHOUT TIME ZONE: $TIMESTAMP_COUNT"
echo "  Column privileges: $COL_PRIV_COUNT"

# Calculate other
OTHER_COUNT=$(echo "$ERRORS - $SELECT_AS_COUNT - $GRANT_REVOKE_COUNT - $CURSOR_COUNT - $SET_COUNT - $FK_ACTION_COUNT - $AGG_ALL_COUNT - $TIMESTAMP_COUNT - $COL_PRIV_COUNT" | bc)

# Generate the updated document
cat > "$OUTPUT_FILE" << EOF
# SQL:1999 Test Failure Analysis

**Generated**: $CURRENT_DATE (Auto-generated from test results)
**Total Failures**: $ERRORS/$TOTAL_TESTS tests ($(echo "scale=1; $ERRORS * 100 / $TOTAL_TESTS" | bc)%)
**Current Pass Rate**: $PASS_RATE_FORMATTED%
**Target**: 90%+ (need to fix ~$TESTS_NEEDED tests)

---

## Executive Summary

Analysis of the $ERRORS failing tests reveals the following primary failure patterns:

**Top Failure Patterns** (by count):
1. **SELECT * AS (col1, col2)** ($SELECT_AS_COUNT tests) - Derived column lists for wildcards
2. **GRANT/REVOKE advanced syntax** ($GRANT_REVOKE_COUNT tests) - Procedure/Function/Domain/Sequence privileges
3. **Cursor operations** ($CURSOR_COUNT tests) - OPEN, FETCH, CLOSE
4. **TIMESTAMP WITHOUT TIME ZONE** ($TIMESTAMP_COUNT tests) - Parser support needed
5. **SET statement extensions** ($SET_COUNT tests) - Transaction isolation, read-only mode
6. **Foreign key referential actions** ($FK_ACTION_COUNT tests) - ON UPDATE/DELETE actions
7. **Aggregate function ALL syntax** ($AGG_ALL_COUNT tests) - ALL keyword in aggregates
8. **Column-level privileges** ($COL_PRIV_COUNT tests) - GRANT/REVOKE on specific columns
9. **Miscellaneous** ($OTHER_COUNT tests) - Various parser/executor gaps

**Path to 90%+**: Need to fix ~$TESTS_NEEDED more tests. Quick wins available in patterns with smaller test counts.

---

## Detailed Failure Patterns

### Pattern 1: SELECT * AS (col1, col2) - $SELECT_AS_COUNT tests

**Error Message**:
\`\`\`
Statement 2 failed: Parse error: ParseError { message: "Expected Symbol('('), found LParen" }
\`\`\`

**Example SQL**:
\`\`\`sql
SELECT * AS (C, D) FROM table_name
\`\`\`

**Issue**: Parser doesn't support derived column lists (AS clause after wildcards).

**Feature Code**: E051-07, E051-08

**Affected Tests**:
EOF

# Add SELECT AS test IDs
grep "e051_0[78]" /tmp/error_list.txt | cut -d'|' -f1 | sort | tr '\n' ', ' | sed 's/,$//' >> "$OUTPUT_FILE"

cat >> "$OUTPUT_FILE" << 'EOF'


---

### Pattern 2: GRANT/REVOKE Advanced Privileges - tests

**Subcategories**:
- GRANT/REVOKE on FUNCTION/PROCEDURE/METHOD
- GRANT/REVOKE on DOMAIN/SEQUENCE/TRANSLATION
- GRANT/REVOKE on CHARACTER SET/COLLATION/TYPE
- Column-level privileges (GRANT SELECT(col) ON table)

**Error Messages**:
```
Expected keyword To/From, found Keyword(Function/Procedure/Domain/Sequence)
Expected keyword On, found LParen (for column privileges)
```

**Issue**: Parser doesn't recognize advanced privilege targets beyond tables and schemas.

**Affected Test Patterns**: f031_03_*, f031_19_*, e081_*

---

### Pattern 3: Cursor Operations - tests

**Error Message**:
```
Statement failed: Parse error: ParseError { message: "Expected statement, found Identifier(\"OPEN\")" }
```

**Example SQL**:
```sql
DECLARE cur1 CURSOR FOR SELECT * FROM t1;
OPEN cur1;
FETCH FROM cur1;
CLOSE cur1;
```

**Issue**: No cursor support (procedural SQL feature).

**Feature Code**: E121 (Cursor support)

**Affected Test Pattern**: e121_*

---

### Pattern 4: TIMESTAMP WITHOUT TIME ZONE - tests

**Error Message**:
```
Parse error: ParseError { message: "Expected RParen, found Keyword(Without)" }
```

**Example SQL**:
```sql
CAST('2016-03-26 01:02:03' AS TIMESTAMP WITHOUT TIME ZONE)
```

**Issue**: Parser doesn't support TIMESTAMP WITHOUT TIME ZONE syntax.

**Affected Test Pattern**: f051_05_*

---

### Pattern 5: SET Statement Extensions - tests

**Error Messages**:
```
Expected SCHEMA, CATALOG, NAMES, or TIME ZONE after SET
```

**Example SQL**:
```sql
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SET TRANSACTION READ ONLY;
SET LOCAL TRANSACTION ...;
```

**Issue**: SET statement only supports SCHEMA, needs transaction-related extensions.

**Affected Test Pattern**: e152_*

---

### Pattern 6: Foreign Key Referential Actions - tests

**Error Message**:
```
Parse error: ParseError { message: "Expected UPDATE after ON" }
```

**Example SQL**:
```sql
CREATE TABLE t2 (
  b INT REFERENCES t1(a) ON UPDATE NO ACTION ON DELETE NO ACTION
);
```

**Issue**: Parser expects only "ON UPDATE" not "ON DELETE" for foreign key actions.

**Affected Test Pattern**: e141_04_*

---

### Pattern 7: Aggregate Function ALL Syntax - tests

**Error Message**:
```
Parse error: ParseError { message: "Expected expression, found Keyword(All)" }
```

**Example SQL**:
```sql
SELECT AVG(ALL col) FROM table;
SELECT COUNT(ALL col) FROM table;
```

**Issue**: Parser doesn't support ALL keyword in aggregate functions.

**Affected Test Pattern**: e091_06_*

---

### Pattern 8: Miscellaneous Parse Errors - ~tests

Various one-off parsing and execution issues requiring individual investigation.

---

## Prioritized Action Plan

### Quick Wins (Highest ROI)

1. **Aggregate ALL syntax** ($AGG_ALL_COUNT tests) - Simple parser fix
2. **SET transaction statements** ($SET_COUNT tests) - Parser extension
3. **Foreign key actions** ($FK_ACTION_COUNT tests) - Parser fix

**Estimated Impact**: ~$(echo "$AGG_ALL_COUNT + $SET_COUNT + $FK_ACTION_COUNT" | bc) tests, moderate effort

### Medium-Term Fixes

4. **SELECT * AS aliasing** ($SELECT_AS_COUNT tests) - Parser + executor work
5. **TIMESTAMP WITHOUT TIME ZONE** ($TIMESTAMP_COUNT tests) - Type system extension
6. **Column-level privileges** ($COL_PRIV_COUNT tests) - Security model enhancement

### Long-Term Features

7. **GRANT/REVOKE advanced** ($GRANT_REVOKE_COUNT tests) - Requires stub objects
8. **Cursor operations** ($CURSOR_COUNT tests) - Major procedural SQL feature

---

## Implementation Notes

**Auto-generated from**: target/sqltest_results.json
**Last test run**: $CURRENT_DATE
**Test suite**: SQL:1999 conformance tests (upstream YAML)

To regenerate this file:
\`\`\`bash
cargo test --test sqltest_conformance --release -- --nocapture
./scripts/update_failure_analysis.sh
\`\`\`

EOF

# Count by pattern number
GRANT_REVOKE_PATTERN=$(grep -E "f031_03|f031_19|e081" /tmp/error_list.txt | wc -l | tr -d ' ')
CURSOR_PATTERN=$(grep "e121" /tmp/error_list.txt | wc -l | tr -d ' ')
TIMESTAMP_PATTERN=$(grep "f051_05.*WITHOUT TIME ZONE" /tmp/error_list.txt | wc -l | tr -d ' ')

# Update the count placeholders
sed -i '' "s/Pattern 2: GRANT\/REVOKE Advanced Privileges - tests/Pattern 2: GRANT\/REVOKE Advanced Privileges - $GRANT_REVOKE_PATTERN tests/" "$OUTPUT_FILE"
sed -i '' "s/Pattern 3: Cursor Operations - tests/Pattern 3: Cursor Operations - $CURSOR_PATTERN tests/" "$OUTPUT_FILE"
sed -i '' "s/Pattern 4: TIMESTAMP WITHOUT TIME ZONE - tests/Pattern 4: TIMESTAMP WITHOUT TIME ZONE - $TIMESTAMP_PATTERN tests/" "$OUTPUT_FILE"
sed -i '' "s/Pattern 5: SET Statement Extensions - tests/Pattern 5: SET Statement Extensions - $SET_COUNT tests/" "$OUTPUT_FILE"
sed -i '' "s/Pattern 6: Foreign Key Referential Actions - tests/Pattern 6: Foreign Key Referential Actions - $FK_ACTION_COUNT tests/" "$OUTPUT_FILE"
sed -i '' "s/Pattern 7: Aggregate Function ALL Syntax - tests/Pattern 7: Aggregate Function ALL Syntax - $AGG_ALL_COUNT tests/" "$OUTPUT_FILE"
sed -i '' "s/Pattern 8: Miscellaneous Parse Errors - ~tests/Pattern 8: Miscellaneous Parse Errors - $OTHER_COUNT tests/" "$OUTPUT_FILE"

# Clean up
rm /tmp/error_list.txt

echo ""
echo "✅ Updated $OUTPUT_FILE with current test results"
echo "   Pass rate: $PASS_RATE_FORMATTED% ($PASSED\/$TOTAL_TESTS)"
echo "   Failures analyzed: $ERRORS"
echo ""
