#!/bin/bash
set -e

# generate_badges.sh - Generate shields.io badge JSON files from test results
#
# Usage: ./scripts/generate_badges.sh <output_dir> [sqltest_results.json] [sqllogictest_cumulative.json]
#
# This script consolidates badge generation logic that was duplicated across
# ci-and-deploy.yml, boost-sqllogictest.yml, and redeploy-web-demo.yml

OUTPUT_DIR="${1:?Output directory required}"
SQLTEST_RESULTS="${2:-}"
SQLLOGICTEST_CUMULATIVE="${3:-}"

mkdir -p "$OUTPUT_DIR"

echo "=== Generating Badge JSON ==="

# ============================================================================
# SQL:1999 Conformance Badge (from sqltest results)
# ============================================================================
if [ -n "$SQLTEST_RESULTS" ] && [ -f "$SQLTEST_RESULTS" ]; then
  echo "Generating SQL:1999 badge from $SQLTEST_RESULTS..."

  PASS_RATE=$(jq -r '.pass_rate // "0"' "$SQLTEST_RESULTS" | xargs printf "%.1f")

  # Determine badge color
  if (( $(echo "$PASS_RATE >= 80" | bc -l) )); then
    COLOR="brightgreen"
  elif (( $(echo "$PASS_RATE >= 60" | bc -l) )); then
    COLOR="green"
  elif (( $(echo "$PASS_RATE >= 40" | bc -l) )); then
    COLOR="yellow"
  elif (( $(echo "$PASS_RATE >= 20" | bc -l) )); then
    COLOR="orange"
  else
    COLOR="red"
  fi

  # Create SQL:1999 badge JSON
  cat > "$OUTPUT_DIR/sql1999-conformance.json" <<JSON
{
  "schemaVersion": 1,
  "label": "SQL:1999",
  "message": "${PASS_RATE}%",
  "color": "$COLOR"
}
JSON

  echo "✓ Generated SQL:1999 badge: ${PASS_RATE}% ($COLOR)"
else
  echo "⚠️  Skipping SQL:1999 badge (no sqltest results provided)"
fi

# ============================================================================
# SQLLogicTest Badge (from cumulative results)
# ============================================================================
if [ -n "$SQLLOGICTEST_CUMULATIVE" ] && [ -f "$SQLLOGICTEST_CUMULATIVE" ]; then
  echo "Generating SQLLogicTest badge from $SQLLOGICTEST_CUMULATIVE..."

  # Validate JSON before processing
  if ! jq empty "$SQLLOGICTEST_CUMULATIVE" 2>/dev/null; then
    echo "⚠️  Invalid JSON in $SQLLOGICTEST_CUMULATIVE - skipping SQLLogicTest badge"
  elif ! jq -e '.summary' "$SQLLOGICTEST_CUMULATIVE" >/dev/null 2>&1; then
    echo "⚠️  No .summary in $SQLLOGICTEST_CUMULATIVE - skipping SQLLogicTest badge"
  else
    SLT_PASS_RATE=$(jq -r '.summary.pass_rate // "0"' "$SQLLOGICTEST_CUMULATIVE" | xargs printf "%.1f")
    SLT_COVERAGE=$(jq -r '.summary.coverage_rate // "0"' "$SQLLOGICTEST_CUMULATIVE" | xargs printf "%.1f")
    SLT_PASSED=$(jq -r '.summary.passed // "0"' "$SQLLOGICTEST_CUMULATIVE")
    SLT_FAILED=$(jq -r '.summary.failed // "0"' "$SQLLOGICTEST_CUMULATIVE")

    # Determine badge color (based on coverage rate)
    if (( $(echo "$SLT_COVERAGE >= 80" | bc -l) )); then
      SLT_COLOR="brightgreen"
    elif (( $(echo "$SLT_COVERAGE >= 60" | bc -l) )); then
      SLT_COLOR="green"
    elif (( $(echo "$SLT_COVERAGE >= 40" | bc -l) )); then
      SLT_COLOR="yellow"
    elif (( $(echo "$SLT_COVERAGE >= 20" | bc -l) )); then
      SLT_COLOR="orange"
    else
      SLT_COLOR="red"
    fi

    # Create SQLLogicTest badge JSON (showing coverage)
    cat > "$OUTPUT_DIR/sqllogictest.json" <<JSON
{
  "schemaVersion": 1,
  "label": "SQLLogicTest",
  "message": "${SLT_PASSED}✓ ${SLT_FAILED}✗ (${SLT_COVERAGE}% tested)",
  "color": "$SLT_COLOR"
}
JSON

    echo "✓ Generated SQLLogicTest badge: ${SLT_PASSED}✓ ${SLT_FAILED}✗ (${SLT_COVERAGE}% coverage, $SLT_COLOR)"
  fi
else
  echo "⚠️  Skipping SQLLogicTest badge (no cumulative results provided)"
fi

echo ""
echo "=== Badge Generation Complete ==="
ls -la "$OUTPUT_DIR"/*.json 2>/dev/null || echo "No badges generated"
