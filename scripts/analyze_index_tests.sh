#!/bin/bash
# Analysis script for index test failures (Issue #1233)

set -e

echo "Index Test Failure Analysis"
echo "============================"
echo ""

# Check if test results database exists
DB_PATH="$HOME/.vibesql/test_results/test_results.db"
if [ ! -f "$DB_PATH" ]; then
    echo "❌ No test results database found at $DB_PATH"
    echo "Run tests first: ./scripts/sqllogictest run --parallel --workers 8"
    exit 1
fi

echo "📊 Analyzing test results from: $DB_PATH"
echo ""

# Overall index test stats
echo "Overall Index Test Statistics:"
echo "-----------------------------"
./scripts/query_test_results.py --query "
SELECT
  COUNT(*) as total,
  SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) as passed,
  SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) as failed,
  ROUND(100.0 * SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) / COUNT(*), 1) as pass_pct
FROM test_files
WHERE file_path LIKE 'index/%'
"
echo ""

# Failures by subcategory
echo "Failures by Subcategory:"
echo "-----------------------"
./scripts/query_test_results.py --query "
SELECT
  CASE
    WHEN file_path LIKE 'index/between/%' THEN 'between'
    WHEN file_path LIKE 'index/commute/%' THEN 'commute'
    WHEN file_path LIKE 'index/delete/%' THEN 'delete'
    WHEN file_path LIKE 'index/in/%' THEN 'in'
    WHEN file_path LIKE 'index/orderby/%' THEN 'orderby'
    WHEN file_path LIKE 'index/orderby_nosort/%' THEN 'orderby_nosort'
    WHEN file_path LIKE 'index/random/%' THEN 'random'
    WHEN file_path LIKE 'index/view/%' THEN 'view'
    ELSE 'other'
  END as subcategory,
  COUNT(*) as total,
  SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) as passed,
  SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) as failed,
  ROUND(100.0 * SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) / COUNT(*), 1) as pass_pct
FROM test_files
WHERE file_path LIKE 'index/%'
GROUP BY subcategory
ORDER BY failed DESC, pass_pct ASC
"
echo ""

# Sample failing tests from top 3 categories
echo "Sample Failing Tests (Top 3 Categories):"
echo "---------------------------------------"
./scripts/query_test_results.py --query "
WITH category_failures AS (
  SELECT
    CASE
      WHEN file_path LIKE 'index/between/%' THEN 'between'
      WHEN file_path LIKE 'index/commute/%' THEN 'commute'
      WHEN file_path LIKE 'index/delete/%' THEN 'delete'
      WHEN file_path LIKE 'index/in/%' THEN 'in'
      WHEN file_path LIKE 'index/orderby/%' THEN 'orderby'
      WHEN file_path LIKE 'index/orderby_nosort/%' THEN 'orderby_nosort'
      WHEN file_path LIKE 'index/random/%' THEN 'random'
      WHEN file_path LIKE 'index/view/%' THEN 'view'
      ELSE 'other'
    END as subcategory,
    file_path,
    status
  FROM test_files
  WHERE file_path LIKE 'index/%' AND status = 'FAIL'
),
top_categories AS (
  SELECT subcategory, COUNT(*) as failures
  FROM category_failures
  GROUP BY subcategory
  ORDER BY failures DESC
  LIMIT 3
)
SELECT cf.file_path
FROM category_failures cf
JOIN top_categories tc ON cf.subcategory = tc.subcategory
GROUP BY cf.subcategory, cf.file_path
HAVING cf.subcategory IN (SELECT subcategory FROM top_categories)
ORDER BY cf.subcategory, cf.file_path
LIMIT 10
"
echo ""

echo "✅ Analysis complete!"
echo ""
echo "Next steps:"
echo "1. Run individual failing tests to examine errors"
echo "2. Group failures by error pattern"
echo "3. Create targeted issues for each pattern"
