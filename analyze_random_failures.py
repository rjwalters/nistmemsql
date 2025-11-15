#!/usr/bin/env python3
"""
Analyze random test failures and categorize by error pattern.

This script reads test results from the database and categorizes failures
into patterns to help prioritize fixes.
"""

import subprocess
import re
from collections import defaultdict
from pathlib import Path

# Get repo root
def get_repo_root():
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        check=True
    )
    return Path(result.stdout.strip())

# Execute query using query_test_results.py
def query_db(sql):
    repo_root = get_repo_root()
    script = repo_root / "scripts" / "query_test_results.py"

    result = subprocess.run(
        [str(script), "--query", sql],
        capture_output=True,
        text=True
    )

    return result.stdout if result.returncode == 0 else None

# Get all random test failures
print("Fetching random test failures...")
failures_query = """
SELECT
    file_path,
    error_message
FROM test_results
WHERE status = 'FAIL'
    AND file_path LIKE '%/random/%'
ORDER BY file_path
"""

output = query_db(failures_query)
if not output:
    print("Error: Could not fetch failures from database")
    exit(1)

# Parse output to extract file paths and error messages
# The output is in table format, so we need to parse it
lines = output.strip().split('\n')

# Find where the table data starts (after the header separator)
data_start = 0
for i, line in enumerate(lines):
    if line.startswith('+---'):
        data_start = i + 1
        break

# Parse table rows
failures = []
for line in lines[data_start:]:
    if line.startswith('|') and not line.startswith('+'):
        # Split by | and clean up
        parts = [p.strip() for p in line.split('|')]
        if len(parts) >= 3:  # Should have: empty, file_path, error_message, empty
            # Remove Varchar() wrapper
            file_path = parts[1]
            error_msg = parts[2]

            # Clean up Varchar() wrappers
            if 'Varchar("' in file_path:
                file_path = re.search(r'Varchar\("([^"]+)"\)', file_path)
                if file_path:
                    file_path = file_path.group(1)
                else:
                    continue

            if 'Varchar("' in error_msg:
                error_msg = re.search(r'Varchar\("(.+)"\)', error_msg)
                if error_msg:
                    error_msg = error_msg.group(1)
                else:
                    error_msg = ""

            failures.append((file_path, error_msg))

print(f"Found {len(failures)} random test failures\n")

# Categorize failures by error pattern
patterns = defaultdict(list)

for file_path, error_msg in failures:
    # Extract subcategory from path
    subcategory = "unknown"
    if "/random/aggregates/" in file_path:
        subcategory = "aggregates"
    elif "/random/expr/" in file_path:
        subcategory = "expr"
    elif "/random/select/" in file_path:
        subcategory = "select"
    elif "/random/groupby/" in file_path:
        subcategory = "groupby"

    # Categorize by error pattern
    pattern_key = None

    if "NULL + 0" in error_msg or "NULL + -" in error_msg or "NULL + " in error_msg:
        pattern_key = "NULL_AGGREGATE_RETURNS_ZERO"
    elif "Unsupported" in error_msg or "unsupported" in error_msg:
        pattern_key = "UNSUPPORTED_FEATURE"
    elif "Parse error" in error_msg or "parse error" in error_msg:
        pattern_key = "PARSE_ERROR"
    elif "Type mismatch" in error_msg or "type mismatch" in error_msg:
        pattern_key = "TYPE_MISMATCH"
    elif "Division by zero" in error_msg or "division by zero" in error_msg:
        pattern_key = "DIVISION_BY_ZERO"
    elif "query result mismatch" in error_msg:
        # Further categorize result mismatches
        if "ORDER BY" in error_msg:
            pattern_key = "RESULT_MISMATCH_ORDERBY"
        elif "GROUP BY" in error_msg:
            pattern_key = "RESULT_MISMATCH_GROUPBY"
        elif "BETWEEN" in error_msg:
            pattern_key = "RESULT_MISMATCH_BETWEEN"
        elif "IN (" in error_msg or "NOT IN" in error_msg:
            pattern_key = "RESULT_MISMATCH_IN"
        elif "CAST" in error_msg:
            pattern_key = "RESULT_MISMATCH_CAST"
        elif "SUM" in error_msg or "COUNT" in error_msg or "AVG" in error_msg or "MAX" in error_msg or "MIN" in error_msg:
            pattern_key = "RESULT_MISMATCH_AGGREGATE"
        else:
            pattern_key = "RESULT_MISMATCH_OTHER"
    elif "Aggregate" in error_msg or "aggregate" in error_msg:
        pattern_key = "AGGREGATE_ERROR"
    elif "Column not found" in error_msg or "column not found" in error_msg:
        pattern_key = "COLUMN_NOT_FOUND"
    else:
        pattern_key = "OTHER_ERROR"

    patterns[pattern_key].append((file_path, subcategory, error_msg))

# Print summary
print("=" * 80)
print("FAILURE PATTERN ANALYSIS - RANDOM TESTS")
print("=" * 80)
print()

# Sort patterns by count
sorted_patterns = sorted(patterns.items(), key=lambda x: len(x[1]), reverse=True)

for pattern, failures_list in sorted_patterns:
    print(f"\n{pattern}")
    print(f"  Total failures: {len(failures_list)}")

    # Count by subcategory
    subcategory_counts = defaultdict(int)
    for _, subcategory, _ in failures_list:
        subcategory_counts[subcategory] += 1

    print(f"  By subcategory:")
    for subcat, count in sorted(subcategory_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"    {subcat}: {count}")

    # Show a few examples
    print(f"  Example files:")
    for file_path, _, _ in failures_list[:3]:
        print(f"    - {file_path}")

    # Show a sample error message
    if failures_list:
        sample_error = failures_list[0][2]
        # Truncate long messages
        if len(sample_error) > 150:
            sample_error = sample_error[:150] + "..."
        print(f"  Sample error: {sample_error}")
    print()

print("=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"Total failures analyzed: {len(failures)}")
print(f"Unique patterns identified: {len(patterns)}")
print()

# Calculate percentages
print("Top patterns by impact:")
for i, (pattern, failures_list) in enumerate(sorted_patterns[:10], 1):
    pct = (len(failures_list) / len(failures)) * 100
    print(f"{i}. {pattern}: {len(failures_list)} ({pct:.1f}%)")
