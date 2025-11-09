#!/usr/bin/env python3
"""
Interactive query tool for SQLLogicTest results.

This script provides easy access to the test results database with preset queries
for common tasks and support for custom SQL queries.

Usage:
    # Run preset queries
    ./scripts/query_test_results.py --preset failed-files
    ./scripts/query_test_results.py --preset progress
    ./scripts/query_test_results.py --preset by-category

    # Custom SQL query
    ./scripts/query_test_results.py --query "SELECT * FROM test_files WHERE status='FAIL'"

    # List available presets
    ./scripts/query_test_results.py --list-presets

Preset Queries:
    failed-files    - Show all currently failing test files
    progress        - Show test pass rate over recent runs
    by-category     - Show pass rate grouped by category
    flaky-tests     - Find tests that sometimes pass, sometimes fail
    recent-runs     - Show recent test run summary
    untested        - Show files that haven't been tested yet
    slow-tests      - Show slowest running tests
"""

import argparse
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple


def get_repo_root() -> Path:
    """Find the repository root directory."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / ".git").exists():
            return current
        current = current.parent
    raise RuntimeError("Could not find git repository root")


# Preset queries for common tasks
PRESET_QUERIES = {
    'failed-files': {
        'description': 'Show all currently failing test files',
        'query': """
SELECT
    file_path,
    category,
    last_tested
FROM test_files
WHERE status = 'FAIL'
ORDER BY category, file_path
"""
    },

    'progress': {
        'description': 'Show test pass rate over recent runs',
        'query': """
SELECT
    run_id,
    datetime(started_at) as run_time,
    passed,
    failed,
    ROUND(100.0 * passed / total_files, 1) as pass_rate
FROM test_runs
ORDER BY started_at DESC
LIMIT 10
"""
    },

    'by-category': {
        'description': 'Show pass rate grouped by category',
        'query': """
SELECT
    category,
    COUNT(*) as total,
    SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) as passed,
    SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) as failed,
    SUM(CASE WHEN status='UNTESTED' THEN 1 ELSE 0 END) as untested,
    ROUND(100.0 * SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) / COUNT(*), 1) as pass_pct
FROM test_files
GROUP BY category
ORDER BY pass_pct DESC
"""
    },

    'flaky-tests': {
        'description': 'Find tests that sometimes pass, sometimes fail',
        'query': """
SELECT
    file_path,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) as passes,
    SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) as fails
FROM test_results
GROUP BY file_path
HAVING passes > 0 AND fails > 0
ORDER BY total_runs DESC
LIMIT 20
"""
    },

    'recent-runs': {
        'description': 'Show recent test run summary',
        'query': """
SELECT
    run_id,
    datetime(started_at) as started,
    total_files,
    passed,
    failed,
    git_commit
FROM test_runs
ORDER BY started_at DESC
LIMIT 5
"""
    },

    'untested': {
        'description': 'Show files that have not been tested yet',
        'query': """
SELECT
    file_path,
    category,
    subcategory
FROM test_files
WHERE status = 'UNTESTED'
ORDER BY category, subcategory, file_path
LIMIT 50
"""
    },

    'slow-tests': {
        'description': 'Show slowest running tests',
        'query': """
SELECT
    file_path,
    AVG(duration_ms) as avg_duration_ms,
    MAX(duration_ms) as max_duration_ms,
    COUNT(*) as run_count
FROM test_results
WHERE duration_ms IS NOT NULL
GROUP BY file_path
HAVING AVG(duration_ms) > 0
ORDER BY avg_duration_ms DESC
LIMIT 20
"""
    },

    'failing-by-category': {
        'description': 'Show failing tests grouped by category',
        'query': """
SELECT
    category,
    subcategory,
    COUNT(*) as failed_count
FROM test_files
WHERE status = 'FAIL'
GROUP BY category, subcategory
ORDER BY failed_count DESC
"""
    },

    'latest-failures': {
        'description': 'Show latest test failures with error messages',
        'query': """
SELECT
    tr.file_path,
    tf.category,
    tr.error_message,
    datetime(tr.tested_at) as tested_at
FROM test_results tr
JOIN test_files tf ON tr.file_path = tf.file_path
WHERE tr.status = 'FAIL'
ORDER BY tr.tested_at DESC
LIMIT 20
"""
    },
}


def execute_query_with_cli(query: str, db_path: Path) -> Tuple[bool, str]:
    """
    Execute SQL query using VibeSQL CLI (if available).

    For now, this displays the query and instructions for manual execution.
    Once Python bindings support execution, this will run queries directly.

    Args:
        query: SQL query to execute
        db_path: Path to SQL dump file

    Returns:
        (success: bool, output: str)
    """
    if not db_path.exists():
        return False, f"Database not found: {db_path}"

    # For now, output the query for manual execution
    output = f"""
Database: {db_path}

Query to execute:
{query}

To run this query:
1. Load the database SQL dump into a SQL client or web demo
2. Execute the query above
3. Or use Python bindings once they support execution

Example (future):
    import vibesql
    db = vibesql.load_sql_dump('{db_path}')
    results = db.execute(query)
"""

    return True, output


def list_presets():
    """Print available preset queries."""
    print("\nAvailable preset queries:\n")
    print(f"{'Preset':<20} Description")
    print("-" * 80)
    for name, info in PRESET_QUERIES.items():
        print(f"{name:<20} {info['description']}")
    print()


def format_output(output: str):
    """Format query output for better readability."""
    # Just print the output as-is for now
    # TODO: Could add table formatting here
    print(output)


def main():
    parser = argparse.ArgumentParser(
        description="Query SQLLogicTest results database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--preset",
        choices=PRESET_QUERIES.keys(),
        help="Run a preset query",
    )
    parser.add_argument(
        "--query",
        type=str,
        help="Custom SQL query to execute",
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=None,
        help="Database SQL dump file (default: target/sqllogictest_results.sql)",
    )
    parser.add_argument(
        "--list-presets",
        action="store_true",
        help="List available preset queries",
    )

    args = parser.parse_args()

    # Handle --list-presets
    if args.list_presets:
        list_presets()
        return 0

    # Require either --preset or --query
    if not args.preset and not args.query:
        parser.print_help()
        print("\nError: Must specify either --preset or --query", file=sys.stderr)
        print("\nUse --list-presets to see available presets", file=sys.stderr)
        return 1

    # Determine database path
    repo_root = get_repo_root()
    db_path = args.database or (repo_root / "target" / "sqllogictest_results.sql")

    # Check database exists
    if not db_path.exists():
        print(f"Error: Database not found: {db_path}", file=sys.stderr)
        print("\nRun tests first to generate database:", file=sys.stderr)
        print("  ./scripts/sqllogictest run", file=sys.stderr)
        return 1

    # Get query
    if args.preset:
        query_info = PRESET_QUERIES[args.preset]
        query = query_info['query']
        print(f"Running preset: {args.preset}")
        print(f"Description: {query_info['description']}\n")
    else:
        query = args.query

    # Execute query
    success, output = execute_query_with_cli(query, db_path)

    if not success:
        print(f"Error executing query:", file=sys.stderr)
        print(output, file=sys.stderr)
        return 1

    # Format and print output
    format_output(output)

    return 0


if __name__ == "__main__":
    sys.exit(main())
