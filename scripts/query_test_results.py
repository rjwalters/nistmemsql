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

# Import shared configuration for test results storage
from test_results_config import get_default_database_path


def get_repo_root() -> Path:
    """Find the repository root directory, handling git worktrees."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        git_path = current / ".git"
        if git_path.exists():
            # Check if this is a worktree (where .git is a file, not directory)
            if git_path.is_file():
                # Parse .git file to find main worktree
                git_content = git_path.read_text().strip()
                # Format: "gitdir: /path/to/main/repo/.git/worktrees/name"
                if git_content.startswith("gitdir: "):
                    git_dir = git_content[8:]  # Remove "gitdir: " prefix
                    # Navigate up from .git/worktrees/name to main repo
                    main_git_dir = Path(git_dir).parent.parent.parent
                    if main_git_dir.exists():
                        return main_git_dir
            else:
                # Regular git directory
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

    'analyze-summary': {
        'description': 'Show latest test run summary for analysis',
        'query': """
SELECT * FROM latest_run_summary
"""
    },

    'analyze-patterns': {
        'description': 'Show failure patterns grouped by error type',
        'query': """
SELECT
    error_pattern,
    error_type,
    failure_count,
    affected_files,
    pct_of_failures
FROM failure_patterns
WHERE error_pattern != 'Other error'
ORDER BY failure_count DESC
LIMIT 20
"""
    },

    'analyze-opportunities': {
        'description': 'Show top fix opportunities prioritized by impact',
        'query': """
SELECT
    rank,
    priority,
    pattern,
    tests_affected,
    printf('%.1f%%', pct_failures) as pct_failures,
    effort,
    impact_ratio
FROM fix_opportunities
ORDER BY rank
"""
    },

    'analyze-examples': {
        'description': 'Show example failures for each pattern',
        'query': """
SELECT
    error_pattern,
    file_path,
    error_type,
    error_message_preview
FROM failure_examples
ORDER BY error_pattern, file_path
"""
    },
}


def execute_query_with_cli(query: str, db_path: Path) -> Tuple[bool, str]:
    """
    Execute SQL query using VibeSQL CLI.

    Args:
        query: SQL query to execute
        db_path: Path to SQL dump file

    Returns:
        (success: bool, output: str)
    """
    if not db_path.exists():
        return False, f"Database not found: {db_path}"

    # Find CLI binary
    repo_root = get_repo_root()
    cli_binary = repo_root / "target" / "release" / "vibesql"

    if not cli_binary.exists():
        return False, f"CLI binary not found: {cli_binary}\n\nBuild with: cargo build --release --package cli"

    try:
        # Execute query: cat db.sql - <<'EOF' | vibesql --stdin --format table
        # We need to pipe both the SQL dump and the query
        input_sql = db_path.read_text() + "\n" + query.strip()

        result = subprocess.run(
            [str(cli_binary), "--stdin", "--format", "table"],
            input=input_sql,
            capture_output=True,
            text=True,
            check=False,  # Don't raise on non-zero exit
        )

        # Filter output to remove noise from INSERT/DELETE statements
        # Keep only lines that are part of the actual query result table
        lines = result.stdout.split('\n')
        filtered_lines = []
        in_table = False

        for line in lines:
            # Check if we're starting/continuing a table
            if line.startswith('+') or line.startswith('|'):
                in_table = True
                filtered_lines.append(line)
            elif in_table:
                # We've reached the end of the table (non-table line after table started)
                # Check if it's a row count line (e.g., "0 rows", "5 rows")
                if line.strip().endswith(' rows'):
                    # This is the query result's row count, keep it
                    filtered_lines.append(line)
                # Stop capturing after reaching end of table
                break

        # Check if we got table output
        # Successful queries with results produce tables with borders
        has_table_output = bool(filtered_lines)

        if not has_table_output:
            # No table output - could be empty result or query failed
            # Look for "0 rows" immediately before execution summary
            # (within 3 lines to account for blank lines)
            lines = result.stdout.split('\n')

            # Find the execution summary line
            summary_index = None
            for i, line in enumerate(lines):
                if "=== Script Execution Summary ===" in line:
                    summary_index = i
                    break

            if summary_index is not None:
                # Look backwards from summary for "0 rows"
                # Check up to 3 lines before the summary
                for i in range(max(0, summary_index - 3), summary_index):
                    if lines[i].strip() == "0 rows":
                        # Found "0 rows" immediately before summary - query succeeded with empty result
                        return True, "0 rows"

            # No table output and no "0 rows" immediately before summary - query failed
            if result.stderr and "Error" in result.stderr:
                return False, f"Query execution failed:\n{result.stderr}"

            return False, "Query produced no output"

        return True, '\n'.join(filtered_lines)

    except Exception as e:
        return False, f"Failed to execute query: {e}"


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
        help="Database SQL dump file (default: ~/.vibesql/test_results/sqllogictest_results.sql)",
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
    db_path = args.database or get_default_database_path()

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
