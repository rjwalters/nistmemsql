#!/usr/bin/env python3
"""
Process SQLLogicTest results into VibeSQL database.

This script is the core of our dogfooding initiative - it takes JSON test results
and stores them in a VibeSQL database, demonstrating real-world usage.

Usage:
    ./scripts/process_test_results.py --input results.json [--database db.sql]

Features:
- Creates database from schema on first run
- Loads existing database state from SQL dump
- Inserts new test run metadata
- Records individual test results with error messages
- Updates current status in test_files table
- Exports SQL dump for version control and web demo

Example:
    # After a test run
    ./scripts/process_test_results.py \
        --input target/sqllogictest_results.json

    # Results stored in: ~/.vibesql/test_results/sqllogictest_results.sql
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Import shared configuration for test results storage
from test_results_config import (
    get_default_database_path,
    get_default_json_path,
    migrate_legacy_results,
)


def get_repo_root() -> Path:
    """Find the repository root directory."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / ".git").exists():
            return current
        current = current.parent
    raise RuntimeError("Could not find git repository root")


def get_git_commit() -> Optional[str]:
    """Get current git commit hash."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return None


def execute_sql_file(sql_path: Path) -> Tuple[bool, str]:
    """
    Execute SQL file using VibeSQL Python bindings.

    Note: For now, we just verify the SQL is valid by checking it can be read.
    Full execution will happen when the SQL dump is loaded into the demo or CLI.

    Returns:
        (success: bool, output: str)
    """
    try:
        with open(sql_path, 'r') as f:
            sql_content = f.read()

        # Basic validation - check for SQL keywords
        if 'CREATE TABLE' in sql_content or 'INSERT INTO' in sql_content:
            return True, "SQL file created successfully"
        else:
            return False, "SQL file appears empty or invalid"

    except Exception as e:
        return False, str(e)


def create_database_from_schema(schema_path: Path, db_path: Path) -> bool:
    """
    Create a new database from schema SQL file.

    Args:
        schema_path: Path to schema SQL file
        db_path: Path where SQL dump will be saved

    Returns:
        True if successful, False otherwise
    """
    print(f"Creating new database from schema: {schema_path}")

    if not schema_path.exists():
        print(f"Error: Schema file not found: {schema_path}", file=sys.stderr)
        return False

    # For now, we'll just copy the schema as the initial database
    # The schema creates empty tables
    import shutil
    shutil.copy(schema_path, db_path)

    print(f"✓ Database created: {db_path}")
    return True


def load_json_results(json_path: Path) -> Dict:
    """Load test results from JSON file."""
    if not json_path.exists():
        raise FileNotFoundError(f"Results file not found: {json_path}")

    with open(json_path, 'r') as f:
        return json.load(f)


def categorize_test_file(file_path: str) -> Tuple[str, str]:
    """
    Determine category and subcategory from file path.

    Returns:
        (category, subcategory)
    """
    parts = Path(file_path).parts

    if len(parts) == 0:
        return "other", "root"

    category = parts[0] if parts[0] in ["index", "random", "evidence", "select", "ddl"] else "other"
    subcategory = parts[1] if len(parts) >= 2 else "root"

    return category, subcategory


def generate_insert_statements(results_json: Dict) -> List[str]:
    """
    Generate SQL INSERT statements from JSON results.

    Returns:
        List of SQL statements to execute
    """
    statements = []

    # Get metadata
    git_commit = get_git_commit()
    # Format timestamp for VibeSQL: 'YYYY-MM-DD HH:MM:SS.ffffff'
    # Not ISO format (which uses 'T' separator)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

    # Extract summary data
    summary = results_json.get('summary', {})
    total = summary.get('total', 0)
    passed = summary.get('passed', 0)
    failed = summary.get('failed', 0)
    untested = summary.get('untested', 0)

    # 1. Insert test run
    # Note: We use a timestamp-based ID since we don't have auto-increment yet
    run_id = int(datetime.now().timestamp())

    statements.append(f"""
INSERT INTO test_runs (run_id, started_at, completed_at, total_files, passed, failed, untested, git_commit)
VALUES ({run_id}, TIMESTAMP '{timestamp}', TIMESTAMP '{timestamp}', {total}, {passed}, {failed}, {untested}, {sql_escape(git_commit)});
""")

    # 2. Process tested files and insert results
    tested_files = results_json.get('tested_files', {})

    # Process passed files
    for file_path in tested_files.get('passed', []):
        category, subcategory = categorize_test_file(file_path)

        # Upsert into test_files using UPDATE + INSERT pattern (avoids FK constraint violations)
        # Try UPDATE first (if row exists)
        statements.append(f"""
UPDATE test_files
SET category = {sql_escape(category)},
    subcategory = {sql_escape(subcategory)},
    status = 'PASS',
    last_tested = TIMESTAMP '{timestamp}',
    last_passed = TIMESTAMP '{timestamp}'
WHERE file_path = {sql_escape(file_path)};
""")
        # Then INSERT (only succeeds if row doesn't exist - will fail with PK violation if it does, but UPDATE already handled it)
        statements.append(f"""
INSERT INTO test_files (file_path, category, subcategory, status, last_tested, last_passed)
SELECT {sql_escape(file_path)}, {sql_escape(category)}, {sql_escape(subcategory)}, 'PASS', TIMESTAMP '{timestamp}', TIMESTAMP '{timestamp}'
WHERE NOT EXISTS (SELECT 1 FROM test_files WHERE file_path = {sql_escape(file_path)});
""")

        # Insert into test_results (references test_files.file_path FK)
        statements.append(f"""
INSERT INTO test_results (result_id, run_id, file_path, status, tested_at, duration_ms, error_message)
VALUES ({abs(hash(f'{run_id}_{file_path}'))}, {run_id}, {sql_escape(file_path)}, 'PASS', TIMESTAMP '{timestamp}', NULL, NULL);
""")

    # Process failed files
    # Build lookup table of detailed error messages (may be truncated list)
    detailed_failures = results_json.get('detailed_failures', [])
    error_lookup = {}
    for failure_info in detailed_failures:
        fp = failure_info.get('file_path', '')
        failures = failure_info.get('failures', [])
        if failures:
            error_lookup[fp] = failures[0].get('error_message', 'Unknown error')[:2000]

    # Process ALL failed files (not just those with detailed errors)
    failed_files = tested_files.get('failed', [])
    for file_path in failed_files:
        category, subcategory = categorize_test_file(file_path)

        # Get error message from lookup, or NULL if not available
        error_message = error_lookup.get(file_path, None)

        # Upsert into test_files using UPDATE + INSERT pattern (avoids FK constraint violations)
        # Try UPDATE first (if row exists)
        statements.append(f"""
UPDATE test_files
SET category = {sql_escape(category)},
    subcategory = {sql_escape(subcategory)},
    status = 'FAIL',
    last_tested = TIMESTAMP '{timestamp}',
    last_passed = NULL
WHERE file_path = {sql_escape(file_path)};
""")
        # Then INSERT (only succeeds if row doesn't exist)
        statements.append(f"""
INSERT INTO test_files (file_path, category, subcategory, status, last_tested, last_passed)
SELECT {sql_escape(file_path)}, {sql_escape(category)}, {sql_escape(subcategory)}, 'FAIL', TIMESTAMP '{timestamp}', NULL
WHERE NOT EXISTS (SELECT 1 FROM test_files WHERE file_path = {sql_escape(file_path)});
""")

        # Insert into test_results (references test_files.file_path FK)
        statements.append(f"""
INSERT INTO test_results (result_id, run_id, file_path, status, tested_at, duration_ms, error_message)
VALUES ({abs(hash(f'{run_id}_{file_path}'))}, {run_id}, {sql_escape(file_path)}, 'FAIL', TIMESTAMP '{timestamp}', NULL, {sql_escape(error_message)});
""")

    return statements


def sql_escape(value: Optional[str]) -> str:
    """Escape a value for SQL, handling NULL and special characters."""
    if value is None:
        return "NULL"

    # Convert to string in case we receive non-string types
    value = str(value)

    # Escape backslashes first (must be before single quote escaping)
    escaped = value.replace('\\', '\\\\')

    # Escape single quotes by doubling them (SQL standard)
    escaped = escaped.replace("'", "''")

    # Replace control characters that could break SQL parsing
    escaped = escaped.replace('\n', ' ')
    escaped = escaped.replace('\r', ' ')
    escaped = escaped.replace('\t', ' ')
    escaped = escaped.replace('\x00', '')  # Remove null bytes

    # Collapse multiple spaces into single space
    escaped = ' '.join(escaped.split())

    return f"'{escaped}'"


def append_statements_to_dump(db_path: Path, statements: List[str]) -> bool:
    """
    Append SQL statements to the database dump file.

    Args:
        db_path: Path to SQL dump file
        statements: SQL statements to append

    Returns:
        True if successful, False otherwise
    """
    try:
        with open(db_path, 'a') as f:
            f.write("\n-- Test results inserted at " + datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') + "\n")
            for stmt in statements:
                f.write(stmt)
                f.write("\n")
        return True
    except Exception as e:
        print(f"Error appending to database: {e}", file=sys.stderr)
        return False


def print_summary(results_json: Dict):
    """Print a summary of what was processed."""
    summary = results_json.get('summary', {})

    print("\n" + "="*60)
    print("Database Update Summary")
    print("="*60)
    print(f"Total files:  {summary.get('total', 0)}")
    print(f"Passed:       {summary.get('passed', 0)}")
    print(f"Failed:       {summary.get('failed', 0)}")
    print(f"Untested:     {summary.get('untested', 0)}")
    print(f"Git commit:   {get_git_commit() or 'unknown'}")
    print("="*60)
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Process SQLLogicTest results into VibeSQL database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Input JSON results file",
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=None,
        help="Database SQL dump file (default: ~/.vibesql/test_results/sqllogictest_results.sql)",
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=None,
        help="Schema SQL file (default: scripts/schema/test_results.sql)",
    )

    args = parser.parse_args()

    # Set defaults
    repo_root = get_repo_root()
    db_path = args.database or get_default_database_path()
    schema_path = args.schema or (repo_root / "scripts" / "schema" / "test_results.sql")

    # Attempt to migrate legacy results from target/ directory
    migrate_legacy_results(repo_root, verbose=True)

    # Ensure target directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Load JSON results
    print(f"Loading results from: {args.input}")
    try:
        results_json = load_json_results(args.input)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in {args.input}: {e}", file=sys.stderr)
        return 1

    # Create or load database
    if not db_path.exists():
        print(f"Database not found, creating from schema...")
        if not create_database_from_schema(schema_path, db_path):
            return 1
    else:
        print(f"Using existing database: {db_path}")

    # Generate SQL statements
    print("Generating SQL statements...")
    statements = generate_insert_statements(results_json)
    print(f"Generated {len(statements)} SQL statements")

    # Append to database
    print(f"Appending to database: {db_path}")
    if not append_statements_to_dump(db_path, statements):
        return 1

    # Print summary
    print_summary(results_json)

    print(f"✓ Database updated: {db_path}")
    print(f"\nQuery your results:")
    print(f"  ./scripts/query_test_results.py --preset failed-files")
    print(f"  ./scripts/query_test_results.py --preset progress")

    return 0


if __name__ == "__main__":
    sys.exit(main())
