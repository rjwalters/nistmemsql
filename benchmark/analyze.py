#!/usr/bin/env python3
"""
Load SQLLogicTest benchmark results into VibeSQL database for analysis.

Usage:
    ./scripts/load_benchmarks.py <benchmark_json_file> [--notes "Run notes"]

Examples:
    ./scripts/load_benchmarks.py target/benchmarks/comparison_20251111_190627.json
    ./scripts/load_benchmarks.py target/benchmark_full_suite.json --notes "Head-to-head with SQLite3"
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional


def get_repo_root() -> Path:
    """Find the repository root directory."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / ".git").exists():
            return current
        current = current.parent
    raise RuntimeError("Could not find git repository root")


def create_tables(db_path: Path):
    """Create benchmark tables if they don't exist."""
    repo_root = get_repo_root()
    schema_file = repo_root / "benchmark" / "schema.sql"

    if not schema_file.exists():
        print(f"Error: Schema file not found: {schema_file}")
        sys.exit(1)

    print(f"Creating tables using schema: {schema_file}")

    # Use vibesql CLI to execute schema
    result = subprocess.run(
        [str(repo_root / "target" / "release" / "vibesql"), "--database", str(db_path), "-f", str(schema_file)],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(f"Error creating tables: {result.stderr}")
        sys.exit(1)

    print("Tables created successfully")


def load_benchmark_data(db_path: Path, json_file: Path, notes: Optional[str] = None):
    """Load benchmark JSON data into database."""
    repo_root = get_repo_root()
    vibesql_bin = repo_root / "target" / "release" / "vibesql"

    if not vibesql_bin.exists():
        print(f"Error: VibeSQL binary not found: {vibesql_bin}")
        print("Build it with: cargo build --release")
        sys.exit(1)

    # Load JSON
    print(f"Loading benchmark data from: {json_file}")
    with open(json_file) as f:
        data = json.load(f)

    timestamp = data.get('timestamp', '')
    total_files = data.get('total_files', 0)
    results = data.get('results', [])

    print(f"Benchmark run: {timestamp}")
    print(f"Total files: {total_files}")
    print(f"Results: {len(results)}")

    # Generate SQL to insert data
    sql_commands = []

    # Insert benchmark run
    description_sql = f"'{notes}'" if notes else "NULL"
    sql_commands.append(
        f"INSERT INTO benchmark_runs (run_id, run_timestamp, total_files, run_description) "
        f"VALUES (1, '{timestamp}', {total_files}, {description_sql});"
    )

    # Get the run_id (will be the last inserted row)
    sql_commands.append("-- Get run_id for subsequent inserts")

    # Insert results - build multi-row insert for efficiency
    print("Generating INSERT statements...")

    for idx, result in enumerate(results):
        if idx % 100 == 0:
            print(f"  Processing result {idx}/{len(results)}")

        file_path = result.get('file', '')
        category = result.get('category', '')

        vibesql_data = result.get('vibesql', {})
        vs_success = 1 if vibesql_data.get('success', False) else 0
        vs_runs = vibesql_data.get('runs', [None, None, None])
        vs_min = vibesql_data.get('min_secs')
        vs_max = vibesql_data.get('max_secs')
        vs_avg = vibesql_data.get('avg_secs')

        sqlite_data = result.get('sqlite3') or result.get('sqlite', {})
        sq_success = 1 if sqlite_data.get('success', False) else 0
        sq_runs = sqlite_data.get('runs', [None, None, None])
        sq_min = sqlite_data.get('min_secs')
        sq_max = sqlite_data.get('max_secs')
        sq_avg = sqlite_data.get('avg_secs')

        # Calculate speed ratio if both succeeded
        speed_ratio = None
        if vs_success and sq_success and vs_avg is not None and sq_avg is not None and sq_avg > 0:
            speed_ratio = vs_avg / sq_avg

        # Format values for SQL
        def fmt(val):
            return 'NULL' if val is None else str(val)

        # Use a fixed run_id (we'll determine it after the run is inserted)
        sql_commands.append(
            f"INSERT INTO benchmark_results ("
            f"run_id, file_path, category, "
            f"vibesql_success, vibesql_run1_secs, vibesql_run2_secs, vibesql_run3_secs, "
            f"vibesql_min_secs, vibesql_max_secs, vibesql_avg_secs, "
            f"sqlite_success, sqlite_run1_secs, sqlite_run2_secs, sqlite_run3_secs, "
            f"sqlite_min_secs, sqlite_max_secs, sqlite_avg_secs, speed_ratio"
            f") VALUES ("
            f"__RUN_ID__, '{file_path}', '{category}', "
            f"{vs_success}, {fmt(vs_runs[0])}, {fmt(vs_runs[1])}, {fmt(vs_runs[2])}, "
            f"{fmt(vs_min)}, {fmt(vs_max)}, {fmt(vs_avg)}, "
            f"{sq_success if sqlite_data else 'NULL'}, {fmt(sq_runs[0])}, {fmt(sq_runs[1])}, {fmt(sq_runs[2])}, "
            f"{fmt(sq_min)}, {fmt(sq_max)}, {fmt(sq_avg)}, {fmt(speed_ratio)}"
            f");"
        )

    # Write SQL to temp file
    sql_file = repo_root / "target" / "benchmark_load.sql"
    print(f"Writing SQL to: {sql_file}")

    # First, execute just the run insert to get the run_id
    run_insert_sql = sql_commands[0]
    with open(sql_file, 'w') as f:
        f.write(run_insert_sql)

    # Execute to create the run
    result = subprocess.run(
        [str(vibesql_bin), "--database", str(db_path), "-f", str(sql_file)],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(f"Error creating benchmark run: {result.stderr}")
        sys.exit(1)

    # Get the run_id that was just created
    get_runid_sql = "SELECT MAX(run_id) FROM benchmark_runs;"
    result = subprocess.run(
        [str(vibesql_bin), "--database", str(db_path), "-c", get_runid_sql],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(f"Error getting run_id: {result.stderr}")
        sys.exit(1)

    # Parse run_id from output
    run_id = result.stdout.strip().split('\n')[-1].strip()
    print(f"Created benchmark run with ID: {run_id}")

    # Replace __RUN_ID__ placeholder with actual run_id
    sql_with_runid = '\n'.join(sql_commands[2:]).replace('__RUN_ID__', run_id)

    with open(sql_file, 'w') as f:
        f.write(sql_with_runid)

    # Execute SQL
    print(f"Loading data into database: {db_path}")
    result = subprocess.run(
        [str(vibesql_bin), "--database", str(db_path), "-f", str(sql_file)],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(f"Error loading data: {result.stderr}")
        print(f"SQL file saved for debugging: {sql_file}")
        sys.exit(1)

    print("✅ Benchmark data loaded successfully!")

    # Show summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)

    summary_sql = "SELECT * FROM benchmark_overall_stats ORDER BY run_id DESC LIMIT 1;"
    result = subprocess.run(
        [str(vibesql_bin), "--database", str(db_path), "-c", summary_sql],
        capture_output=True,
        text=True
    )

    if result.returncode == 0:
        print(result.stdout)
    else:
        print("(Unable to fetch summary)")


def main():
    parser = argparse.ArgumentParser(description='Load benchmark results into VibeSQL database')
    parser.add_argument('json_file', type=Path, help='Benchmark JSON file to load')
    parser.add_argument('--db', type=Path, default=None, help='Database file (default: benchmarks.db)')
    parser.add_argument('--notes', type=str, help='Optional notes about this benchmark run')
    parser.add_argument('--create-tables', action='store_true', help='Create tables if they don\'t exist')

    args = parser.parse_args()

    if not args.json_file.exists():
        print(f"Error: JSON file not found: {args.json_file}")
        sys.exit(1)

    # Default database location
    if args.db is None:
        repo_root = get_repo_root()
        args.db = repo_root / "benchmarks.db"

    print(f"Database: {args.db}")

    # Create tables if requested or if database doesn't exist
    if args.create_tables or not args.db.exists():
        create_tables(args.db)

    # Load data
    load_benchmark_data(args.db, args.json_file, args.notes)

    print("\n" + "="*60)
    print("NEXT STEPS")
    print("="*60)
    print(f"Query your benchmarks with:")
    print(f"  ./target/release/vibesql {args.db}")
    print()
    print("Useful queries:")
    print("  SELECT * FROM benchmark_summary_by_category;")
    print("  SELECT * FROM top_vibesql_wins;")
    print("  SELECT * FROM top_sqlite_wins;")
    print("  SELECT * FROM benchmark_overall_stats;")


if __name__ == '__main__':
    main()
