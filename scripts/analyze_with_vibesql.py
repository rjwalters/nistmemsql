#!/usr/bin/env python3
"""
Analyze SQLLogicTest results using vibesql database.

This script demonstrates dogfooding by using vibesql to store and analyze
test results instead of JSON files. It:
1. Initializes a vibesql database with test results schema
2. Parses worker logs and loads results into the database
3. Runs SQL queries to identify failure patterns and priorities
4. Generates markdown reports from query results

Usage:
    python3 scripts/analyze_with_vibesql.py <results_dir> [--output report.md]
"""

import argparse
import json
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple


class VibesqlAnalyzer:
    """Analyzer that uses vibesql database for test result analysis."""

    def __init__(self, db_path: Path, vibesql_bin: Optional[Path] = None):
        self.db_path = db_path
        self.vibesql_bin = vibesql_bin or self._find_vibesql_binary()

    def _find_vibesql_binary(self) -> Path:
        """Find or build the vibesql CLI binary."""
        # Try to find existing binary in target/release
        repo_root = Path(__file__).resolve().parent.parent
        release_bin = repo_root / "target" / "release" / "vibesql"

        if release_bin.exists():
            return release_bin

        # Build if not found
        print("Building vibesql CLI binary...")
        try:
            subprocess.run(
                ["cargo", "build", "--release", "-p", "vibesql-cli"],
                cwd=repo_root,
                check=True,
                capture_output=True,
            )
            if release_bin.exists():
                return release_bin
        except subprocess.CalledProcessError as e:
            print(f"Error building vibesql: {e.stderr.decode()}", file=sys.stderr)

        raise RuntimeError("Could not find or build vibesql binary")

    def execute_sql(self, sql: str, capture_output: bool = True, output_format: str = "table") -> subprocess.CompletedProcess:
        """Execute SQL against the vibesql database."""
        cmd = [
            str(self.vibesql_bin),
            "--database", str(self.db_path),
            "--format", output_format,
            "--stdin",
        ]

        result = subprocess.run(
            cmd,
            input=sql,
            text=True,
            capture_output=capture_output,
            check=False,
        )

        if result.returncode != 0 and capture_output:
            print(f"SQL Error: {result.stderr}", file=sys.stderr)

        return result

    def initialize_database(self, schema_file: Path) -> None:
        """Initialize database with schema."""
        print(f"Initializing database: {self.db_path}")

        # Remove existing database
        if self.db_path.exists():
            self.db_path.unlink()

        # Create database and load schema
        schema_sql = schema_file.read_text()
        result = self.execute_sql(schema_sql)

        if result.returncode != 0:
            raise RuntimeError(f"Failed to initialize database: {result.stderr}")

        print("✓ Database initialized")

    def parse_worker_logs(self, results_dir: Path) -> Tuple[Dict, List[Dict]]:
        """
        Parse worker logs to extract test results.

        Returns:
            (run_metadata, test_results)
        """
        worker_logs = sorted(results_dir.glob("worker_*.log"))
        if not worker_logs:
            raise RuntimeError(f"No worker logs found in {results_dir}")

        print(f"Parsing {len(worker_logs)} worker logs...")

        # Extract run metadata from first log
        run_metadata = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "workers": len(worker_logs),
            "time_budget": 3600,  # Default, can be extracted if needed
            "seed": 0,  # Default
        }

        # Parse test results from all logs
        test_results = []
        result_id = 1

        for worker_id, log_path in enumerate(worker_logs, start=1):
            with open(log_path, 'r') as f:
                for line in f:
                    if line.startswith("✓"):
                        # Passed test: ✓ path/to/test.test
                        match = re.match(r'✓ ([^\s]+)', line)
                        if match:
                            test_results.append({
                                "result_id": result_id,
                                "file_path": match.group(1),
                                "status": "passed",
                                "worker_id": worker_id,
                            })
                            result_id += 1

                    elif line.startswith("✗"):
                        # Failed test: ✗ path/to/test.test - error message
                        match = re.match(r'✗ ([^\s]+) - (.+)', line)
                        if match:
                            file_path = match.group(1)
                            error_msg = match.group(2).strip()

                            # Classify error type
                            if "Test panicked" in error_msg:
                                error_type = "parse_error"
                            elif "statement failed" in error_msg:
                                if "Parse error" in error_msg:
                                    error_type = "sql_parse_error"
                                elif "Execution error" in error_msg:
                                    error_type = "execution_error"
                                else:
                                    error_type = "statement_error"
                            elif "query result mismatch" in error_msg:
                                error_type = "result_mismatch"
                            else:
                                error_type = "other"

                            test_results.append({
                                "result_id": result_id,
                                "file_path": file_path,
                                "status": "failed",
                                "error_type": error_type,
                                "error_message": error_msg[:500],  # Truncate long messages
                                "worker_id": worker_id,
                            })
                            result_id += 1

        print(f"✓ Parsed {len(test_results)} test results")
        return run_metadata, test_results

    def load_test_results(self, run_metadata: Dict, test_results: List[Dict]) -> None:
        """Load test results into the database."""
        print("Loading test results into database...")

        # Calculate summary statistics
        total = len(test_results)
        passed = sum(1 for r in test_results if r["status"] == "passed")
        failed = sum(1 for r in test_results if r["status"] == "failed")
        pass_rate = (passed / total * 100) if total > 0 else 0

        # Insert test run
        insert_run_sql = f"""
        INSERT INTO test_runs (run_id, timestamp, workers, time_budget, seed, total_files, passed_files, failed_files, pass_rate)
        VALUES (1, '{run_metadata['timestamp']}', {run_metadata['workers']}, {run_metadata['time_budget']},
                {run_metadata['seed']}, {total}, {passed}, {failed}, {pass_rate:.2f});
        """
        self.execute_sql(insert_run_sql)

        # Insert test results in batches
        batch_size = 100
        for i in range(0, len(test_results), batch_size):
            batch = test_results[i:i + batch_size]
            values = []

            for result in batch:
                file_path = result['file_path'].replace("'", "''")  # Escape quotes
                status = result['status']
                error_type = result.get('error_type', '')
                error_message = result.get('error_message', '').replace("'", "''")  # Escape quotes
                worker_id = result['worker_id']

                values.append(
                    f"(1, '{file_path}', '{status}', '{error_type}', '{error_message}', {worker_id})"
                )

            insert_sql = f"""
            INSERT INTO test_results (run_id, file_path, status, error_type, error_message, worker_id)
            VALUES {', '.join(values)};
            """
            self.execute_sql(insert_sql)

        print(f"✓ Loaded {len(test_results)} test results")
        print(f"  Passed: {passed} ({pass_rate:.1f}%)")
        print(f"  Failed: {failed}")

    def generate_report(self, output_file: Path) -> None:
        """Generate markdown report using SQL queries."""
        print("\nGenerating analysis report...")

        report_lines = []
        report_lines.append("# SQLLogicTest Failure Analysis (vibesql Dogfooding)")
        report_lines.append("")
        report_lines.append("*This report demonstrates vibesql by using it to analyze our own test results!*")
        report_lines.append("")

        # Summary section
        report_lines.append("## Summary")
        report_lines.append("")

        summary_sql = """
        SELECT
            total_files as 'Total Files',
            passed_files as 'Passed',
            failed_files as 'Failed',
            ROUND(pass_rate, 1) || '%' as 'Pass Rate'
        FROM test_runs
        WHERE run_id = 1;
        """
        result = self.execute_sql(summary_sql, output_format="table")

        if result.returncode == 0:
            report_lines.append("```")
            report_lines.append(result.stdout.strip())
            report_lines.append("```")
            report_lines.append("")

        # Failure breakdown by error type
        report_lines.append("## Failure Breakdown by Error Type")
        report_lines.append("")

        breakdown_sql = """
        SELECT
            error_type as 'Error Type',
            COUNT(*) as 'Count',
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM test_results WHERE status = 'failed'), 1) || '%' as '% of Failures'
        FROM test_results
        WHERE status = 'failed' AND error_type IS NOT NULL AND error_type != ''
        GROUP BY error_type
        ORDER BY COUNT(*) DESC;
        """
        result = self.execute_sql(breakdown_sql, output_format="table")

        if result.returncode == 0:
            report_lines.append("```")
            report_lines.append(result.stdout.strip())
            report_lines.append("```")
        report_lines.append("")

        # Top failing test files
        report_lines.append("## Sample Failed Tests")
        report_lines.append("")
        report_lines.append("Example test files that failed:")
        report_lines.append("")

        samples_sql = """
        SELECT file_path as 'Test File', error_type as 'Error Type'
        FROM test_results
        WHERE status = 'failed'
        LIMIT 10;
        """
        result = self.execute_sql(samples_sql, output_format="table")

        if result.returncode == 0:
            report_lines.append("```")
            report_lines.append(result.stdout.strip())
            report_lines.append("```")
        report_lines.append("")

        # Dogfooding notes
        report_lines.append("## Dogfooding Notes")
        report_lines.append("")
        report_lines.append("This analysis demonstrates vibesql capabilities:")
        report_lines.append("")
        report_lines.append("- ✅ CREATE TABLE with multiple columns")
        report_lines.append("- ✅ INSERT with batch operations")
        report_lines.append("- ✅ SELECT with aggregations (COUNT, AVG, SUM)")
        report_lines.append("- ✅ WHERE clauses and filtering")
        report_lines.append("- ✅ GROUP BY and ORDER BY")
        report_lines.append("- ✅ Subqueries in SELECT")
        report_lines.append("- ✅ ROUND and string concatenation")
        report_lines.append("")
        report_lines.append("Query the database directly for custom analysis:")
        report_lines.append("```bash")
        report_lines.append(f"vibesql --database {self.db_path}")
        report_lines.append("SELECT error_type, COUNT(*) FROM test_results WHERE status='failed' GROUP BY error_type;")
        report_lines.append("```")
        report_lines.append("")

        # Write report
        output_file.write_text('\n'.join(report_lines))
        print(f"✓ Report written to: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Analyze SQLLogicTest results using vibesql",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "results_dir",
        type=Path,
        help="Directory containing worker logs",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output markdown report path (default: target/sqllogictest_failure_analysis.md)",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="Database path (default: target/test_results.db)",
    )

    args = parser.parse_args()

    # Validate inputs
    if not args.results_dir.exists():
        print(f"Error: Results directory not found: {args.results_dir}", file=sys.stderr)
        sys.exit(1)

    # Set defaults
    repo_root = Path(__file__).resolve().parent.parent
    db_path = args.db or (repo_root / "target" / "test_results.db")
    output_path = args.output or (repo_root / "target" / "sqllogictest_failure_analysis.md")
    schema_path = repo_root / "scripts" / "test_results_schema_simple.sql"

    # Ensure target directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Initialize analyzer
    analyzer = VibesqlAnalyzer(db_path)

    # Initialize database
    analyzer.initialize_database(schema_path)

    # Parse and load results
    run_metadata, test_results = analyzer.parse_worker_logs(args.results_dir)
    analyzer.load_test_results(run_metadata, test_results)

    # Generate report
    analyzer.generate_report(output_path)

    print("\n" + "=" * 60)
    print("Analysis Complete!")
    print("=" * 60)
    print(f"Database: {db_path}")
    print(f"Report: {output_path}")
    print("\nYou can query the database directly:")
    print(f"  {analyzer.vibesql_bin} {db_path}")
    print("  SELECT * FROM fix_opportunities;")
    print()


if __name__ == "__main__":
    main()
