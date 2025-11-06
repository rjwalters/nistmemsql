#!/usr/bin/env python3
"""
Aggregate SQLLogicTest results from multiple parallel workers.

This script processes individual worker log files and aggregates
the results into a single cumulative summary, deduplicating test
results across workers.
"""

import json
import re
import sys
from pathlib import Path
from collections import defaultdict
from typing import Set, Dict


def extract_results_from_log(log_path: Path) -> tuple[Set[str], Set[str], int]:
    """
    Extract passed, failed test files and total available files from a worker log.

    Returns:
        (passed_files, failed_files, total_available)
    """
    passed = set()
    failed = set()
    total_available = 0

    with open(log_path, 'r') as f:
        for line in f:
            line = line.strip()

            # Match passed tests
            if line.startswith("✓ "):
                test_file = line[2:].strip()
                # Skip meta messages
                if not test_file.startswith("Results written") and not test_file.startswith("Test binary"):
                    passed.add(test_file)

            # Match failed tests
            elif line.startswith("✗ "):
                # Extract just the filename (before " - ")
                match = re.match(r'✗ ([^ ]+(?:\.test)?)', line)
                if match:
                    test_file = match.group(1)
                    failed.add(test_file)

            # Extract total available files
            elif "Total available test files:" in line:
                match = re.search(r'Total available test files: (\d+)', line)
                if match:
                    total_available = int(match.group(1))

    return passed, failed, total_available


def aggregate_results(results_dir: Path) -> Dict:
    """
    Aggregate results from all worker log files in a directory.

    Returns:
        Dictionary with summary and detailed results
    """
    all_passed = set()
    all_failed = set()
    total_available = 0
    workers_processed = 0

    # Find all worker log files
    worker_logs = sorted(results_dir.glob("worker_*.log"))

    if not worker_logs:
        print(f"Error: No worker log files found in {results_dir}", file=sys.stderr)
        return None

    print(f"Found {len(worker_logs)} worker logs")

    for log_path in worker_logs:
        passed, failed, available = extract_results_from_log(log_path)

        # Merge results (sets automatically deduplicate)
        all_passed.update(passed)
        all_failed.update(failed)

        # Use the total_available from any worker (they should all be the same)
        if available > 0:
            total_available = available

        workers_processed += 1

        if workers_processed % 50 == 0:
            print(f"  Processed {workers_processed}/{len(worker_logs)} workers...")

    print(f"✓ Processed {workers_processed} workers")

    # Remove duplicates: if a file appears in both passed and failed, mark as failed
    all_passed = all_passed - all_failed

    total_tested = len(all_passed) + len(all_failed)
    pass_rate = (len(all_passed) / total_tested * 100) if total_tested > 0 else 0
    coverage_rate = (total_tested / total_available * 100) if total_available > 0 else 0
    untested = total_available - total_tested

    return {
        "timestamp": None,  # Will be set by merge script
        "summary": {
            "total_available_files": total_available,
            "total_tested_files": total_tested,
            "passed": len(all_passed),
            "failed": len(all_failed),
            "untested": untested,
            "pass_rate": round(pass_rate, 2),
            "coverage_rate": round(coverage_rate, 2)
        },
        "tested_files": {
            "passed": sorted(list(all_passed)),
            "failed": sorted(list(all_failed))
        },
        "detailed_failures": [],  # Could be populated with error details if needed
        "categories": {
            "select": {"total": 0, "passed": 0, "failed": 0},
            "evidence": {"total": 0, "passed": 0, "failed": 0},
            "index": {"total": 0, "passed": 0, "failed": 0},
            "random": {"total": 0, "passed": 0, "failed": 0},
            "ddl": {"total": 0, "passed": 0, "failed": 0},
            "other": {"total": 0, "passed": 0, "failed": 0}
        },
        "merge_info": {
            "workers_processed": workers_processed,
            "source": str(results_dir)
        }
    }


def main():
    if len(sys.argv) < 2:
        print("Usage: aggregate_worker_results.py <results_directory> [output_file]")
        print("Example: aggregate_worker_results.py /tmp/sqllogictest_results_20251106_070141")
        sys.exit(1)

    results_dir = Path(sys.argv[1])
    output_file = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("target/sqllogictest_cumulative.json")

    if not results_dir.exists():
        print(f"Error: Directory not found: {results_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Aggregating results from: {results_dir}")

    results = aggregate_results(results_dir)

    if results is None:
        sys.exit(1)

    # Add timestamp
    from datetime import datetime
    results["timestamp"] = datetime.now().isoformat()

    # Ensure output directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Write results
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"\n✓ Aggregated results written to: {output_file}")
    print("\n=== Summary ===")
    print(json.dumps(results["summary"], indent=2))


if __name__ == "__main__":
    main()
