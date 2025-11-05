#!/usr/bin/env python3
"""
Merge SQLLogicTest results from multiple CI runs.

This script combines test results from the current run with historical results
to progressively build complete test coverage.
"""

import sys
import json
from pathlib import Path
from typing import Dict, Set
from datetime import datetime


def load_json(filepath: str) -> Dict:
    """Load JSON from file, return empty dict if file doesn't exist or is invalid."""
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def merge_results(current: Dict, historical: Dict) -> Dict:
    """
    Merge current test results with historical results.

    Rules:
    - A file that passed in any run is marked as passed (unless it later failed)
    - A file that failed in the most recent test takes precedence
    - Untested files are calculated from total available - tested
    """
    # Handle both old and new JSON formats
    if "summary" in current:
        # New format
        current_summary = current.get("summary", {})
        current_passed = set(current.get("tested_files", {}).get("passed", []))
        current_failed = set(current.get("tested_files", {}).get("failed", []))
    else:
        # Old format - convert
        current_summary = {
            "total_available_files": current.get("total_available_files", 0),
            "total_tested_files": len(current.get("tested_files", [])),
            "passed": current.get("passed", 0),
            "failed": current.get("failed", 0),
            "errors": current.get("errors", 0),
            "skipped": current.get("skipped", 0),
            "pass_rate": current.get("pass_rate", 0),
        }
        # For old format, we don't have separate passed/failed lists, so we need to infer
        current_passed = set()
        current_failed = set()

    if "summary" in historical:
        # New format
        historical_summary = historical.get("summary", {})
        historical_passed = set(historical.get("tested_files", {}).get("passed", []))
        historical_failed = set(historical.get("tested_files", {}).get("failed", []))
    else:
        # Old format
        historical_summary = {
            "total_available_files": historical.get("total_available_files", 0),
            "total_tested_files": len(historical.get("tested_files", [])),
            "passed": historical.get("passed", 0),
            "failed": historical.get("failed", 0),
            "errors": historical.get("errors", 0),
            "skipped": historical.get("skipped", 0),
            "pass_rate": historical.get("pass_rate", 0),
        }
        historical_passed = set()
        historical_failed = set()

    # Merge: Current results override historical for files tested in current run
    # Files only in historical retain their status
    merged_passed = (historical_passed - current_failed) | current_passed
    merged_failed = (historical_failed - current_passed) | current_failed

    # Calculate totals
    total_available = current_summary.get("total_available_files",
                                          historical_summary.get("total_available_files", 0))
    total_tested = len(merged_passed) + len(merged_failed)
    untested = total_available - total_tested
    pass_rate = (len(merged_passed) / total_tested * 100) if total_tested > 0 else 0
    coverage_rate = (total_tested / total_available * 100) if total_available > 0 else 0

    # Merge detailed failures from current run
    current_detailed_failures = current.get("detailed_failures", [])
    historical_detailed_failures = historical.get("detailed_failures", [])

    # For simplicity, keep only current run's detailed failures
    # In a full implementation, we'd merge and deduplicate
    merged_detailed_failures = current_detailed_failures

    # Add timestamp
    timestamp = datetime.now().isoformat()

    return {
        "timestamp": timestamp,
        "summary": {
            "total_available_files": total_available,
            "total_tested_files": total_tested,
            "passed": len(merged_passed),
            "failed": len(merged_failed),
            "untested": untested,
            "pass_rate": round(pass_rate, 2),
            "coverage_rate": round(coverage_rate, 2)
        },
        "tested_files": {
            "passed": sorted(list(merged_passed)),
            "failed": sorted(list(merged_failed))
        },
        "detailed_failures": merged_detailed_failures,
        "categories": {
            "select": {"total": 0, "passed": 0, "failed": 0},  # Placeholder - would be calculated from actual data
            "evidence": {"total": 0, "passed": 0, "failed": 0},
            "index": {"total": 0, "passed": 0, "failed": 0},
            "random": {"total": 0, "passed": 0, "failed": 0},
            "ddl": {"total": 0, "passed": 0, "failed": 0},
            "other": {"total": 0, "passed": 0, "failed": 0}
        },
        "merge_info": {
            "current_run_tested": len(current_passed) + len(current_failed),
            "historical_tested": len(historical_passed) + len(historical_failed),
            "new_files_tested": len((current_passed | current_failed) - (historical_passed | historical_failed)),
            "status_changes": len((current_passed & historical_failed) | (current_failed & historical_passed))
        }
    }


def main():
    """Main entry point."""
    if len(sys.argv) < 4:
        print("Usage: python merge_sqllogictest_results.py <current.json> <historical.json> <output.json>")
        print()
        print("Merges SQLLogicTest results from multiple CI runs to build cumulative coverage.")
        sys.exit(1)

    current_file = sys.argv[1]
    historical_file = sys.argv[2]
    output_file = sys.argv[3]

    # Load input files
    print(f"Loading current results from {current_file}...")
    current = load_json(current_file)

    print(f"Loading historical results from {historical_file}...")
    historical = load_json(historical_file)

    # Merge results
    print("Merging results...")
    merged = merge_results(current, historical)

    # Archive historical results
    history_dir = Path("badges/history")
    history_dir.mkdir(parents=True, exist_ok=True)

    timestamp = merged["timestamp"]
    # Create a filename-safe timestamp (replace colons and dots)
    safe_timestamp = timestamp.replace(":", "-").replace(".", "-")
    history_file = history_dir / f"sqllogictest_results_{safe_timestamp}.json"

    print(f"Archiving results to {history_file}...")
    with open(history_file, 'w') as f:
        json.dump(merged, f, indent=2)

    # Write current cumulative results
    print(f"Writing merged results to {output_file}...")
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, 'w') as f:
        json.dump(merged, f, indent=2)

    # Print summary
    print("\n=== Merge Summary ===")
    print(f"Total available files: {merged['summary']['total_available_files']}")
    print(f"Total tested files: {merged['summary']['total_tested_files']} ({merged['summary']['coverage_rate']:.1f}% coverage)")
    print(f"  Passed: {merged['summary']['passed']}")
    print(f"  Failed: {merged['summary']['failed']}")
    print(f"  Untested: {merged['summary']['untested']}")
    print(f"Pass rate: {merged['summary']['pass_rate']:.1f}%")
    print()
    print(f"New files tested this run: {merged['merge_info']['new_files_tested']}")
    print(f"Status changes: {merged['merge_info']['status_changes']}")
    print()
    print(f"✓ Results archived to {history_file}")
    print(f"✓ Merged results written to {output_file}")


if __name__ == "__main__":
    main()
