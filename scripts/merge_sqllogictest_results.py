#!/usr/bin/env python3
"""
Merge SQLLogicTest results from multiple CI runs.

This script combines test results from the current run with historical results
to progressively build complete test coverage over time.

Usage:
    python merge_sqllogictest_results.py current_results.json historical_results.json output.json
"""

import sys
import json
from pathlib import Path
from typing import Dict, Set


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
    # Get tested files from both runs
    current_passed = set(current.get("tested_files", {}).get("passed", []))
    current_failed = set(current.get("tested_files", {}).get("failed", []))

    historical_passed = set(historical.get("tested_files", {}).get("passed", []))
    historical_failed = set(historical.get("tested_files", {}).get("failed", []))

    # Merge: Current results override historical for files tested in current run
    # Files only in historical retain their status
    merged_passed = (historical_passed - current_failed) | current_passed
    merged_failed = (historical_failed - current_passed) | current_failed

    # Calculate totals
    total_available = current.get("summary", {}).get("total_available_files",
                                                      historical.get("summary", {}).get("total_available_files", 0))
    total_tested = len(merged_passed) + len(merged_failed)
    untested = total_available - total_tested
    pass_rate = (len(merged_passed) / total_tested * 100) if total_tested > 0 else 0
    coverage_rate = (total_tested / total_available * 100) if total_available > 0 else 0

    return {
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

    # Write output
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
    print(f"✓ Merged results written to {output_file}")


if __name__ == "__main__":
    main()
