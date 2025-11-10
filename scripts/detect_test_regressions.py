#!/usr/bin/env python3
"""
Detect regressions and improvements between two SQLLogicTest runs.

This script compares two test result JSON files and identifies:
- New failures (tests that were passing, now failing)
- Fixed failures (tests that were failing, now passing)
- Still failing tests (persistent failures)
- Pass rate changes

Usage:
    python scripts/detect_test_regressions.py <baseline.json> <current.json>

Examples:
    # Compare against previous run
    python scripts/detect_test_regressions.py \\
        target/sqllogictest_baseline.json \\
        target/sqllogictest_cumulative.json

    # Output JSON for CI processing
    python scripts/detect_test_regressions.py \\
        baseline.json current.json --format json > regressions.json
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Set


def load_test_results(file_path: Path) -> Dict:
    """Load test results from JSON file."""
    if not file_path.exists():
        raise FileNotFoundError(f"Results file not found: {file_path}")

    with open(file_path, 'r') as f:
        return json.load(f)


def detect_regressions(baseline: Dict, current: Dict) -> Dict:
    """
    Compare two test runs to find regressions and improvements.

    Args:
        baseline: Previous test run results
        current: Current test run results

    Returns:
        Dictionary with regression analysis
    """
    # Extract test file lists
    baseline_tested = baseline.get("tested_files", {})
    current_tested = current.get("tested_files", {})

    baseline_passed = set(baseline_tested.get("passed", []))
    baseline_failed = set(baseline_tested.get("failed", []))

    current_passed = set(current_tested.get("passed", []))
    current_failed = set(current_tested.get("failed", []))

    # Calculate changes
    new_failures = current_failed - baseline_failed - baseline_passed
    regressions = (current_failed & baseline_passed)  # Was passing, now failing
    fixed_tests = (current_passed & baseline_failed)  # Was failing, now passing
    still_failing = current_failed & baseline_failed
    still_passing = current_passed & baseline_passed

    # Get pass rates
    baseline_summary = baseline.get("summary", {})
    current_summary = current.get("summary", {})

    baseline_pass_rate = baseline_summary.get("pass_rate", 0.0)
    current_pass_rate = current_summary.get("pass_rate", 0.0)
    pass_rate_change = current_pass_rate - baseline_pass_rate

    # Get coverage rates
    baseline_coverage = baseline_summary.get("coverage_rate", 0.0)
    current_coverage = current_summary.get("coverage_rate", 0.0)
    coverage_change = current_coverage - baseline_coverage

    return {
        "summary": {
            "baseline_pass_rate": round(baseline_pass_rate, 2),
            "current_pass_rate": round(current_pass_rate, 2),
            "pass_rate_change": round(pass_rate_change, 2),
            "baseline_coverage": round(baseline_coverage, 2),
            "current_coverage": round(current_coverage, 2),
            "coverage_change": round(coverage_change, 2),
            "baseline_passed": len(baseline_passed),
            "current_passed": len(current_passed),
            "baseline_failed": len(baseline_failed),
            "current_failed": len(current_failed),
        },
        "changes": {
            "new_failures": sorted(list(new_failures)),
            "regressions": sorted(list(regressions)),
            "fixed_tests": sorted(list(fixed_tests)),
            "still_failing": len(still_failing),
            "still_passing": len(still_passing),
        }
    }


def print_markdown_report(analysis: Dict) -> None:
    """Print analysis in markdown format."""
    summary = analysis["summary"]
    changes = analysis["changes"]

    print("# SQLLogicTest Regression Analysis\n")

    # Summary
    print("## Summary\n")
    print(f"**Pass Rate**: {summary['current_pass_rate']}% ", end="")

    pass_rate_change = summary['pass_rate_change']
    if pass_rate_change > 0:
        print(f"(+{pass_rate_change}% ✅)")
    elif pass_rate_change < 0:
        print(f"({pass_rate_change}% ❌)")
    else:
        print("(unchanged)")

    print(f"**Coverage**: {summary['current_coverage']}% ", end="")

    coverage_change = summary['coverage_change']
    if coverage_change > 0:
        print(f"(+{coverage_change}%)")
    elif coverage_change < 0:
        print(f"({coverage_change}%)")
    else:
        print("(unchanged)")

    print()

    # Comparison table
    print("| Metric | Baseline | Current | Change |")
    print("|--------|----------|---------|--------|")
    print(f"| Pass Rate | {summary['baseline_pass_rate']}% | {summary['current_pass_rate']}% | ", end="")
    if pass_rate_change >= 0:
        print(f"+{pass_rate_change}% |")
    else:
        print(f"{pass_rate_change}% |")

    print(f"| Tests Passing | {summary['baseline_passed']} | {summary['current_passed']} | ", end="")
    pass_delta = summary['current_passed'] - summary['baseline_passed']
    if pass_delta >= 0:
        print(f"+{pass_delta} |")
    else:
        print(f"{pass_delta} |")

    print(f"| Tests Failing | {summary['baseline_failed']} | {summary['current_failed']} | ", end="")
    fail_delta = summary['current_failed'] - summary['baseline_failed']
    if fail_delta > 0:
        print(f"+{fail_delta} |")
    elif fail_delta < 0:
        print(f"{fail_delta} |")
    else:
        print("0 |")

    print()

    # Changes
    print("## Changes\n")

    # Fixed tests (improvements)
    if changes["fixed_tests"]:
        print(f"### ✅ Fixed Tests ({len(changes['fixed_tests'])})\n")
        print("These tests were failing in the baseline but are now passing:\n")
        for test in changes["fixed_tests"][:10]:  # Show first 10
            print(f"- `{test}`")
        if len(changes["fixed_tests"]) > 10:
            print(f"- ... and {len(changes['fixed_tests']) - 10} more")
        print()

    # Regressions (tests that were passing, now failing)
    if changes["regressions"]:
        print(f"### ❌ Regressions ({len(changes['regressions'])})\n")
        print("These tests were passing in the baseline but are now failing:\n")
        for test in changes["regressions"][:10]:
            print(f"- `{test}`")
        if len(changes["regressions"]) > 10:
            print(f"- ... and {len(changes['regressions']) - 10} more")
        print()

    # New failures (tests not in baseline)
    if changes["new_failures"]:
        print(f"### 🆕 New Failures ({len(changes['new_failures'])})\n")
        print("These tests were not tested in baseline but are now failing:\n")
        for test in changes["new_failures"][:10]:
            print(f"- `{test}`")
        if len(changes["new_failures"]) > 10:
            print(f"- ... and {len(changes['new_failures']) - 10} more")
        print()

    # Still failing
    if changes["still_failing"] > 0:
        print(f"### 🔄 Still Failing: {changes['still_failing']} tests\n")

    # Still passing
    if changes["still_passing"] > 0:
        print(f"### ✅ Still Passing: {changes['still_passing']} tests\n")


def main():
    parser = argparse.ArgumentParser(
        description="Detect regressions between two SQLLogicTest runs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "baseline",
        type=Path,
        help="Baseline (previous) test results JSON file",
    )
    parser.add_argument(
        "current",
        type=Path,
        help="Current test results JSON file",
    )
    parser.add_argument(
        "--format",
        choices=["markdown", "json"],
        default="markdown",
        help="Output format (default: markdown)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output file (default: stdout)",
    )

    args = parser.parse_args()

    # Load test results
    try:
        baseline = load_test_results(args.baseline)
        current = load_test_results(args.current)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON: {e}", file=sys.stderr)
        return 1

    # Detect regressions
    analysis = detect_regressions(baseline, current)

    # Output results
    output = sys.stdout
    if args.output:
        output = open(args.output, 'w')

    try:
        if args.format == "json":
            json.dump(analysis, output, indent=2)
            output.write("\n")
        else:
            # Redirect stdout temporarily
            original_stdout = sys.stdout
            sys.stdout = output
            print_markdown_report(analysis)
            sys.stdout = original_stdout
    finally:
        if args.output:
            output.close()

    # Print summary to stderr if outputting to file
    if args.output:
        summary = analysis["summary"]
        print(f"\nRegression analysis saved to {args.output}", file=sys.stderr)
        print(f"Pass rate: {summary['baseline_pass_rate']}% → {summary['current_pass_rate']}% "
              f"({summary['pass_rate_change']:+.2f}%)", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
