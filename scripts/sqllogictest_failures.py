#!/usr/bin/env python3
"""
SQLLogicTest Failures Summary Script

Fetches the cumulative SQLLogicTest results from GitHub Pages and displays
a summary of all failing test files with additional details if available.

Usage:
    python3 scripts/sqllogictest_failures.py
    python3 scripts/sqllogictest_failures.py --local  # Use local results
"""

import argparse
import json
import sys
import urllib.request
from pathlib import Path
from typing import Dict, List, Any


GITHUB_PAGES_URL = "https://rjwalters.github.io/nistmemsql/badges/sqllogictest_cumulative.json"
LOCAL_CUMULATIVE_PATH = "target/sqllogictest_cumulative.json"
LOCAL_ANALYSIS_PATH = "target/sqllogictest_analysis.json"


def fetch_results(use_local: bool = False) -> Dict[str, Any]:
    """Fetch SQLLogicTest results from GitHub Pages or local file."""
    if use_local:
        cumulative_path = Path(LOCAL_CUMULATIVE_PATH)
        if not cumulative_path.exists():
            print(f"❌ Error: Local cumulative results not found at {LOCAL_CUMULATIVE_PATH}", file=sys.stderr)
            print("Run tests first: cargo test --test sqllogictest_suite --release", file=sys.stderr)
            sys.exit(1)

        with open(cumulative_path) as f:
            return json.load(f)
    else:
        try:
            with urllib.request.urlopen(GITHUB_PAGES_URL) as response:
                return json.loads(response.read())
        except Exception as e:
            print(f"❌ Error fetching results from GitHub Pages: {e}", file=sys.stderr)
            print(f"Tried URL: {GITHUB_PAGES_URL}", file=sys.stderr)
            print("\nTry using --local flag to read from local results instead.", file=sys.stderr)
            sys.exit(1)


def fetch_local_analysis() -> Dict[str, Any]:
    """Fetch local analysis if available."""
    analysis_path = Path(LOCAL_ANALYSIS_PATH)
    if analysis_path.exists():
        with open(analysis_path) as f:
            return json.load(f)
    return {}


def print_summary(results: Dict[str, Any], analysis: Dict[str, Any] = None):
    """Print a formatted summary of failing tests."""
    summary = results.get("summary", {})
    tested_files = results.get("tested_files", {})
    failed_files = tested_files.get("failed", [])
    passed_files = tested_files.get("passed", [])

    print("=" * 80)
    print("SQLLogicTest Failures Summary")
    print("=" * 80)
    print()

    # Overall statistics
    print("📊 Overall Statistics:")
    print(f"  Total available files: {summary.get('total_available_files', 0):,}")
    print(f"  Total tested files:    {summary.get('total_tested_files', 0):,}")
    print(f"  Coverage rate:         {summary.get('coverage_rate', 0):.2f}%")
    print()

    print(f"  ✅ Passed:             {summary.get('passed', 0):,}")
    print(f"  ❌ Failed:             {summary.get('failed', 0):,}")
    print(f"  ⏭️  Untested:           {summary.get('untested', 0):,}")
    print(f"  Pass rate:             {summary.get('pass_rate', 0):.2f}%")
    print()

    # Failing files
    if failed_files:
        print("=" * 80)
        print(f"❌ Failing Test Files ({len(failed_files)})")
        print("=" * 80)
        print()

        for i, file_path in enumerate(failed_files, 1):
            print(f"{i}. {file_path}")

            # Show file location
            full_path = f"third_party/sqllogictest/test/{file_path}"
            print(f"   📁 Location: {full_path}")

            # Try to find error details in analysis if available
            if analysis:
                missing_features = analysis.get("missing_features", {})
                if missing_features:
                    print(f"   🔍 Possible issues:")
                    for feature, count in missing_features.items():
                        print(f"      • {feature}")

            print()
    else:
        print("🎉 No failing tests! All tested files passed.")
        print()

    # Error categories from analysis
    if analysis:
        error_categories = analysis.get("error_categories", {})
        if error_categories:
            print("=" * 80)
            print("📋 Error Categories")
            print("=" * 80)
            print()
            for category, count in error_categories.items():
                print(f"  • {category}: {count}")
            print()

        missing_features = analysis.get("missing_features", {})
        if missing_features:
            print("=" * 80)
            print("🚧 Missing Features Detected")
            print("=" * 80)
            print()
            for feature, count in sorted(missing_features.items(), key=lambda x: -x[1]):
                print(f"  • {feature}: {count} occurrence(s)")
            print()

    # Merge info (if this is a cumulative result)
    merge_info = results.get("merge_info", {})
    if merge_info:
        print("=" * 80)
        print("ℹ️  Merge Information")
        print("=" * 80)
        print()
        print(f"  Last updated:          {merge_info.get('merged_at', 'unknown')}")
        print(f"  Total merges:          {merge_info.get('total_merges', 0)}")
        print(f"  New files this merge:  {merge_info.get('new_files_tested', 0)}")
        print()

    # Recommendations
    if failed_files:
        print("=" * 80)
        print("💡 Next Steps")
        print("=" * 80)
        print()
        print("To investigate a failing test:")
        print(f"  1. View the test file: cat third_party/sqllogictest/test/{failed_files[0]}")
        print(f"  2. Run the specific test: cargo test --test sqllogictest_suite {failed_files[0].split('/')[0]} -- --nocapture")
        print(f"  3. Check for missing features or data types")
        print()


def main():
    parser = argparse.ArgumentParser(
        description="Display summary of failing SQLLogicTest files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch results from GitHub Pages (deployed version)
  python3 scripts/sqllogictest_failures.py

  # Use local test results
  python3 scripts/sqllogictest_failures.py --local
        """
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Use local results from target/ instead of GitHub Pages"
    )

    args = parser.parse_args()

    # Fetch results
    results = fetch_results(use_local=args.local)

    # Fetch local analysis if available (only when using local results)
    analysis = None
    if args.local:
        analysis = fetch_local_analysis()

    # Print summary
    print_summary(results, analysis)


if __name__ == "__main__":
    main()
