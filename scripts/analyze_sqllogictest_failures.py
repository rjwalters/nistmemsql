#!/usr/bin/env python3
"""
Analyze SQLLogicTest failures to identify opportunities for improving conformance.

This script examines the cumulative SQLLogicTest results and generates insights about:
- Common failure patterns
- Missing SQL features
- Database-specific features being tested
- Prioritization recommendations based on test coverage

Usage:
    python scripts/analyze_sqllogictest_failures.py [cumulative_results.json]
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple


def build_files_dict(results: Dict) -> Dict[str, str]:
    """Build a file path -> status dictionary from tested_files structure."""
    files = {}

    # Add passed files
    for file_path in results.get("tested_files", {}).get("passed", []):
        files[file_path] = "passed"

    # Add failed files
    for file_path in results.get("tested_files", {}).get("failed", []):
        files[file_path] = "failed"

    return files


def analyze_file_patterns(files: Dict[str, str]) -> Dict[str, List[str]]:
    """Analyze file naming patterns to identify feature categories."""
    patterns = defaultdict(list)

    for file_path, status in files.items():
        # Use full path for pattern matching (directory names contain feature info)
        path_lower = file_path.lower()

        # Extract database-specific tests
        if "postgresql" in path_lower:
            patterns["PostgreSQL-specific"].append(file_path)
        elif "mysql" in path_lower:
            patterns["MySQL-specific"].append(file_path)
        elif "sqlite" in path_lower:
            patterns["SQLite-specific"].append(file_path)

        # Extract feature categories from path patterns
        if "index" in path_lower:
            patterns["Indexes"].append(file_path)
        elif "join" in path_lower:
            patterns["Joins"].append(file_path)
        elif "aggregate" in path_lower or "groupby" in path_lower:
            patterns["Aggregates/GROUP BY"].append(file_path)
        elif "view" in path_lower:
            patterns["Views"].append(file_path)
        elif "subquery" in path_lower or "subselect" in path_lower:
            patterns["Subqueries"].append(file_path)
        elif "window" in path_lower:
            patterns["Window Functions"].append(file_path)
        elif "transaction" in path_lower or "commit" in path_lower:
            patterns["Transactions"].append(file_path)
        elif "constraint" in path_lower or "foreign" in path_lower:
            patterns["Constraints"].append(file_path)
        elif "trigger" in path_lower:
            patterns["Triggers"].append(file_path)
        elif "random" in path_lower:
            patterns["Random/Edge Cases"].append(file_path)
        elif "select" in path_lower and "from" not in path_lower:
            patterns["SELECT queries"].append(file_path)
        elif "ddl" in path_lower:
            patterns["DDL (CREATE/ALTER/DROP)"].append(file_path)
        elif "evidence" in path_lower:
            patterns["Evidence Tests"].append(file_path)

    return patterns


def categorize_failures(files: Dict[str, str]) -> Tuple[Dict[str, int], Dict[str, List[str]]]:
    """Categorize failures by feature type."""
    feature_failures = defaultdict(int)
    feature_examples = defaultdict(list)

    patterns = analyze_file_patterns(files)

    for category, file_list in patterns.items():
        for file_path in file_list:
            status = files.get(file_path, "untested")
            if status == "failed":
                feature_failures[category] += 1
                if len(feature_examples[category]) < 5:  # Keep max 5 examples
                    feature_examples[category].append(file_path)

    return feature_failures, feature_examples


def calculate_quick_wins(files: Dict[str, str], patterns: Dict[str, List[str]]) -> List[Tuple[str, int, int]]:
    """Identify quick wins - small feature categories with high test counts."""
    quick_wins = []

    for category, file_list in patterns.items():
        if category.endswith("-specific"):  # Skip database-specific features
            continue

        total = len(file_list)
        if total == 0:
            continue

        failed = sum(1 for f in file_list if files.get(f) == "failed")
        passed = sum(1 for f in file_list if files.get(f) == "passed")

        # Quick wins: small categories (5-20 files) with mostly failures
        if 5 <= total <= 20 and failed > 0:
            impact_score = failed  # Simple impact = number of tests we'd fix
            quick_wins.append((category, total, impact_score))

    # Sort by impact score descending
    quick_wins.sort(key=lambda x: x[2], reverse=True)
    return quick_wins


def calculate_high_impact(files: Dict[str, str], patterns: Dict[str, List[str]]) -> List[Tuple[str, int, int, float]]:
    """Identify high-impact features - categories with many failing tests."""
    high_impact = []

    for category, file_list in patterns.items():
        if category.endswith("-specific"):  # Skip database-specific features
            continue

        total = len(file_list)
        if total == 0:
            continue

        failed = sum(1 for f in file_list if files.get(f) == "failed")
        passed = sum(1 for f in file_list if files.get(f) == "passed")

        if failed > 0:
            failure_rate = (failed / total) * 100
            # High impact: many failing tests (>10)
            if failed >= 10:
                high_impact.append((category, total, failed, failure_rate))

    # Sort by number of failures descending
    high_impact.sort(key=lambda x: x[2], reverse=True)
    return high_impact


def analyze_vendor_specific(files: Dict[str, str], patterns: Dict[str, List[str]]) -> Dict[str, Dict[str, int]]:
    """Analyze vendor-specific test results."""
    vendor_stats = {}

    for vendor in ["PostgreSQL-specific", "MySQL-specific", "SQLite-specific"]:
        if vendor not in patterns:
            continue

        file_list = patterns[vendor]
        stats = {
            "total": len(file_list),
            "passed": sum(1 for f in file_list if files.get(f) == "passed"),
            "failed": sum(1 for f in file_list if files.get(f) == "failed"),
            "untested": sum(1 for f in file_list if files.get(f) == "untested"),
        }
        vendor_stats[vendor] = stats

    return vendor_stats


def main():
    parser = argparse.ArgumentParser(
        description="Analyze SQLLogicTest failures for improvement opportunities",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "results_file",
        nargs="?",
        default="target/sqllogictest_cumulative.json",
        help="Path to cumulative results JSON (default: target/sqllogictest_cumulative.json)",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json", "markdown"],
        default="markdown",
        help="Output format (default: markdown)",
    )

    args = parser.parse_args()

    # Load results
    results_path = Path(args.results_file)
    if not results_path.exists():
        print(f"Error: Results file not found: {results_path}", file=sys.stderr)
        return 1

    with open(results_path) as f:
        results = json.load(f)

    # Build files dictionary from tested_files structure
    files = build_files_dict(results)

    # Analyze
    summary = results.get("summary", {})
    patterns = analyze_file_patterns(files)
    feature_failures, feature_examples = categorize_failures(files)
    quick_wins = calculate_quick_wins(files, patterns)
    high_impact = calculate_high_impact(files, patterns)
    vendor_stats = analyze_vendor_specific(files, patterns)

    # Output based on format
    if args.format == "json":
        output = {
            "summary": summary,
            "feature_failures": dict(feature_failures),
            "quick_wins": [{"category": c, "total_tests": t, "impact": i} for c, t, i in quick_wins],
            "high_impact": [{"category": c, "total": t, "failed": f, "failure_rate": r}
                           for c, t, f, r in high_impact],
            "vendor_specific": vendor_stats,
        }
        print(json.dumps(output, indent=2))

    elif args.format == "markdown":
        print("# SQLLogicTest Failure Analysis")
        print()

        # Summary
        print("## Summary")
        print()
        print(f"- **Total Files**: {summary.get('total_available_files', 0)}")
        print(f"- **Tested**: {summary.get('total_tested_files', 0)} ({summary.get('coverage_rate', 0):.1f}%)")
        print(f"- **Passed**: {summary.get('passed', 0)}")
        print(f"- **Failed**: {summary.get('failed', 0)}")
        print(f"- **Pass Rate**: {summary.get('pass_rate', 0):.1f}%")
        print()

        # Quick wins
        if quick_wins:
            print("## Quick Wins")
            print()
            print("Small feature categories that could significantly improve pass rate:")
            print()
            print("| Category | Total Tests | Failing Tests | Impact |")
            print("|----------|-------------|---------------|--------|")
            for category, total, impact in quick_wins[:10]:
                print(f"| {category} | {total} | {impact} | {'🔥' * min(impact // 2, 5)} |")
            print()

        # High impact
        if high_impact:
            print("## High-Impact Features")
            print()
            print("Categories with the most failing tests:")
            print()
            print("| Category | Total Tests | Failed | Failure Rate |")
            print("|----------|-------------|--------|--------------|")
            for category, total, failed, rate in high_impact[:10]:
                print(f"| {category} | {total} | {failed} | {rate:.1f}% |")
            print()

        # Vendor-specific
        if vendor_stats:
            print("## Vendor-Specific Tests")
            print()
            print("Database-specific features (informational - may not be priorities):")
            print()
            print("| Database | Total | Passed | Failed | Untested |")
            print("|----------|-------|--------|--------|----------|")
            for vendor, stats in vendor_stats.items():
                vendor_name = vendor.replace("-specific", "")
                print(f"| {vendor_name} | {stats['total']} | {stats['passed']} | "
                      f"{stats['failed']} | {stats['untested']} |")
            print()

        # Recommendations
        print("## Recommendations")
        print()
        print("### Priority 1: Quick Wins")
        if quick_wins:
            print()
            for i, (category, total, impact) in enumerate(quick_wins[:3], 1):
                print(f"{i}. **{category}** - Fix {impact} failing tests with small implementation effort")
                if category in feature_examples and feature_examples[category]:
                    print(f"   - Example test: `{Path(feature_examples[category][0]).name}`")
        else:
            print("- No quick wins identified")
        print()

        print("### Priority 2: High-Impact Features")
        if high_impact:
            print()
            for i, (category, total, failed, rate) in enumerate(high_impact[:3], 1):
                print(f"{i}. **{category}** - {failed} failing tests ({rate:.0f}% failure rate)")
                if category in feature_examples and feature_examples[category]:
                    print(f"   - Example test: `{Path(feature_examples[category][0]).name}`")
        else:
            print("- No high-impact opportunities identified")
        print()

        print("### Next Steps")
        print()
        print("1. Review the example test files for quick wins to understand implementation requirements")
        print("2. Implement missing features starting with quick wins")
        print("3. Re-run tests to verify improvements")
        print("4. Track progress - aim for 40%+ pass rate on standard SQL tests")
        print()

    else:  # text format
        print("=== SQLLogicTest Failure Analysis ===")
        print()
        print(f"Tested: {summary.get('total_tested_files', 0)}/{summary.get('total_available_files', 0)}")
        print(f"Passed: {summary.get('passed', 0)}")
        print(f"Failed: {summary.get('failed', 0)}")
        print(f"Pass Rate: {summary.get('pass_rate', 0):.1f}%")
        print()

        if quick_wins:
            print("Quick Wins:")
            for category, total, impact in quick_wins[:5]:
                print(f"  - {category}: {impact} failures in {total} tests")

        if high_impact:
            print()
            print("High Impact:")
            for category, total, failed, rate in high_impact[:5]:
                print(f"  - {category}: {failed} failures ({rate:.0f}%)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
