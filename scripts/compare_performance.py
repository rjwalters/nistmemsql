#!/usr/bin/env python3
"""
Generate performance comparison report from pytest-benchmark results.

Reads benchmark_results.json and generates a formatted comparison report
showing nistmemsql vs SQLite performance ratios.
"""
import json
import sys
from pathlib import Path
from typing import Dict, List, Tuple


def load_benchmark_results(results_path: str = "benchmark_results.json") -> Dict:
    """Load pytest-benchmark JSON results."""
    path = Path(results_path)
    if not path.exists():
        print(f"Error: {results_path} not found. Run benchmarks first.", file=sys.stderr)
        sys.exit(1)

    with path.open() as f:
        return json.load(f)


def calculate_ratio(nistmemsql_time: float, sqlite_time: float) -> float:
    """Calculate performance ratio (nistmemsql / sqlite)."""
    if sqlite_time == 0:
        return float('inf')
    return nistmemsql_time / sqlite_time


def group_benchmarks_by_test(benchmarks: List[Dict]) -> Dict[str, Dict[str, Dict]]:
    """Group benchmarks by test name and database."""
    grouped = {}

    for bench in benchmarks:
        # Parse test name - format: test_name[db_name]
        full_name = bench['name']

        # Split on [ to separate test name from parameter
        if '[' in full_name:
            test_name, param_part = full_name.split('[', 1)
            db_name = param_part.rstrip(']')
        else:
            # Non-parametrized test - use database name from test name
            if 'sqlite' in full_name:
                test_name = full_name.replace('_sqlite', '')
                db_name = 'sqlite'
            elif 'nistmemsql' in full_name:
                test_name = full_name.replace('_nistmemsql', '')
                db_name = 'nistmemsql'
            else:
                # Skip tests we can't categorize
                continue

        if test_name not in grouped:
            grouped[test_name] = {}

        grouped[test_name][db_name] = bench

    return grouped


def format_time(seconds: float) -> Tuple[float, str]:
    """Format time with appropriate unit (ms, ¼s, ns)."""
    if seconds >= 0.001:
        return seconds * 1000, "ms"
    elif seconds >= 0.000001:
        return seconds * 1000000, "¼s"
    else:
        return seconds * 1000000000, "ns"


def generate_comparison_table(grouped: Dict[str, Dict[str, Dict]]) -> str:
    """Generate markdown comparison table."""
    lines = []
    lines.append("# Performance Comparison: nistmemsql vs SQLite\n")
    lines.append("| Benchmark | nistmemsql | SQLite | Ratio | Status |")
    lines.append("|-----------|------------|--------|-------|--------|")

    ratios = []

    for test_name in sorted(grouped.keys()):
        dbs = grouped[test_name]

        # Skip if we don't have both databases
        if 'nistmemsql' not in dbs or 'sqlite' not in dbs:
            continue

        nist_stats = dbs['nistmemsql']['stats']
        sqlite_stats = dbs['sqlite']['stats']

        # Use median for comparison (more robust than mean)
        nist_time = nist_stats['median']
        sqlite_time = sqlite_stats['median']

        ratio = calculate_ratio(nist_time, sqlite_time)
        ratios.append(ratio)

        # Format times
        nist_val, nist_unit = format_time(nist_time)
        sqlite_val, sqlite_unit = format_time(sqlite_time)

        # Status indicator
        if ratio <= 1.5:
            status = " Good"
        elif ratio <= 3.0:
            status = "Ë Fair"
        else:
            status = "  Slow"

        lines.append(
            f"| {test_name} | "
            f"{nist_val:.2f} {nist_unit} | "
            f"{sqlite_val:.2f} {sqlite_unit} | "
            f"{ratio:.2f}x | "
            f"{status} |"
        )

    # Add summary statistics
    if ratios:
        lines.append("")
        lines.append("## Summary Statistics")
        lines.append("")
        lines.append(f"- **Geometric Mean Ratio**: {geometric_mean(ratios):.2f}x")
        lines.append(f"- **Median Ratio**: {sorted(ratios)[len(ratios)//2]:.2f}x")
        lines.append(f"- **Min Ratio**: {min(ratios):.2f}x")
        lines.append(f"- **Max Ratio**: {max(ratios):.2f}x")
        lines.append(f"- **Tests Measured**: {len(ratios)}")

    return "\n".join(lines)


def geometric_mean(values: List[float]) -> float:
    """Calculate geometric mean of values."""
    if not values:
        return 0.0

    product = 1.0
    for v in values:
        product *= v

    return product ** (1.0 / len(values))


def generate_json_summary(grouped: Dict[str, Dict[str, Dict]],
                         machine_info: Dict) -> Dict:
    """Generate JSON summary matching BENCHMARK_STRATEGY.md format."""
    from datetime import datetime

    summary = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "hardware": machine_info.get("machine_info", {}),
        "benchmarks": {},
        "summary": {}
    }

    ratios = []

    for test_name in sorted(grouped.keys()):
        dbs = grouped[test_name]

        if 'nistmemsql' not in dbs or 'sqlite' not in dbs:
            continue

        nist_time = dbs['nistmemsql']['stats']['median']
        sqlite_time = dbs['sqlite']['stats']['median']
        ratio = calculate_ratio(nist_time, sqlite_time)
        ratios.append(ratio)

        # Convert to milliseconds
        summary["benchmarks"][test_name] = {
            "nistmemsql_ms": nist_time * 1000,
            "sqlite_ms": sqlite_time * 1000,
            "ratio": round(ratio, 2)
        }

    if ratios:
        summary["summary"] = {
            "geometric_mean_ratio": round(geometric_mean(ratios), 2),
            "median_ratio": round(sorted(ratios)[len(ratios)//2], 2),
            "range": [round(min(ratios), 2), round(max(ratios), 2)]
        }

    return summary


def main():
    """Main entry point."""
    print("Loading benchmark results...")
    results = load_benchmark_results()

    print("Grouping benchmarks...")
    grouped = group_benchmarks_by_test(results['benchmarks'])

    print("\nGenerating comparison table...")
    table = generate_comparison_table(grouped)
    print(table)

    # Save markdown report
    with open("benchmark_comparison.md", "w") as f:
        f.write(table)
    print("\n Saved comparison table to benchmark_comparison.md")

    # Generate and save JSON summary
    json_summary = generate_json_summary(grouped, results)
    with open("benchmark_summary.json", "w") as f:
        json.dump(json_summary, f, indent=2)
    print(" Saved JSON summary to benchmark_summary.json")


if __name__ == "__main__":
    main()
