#!/usr/bin/env python3
"""
Format benchmark results for display in GitHub Actions summary.
"""
import json
import sys
from pathlib import Path


def format_time(seconds: float) -> str:
    """Format time in appropriate units."""
    if seconds < 0.001:
        return f"{seconds * 1_000_000:.2f} µs"
    elif seconds < 1:
        return f"{seconds * 1000:.2f} ms"
    else:
        return f"{seconds:.2f} s"


def format_results(filepath: str):
    """Format benchmark results as markdown table."""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"❌ Error: File not found: {filepath}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Error: Invalid JSON: {e}")
        sys.exit(1)

    benchmarks = data.get('benchmarks', [])

    if not benchmarks:
        print("No benchmark results found")
        return

    # Print markdown table
    print("| Benchmark | Mean | Std Dev | Min | Max | Rounds |")
    print("|-----------|------|---------|-----|-----|--------|")

    for bench in benchmarks:
        name = bench.get('name', 'unknown')
        stats = bench.get('stats', {})

        mean = stats.get('mean', 0)
        stddev = stats.get('stddev', 0)
        min_time = stats.get('min', 0)
        max_time = stats.get('max', 0)
        rounds = stats.get('rounds', 0)

        print(f"| `{name}` | {format_time(mean)} | {format_time(stddev)} | "
              f"{format_time(min_time)} | {format_time(max_time)} | {rounds} |")

    # Print summary stats
    total_benchmarks = len(benchmarks)
    print()
    print(f"**Total Benchmarks:** {total_benchmarks}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python format_results.py <results.json>")
        sys.exit(1)

    filepath = sys.argv[1]
    format_results(filepath)


if __name__ == '__main__':
    main()
