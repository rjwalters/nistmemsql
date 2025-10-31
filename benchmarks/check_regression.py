#!/usr/bin/env python3
"""
Check for performance regressions between current and baseline benchmarks.

Exit codes:
  0 - No regressions detected
  1 - Regressions found or error occurred
"""
import json
import sys
from pathlib import Path


def load_results(filepath: str) -> dict:
    """Load benchmark results from JSON file."""
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Error: File not found: {filepath}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Error: Invalid JSON in {filepath}: {e}")
        sys.exit(1)


def extract_benchmarks(data: dict) -> dict:
    """Extract benchmark results into a name -> stats dict."""
    benchmarks = {}

    if 'benchmarks' not in data:
        print("❌ Error: No 'benchmarks' key in results")
        sys.exit(1)

    for bench in data['benchmarks']:
        name = bench.get('name', 'unknown')
        stats = bench.get('stats', {})
        benchmarks[name] = {
            'mean': stats.get('mean', 0),
            'stddev': stats.get('stddev', 0),
            'min': stats.get('min', 0),
            'max': stats.get('max', 0),
        }

    return benchmarks


def check_regression(current_file: str, baseline_file: str, threshold: float = 0.10) -> bool:
    """
    Compare current benchmark results to baseline.

    Args:
        current_file: Path to current benchmark results
        baseline_file: Path to baseline benchmark results
        threshold: Regression threshold (default 10% = 0.10)

    Returns:
        True if regressions found, False otherwise
    """
    print(f"📊 Checking for performance regressions (threshold: {threshold*100:.0f}%)")
    print(f"   Current: {current_file}")
    print(f"   Baseline: {baseline_file}")
    print()

    current_data = load_results(current_file)
    baseline_data = load_results(baseline_file)

    current_benchmarks = extract_benchmarks(current_data)
    baseline_benchmarks = extract_benchmarks(baseline_data)

    regressions = []
    improvements = []

    for name, current_stats in current_benchmarks.items():
        if name not in baseline_benchmarks:
            print(f"⚠️  New benchmark: {name} (no baseline)")
            continue

        baseline_stats = baseline_benchmarks[name]
        current_mean = current_stats['mean']
        baseline_mean = baseline_stats['mean']

        if baseline_mean == 0:
            print(f"⚠️  Skipping {name}: baseline mean is 0")
            continue

        # Calculate change (positive = slower/regression, negative = faster/improvement)
        change = (current_mean - baseline_mean) / baseline_mean
        change_pct = change * 100

        if change > threshold:
            regressions.append({
                'name': name,
                'current': current_mean,
                'baseline': baseline_mean,
                'change_pct': change_pct
            })
        elif change < -0.05:  # Report improvements > 5%
            improvements.append({
                'name': name,
                'current': current_mean,
                'baseline': baseline_mean,
                'change_pct': change_pct
            })

    # Report improvements
    if improvements:
        print("✅ Performance Improvements:")
        for imp in improvements:
            print(f"   {imp['name']}")
            print(f"      Baseline: {imp['baseline']*1000:.2f} ms")
            print(f"      Current:  {imp['current']*1000:.2f} ms")
            print(f"      Change:   {abs(imp['change_pct']):.1f}% faster 🚀")
            print()

    # Report regressions
    if regressions:
        print("❌ Performance Regressions Detected:")
        print()
        for reg in regressions:
            print(f"   {reg['name']}")
            print(f"      Baseline: {reg['baseline']*1000:.2f} ms")
            print(f"      Current:  {reg['current']*1000:.2f} ms")
            print(f"      Change:   +{reg['change_pct']:.1f}% slower ⚠️")
            print()

        print(f"❌ Found {len(regressions)} regression(s) exceeding {threshold*100:.0f}% threshold")
        return True
    else:
        print("✅ No significant performance regressions detected")
        return False


def main():
    if len(sys.argv) < 3:
        print("Usage: python check_regression.py <current_results.json> <baseline_results.json> [threshold]")
        print()
        print("Example: python check_regression.py current.json baseline.json 0.10")
        print("         (threshold defaults to 0.10 = 10% regression)")
        sys.exit(1)

    current_file = sys.argv[1]
    baseline_file = sys.argv[2]
    threshold = float(sys.argv[3]) if len(sys.argv) > 3 else 0.10

    has_regressions = check_regression(current_file, baseline_file, threshold)

    sys.exit(1 if has_regressions else 0)


if __name__ == '__main__':
    main()
