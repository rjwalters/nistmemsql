#!/usr/bin/env python3
"""Analyze TPC-H profiling output and extract key metrics."""

import re
import sys
from collections import defaultdict

def parse_duration(s):
    """Parse duration string to microseconds."""
    s = s.strip()
    if s.endswith('ms'):
        return float(s[:-2]) * 1000
    elif s.endswith('µs'):
        return float(s[:-2])
    elif s.endswith('ns'):
        return float(s[:-2]) / 1000
    elif s.endswith('s') and not s.endswith('ms') and not s.endswith('µs') and not s.endswith('ns'):
        return float(s[:-1]) * 1_000_000
    return 0

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 analyze_profile.py <profile_output.txt>")
        sys.exit(1)

    filename = sys.argv[1]

    # Track metrics per query
    queries = {}
    current_query = None

    # Profile metrics
    profile_metrics = defaultdict(list)

    with open(filename) as f:
        for line in f:
            # Detect query start
            m = re.match(r'^=== (Q\d+) ===$', line)
            if m:
                current_query = m.group(1)
                queries[current_query] = {'status': 'running'}
                continue

            # Capture execute time
            m = re.match(r'^\s+Execute:\s+(.+?)(?:\s+\((\d+) rows\))?$', line)
            if m and current_query:
                time_str = m.group(1).strip()
                if 'ERROR' in time_str:
                    queries[current_query]['status'] = 'timeout'
                    queries[current_query]['time_us'] = parse_duration(time_str.split()[0])
                else:
                    queries[current_query]['status'] = 'ok'
                    queries[current_query]['time_us'] = parse_duration(time_str)
                if m.group(2):
                    queries[current_query]['rows'] = int(m.group(2))
                continue

            # Capture profile metrics
            m = re.match(r'^\[Q6 PROFILE\]\s+(.+?):\s+(.+)$', line)
            if m and current_query:
                metric = m.group(1).strip()
                value = m.group(2).strip()
                # Extract just the duration part
                duration_match = re.match(r'([\d.]+(?:ms|µs|ns|s))', value)
                if duration_match:
                    profile_metrics[(current_query, metric)].append(
                        parse_duration(duration_match.group(1))
                    )

    # Print summary
    print("=" * 70)
    print("TPC-H QUERY SUMMARY")
    print("=" * 70)
    print(f"{'Query':<8} {'Status':<10} {'Time':<15} {'Rows':<10}")
    print("-" * 70)

    for q in sorted(queries.keys(), key=lambda x: int(x[1:])):
        info = queries[q]
        status = info.get('status', 'unknown')
        time_us = info.get('time_us', 0)
        rows = info.get('rows', '-')

        if time_us >= 1_000_000:
            time_str = f"{time_us/1_000_000:.2f}s"
        elif time_us >= 1000:
            time_str = f"{time_us/1000:.2f}ms"
        else:
            time_str = f"{time_us:.2f}µs"

        if status == 'timeout':
            time_str += " TIMEOUT"

        print(f"{q:<8} {status:<10} {time_str:<15} {rows:<10}")

    # Print detailed profile metrics per query
    print("\n" + "=" * 70)
    print("DETAILED PROFILE METRICS (mean values)")
    print("=" * 70)

    for q in sorted(queries.keys(), key=lambda x: int(x[1:])):
        q_metrics = {k[1]: v for k, v in profile_metrics.items() if k[0] == q}
        if q_metrics:
            print(f"\n{q}:")
            for metric, values in sorted(q_metrics.items()):
                mean_us = sum(values) / len(values)
                if mean_us >= 1_000_000:
                    mean_str = f"{mean_us/1_000_000:.2f}s"
                elif mean_us >= 1000:
                    mean_str = f"{mean_us/1000:.2f}ms"
                else:
                    mean_str = f"{mean_us:.2f}µs"
                print(f"  {metric}: {mean_str} (n={len(values)})")

if __name__ == '__main__':
    main()
