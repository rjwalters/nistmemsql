#!/usr/bin/env python3
"""
Run TPC-H benchmarks and convert Criterion output to web demo format.

This script:
1. Runs cargo bench for TPC-H queries
2. Parses Criterion's JSON output
3. Converts to format expected by web-demo/src/benchmarks.ts
4. Outputs benchmark_results.json for the web demo
"""

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List

# TPC-H Query descriptions (from TPCH_README.md)
TPCH_QUERIES = {
    "q1": "Pricing Summary Report",
    "q2": "Minimum Cost Supplier",
    "q3": "Shipping Priority",
    "q4": "Order Priority Checking",
    "q5": "Local Supplier Volume",
    "q6": "Forecasting Revenue Change",
    "q7": "Volume Shipping",
    "q8": "National Market Share",
    "q9": "Product Type Profit Measure",
    "q10": "Returned Item Reporting",
    "q11": "Important Stock Identification",
    "q12": "Shipping Modes Priority",
    "q13": "Customer Distribution",
    "q14": "Promotion Effect",
    "q15": "Top Supplier",
    "q16": "Parts/Supplier Relationship",
    "q17": "Small-Quantity-Order Revenue",
    "q18": "Large Volume Customer",
    "q19": "Discounted Revenue",
    "q20": "Potential Part Promotion",
    "q21": "Suppliers Who Kept Orders Waiting",
    "q22": "Global Sales Opportunity",
}


def run_tpch_benchmarks(quick: bool = False) -> Dict:
    """Run TPC-H benchmarks with Criterion and return parsed results."""
    print("🚀 Running TPC-H benchmarks...")
    print(f"   Mode: {'Quick (limited queries)' if quick else 'Full (all 22 queries)'}")

    # Build command
    cmd = [
        "cargo", "bench",
        "--bench", "tpch_benchmark",
        "--features", "benchmark-comparison",
        "--",
        "--output-format", "bencher",
    ]

    # For quick mode, only run a few representative queries
    if quick:
        # Q1: Simple aggregation, Q3: Joins, Q6: Filtering
        cmd.extend(["q1", "q3", "q6"])

    # Run benchmarks
    result = subprocess.run(
        cmd,
        cwd=Path(__file__).parent.parent,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"❌ Benchmark failed!")
        print(f"STDOUT:\n{result.stdout}")
        print(f"STDERR:\n{result.stderr}")
        sys.exit(1)

    print("✅ Benchmarks completed")
    return parse_criterion_output(result.stdout)


def parse_criterion_output(output: str) -> Dict:
    """Parse Criterion's bencher format output."""
    benchmarks = []

    for line in output.split('\n'):
        # Criterion bencher format: "test <name> ... bench: <time> ns/iter (+/- <stddev>)"
        if 'bench:' in line:
            parts = line.split()
            if len(parts) < 5:
                continue

            # Extract name (between "test" and "...")
            try:
                test_idx = parts.index('test')
                dots_idx = parts.index('...')
                name = ' '.join(parts[test_idx + 1:dots_idx])

                # Extract time (after "bench:")
                bench_idx = parts.index('bench:')
                time_ns = float(parts[bench_idx + 1].replace(',', ''))
                time_s = time_ns / 1_000_000_000  # Convert to seconds

                # Extract stddev if available
                stddev_s = 0.0
                if '(+/-' in line:
                    paren_idx = line.index('(+/-')
                    stddev_part = line[paren_idx + 5:].split(')')[0].strip()
                    stddev_ns = float(stddev_part.replace(',', ''))
                    stddev_s = stddev_ns / 1_000_000_000

                benchmarks.append({
                    "name": name,
                    "stats": {
                        "mean": time_s,
                        "stddev": stddev_s,
                        "min": time_s - stddev_s,  # Approximation
                        "max": time_s + stddev_s,  # Approximation
                        "rounds": 5  # Criterion default
                    }
                })
            except (ValueError, IndexError) as e:
                print(f"⚠️  Warning: Failed to parse line: {line}")
                print(f"   Error: {e}")
                continue

    return {
        "benchmarks": benchmarks,
        "datetime": datetime.utcnow().isoformat() + "Z",
        "machine_info": {
            "note": "GitHub Actions runner (ubuntu-latest)",
            "benchmark_type": "TPC-H Decision Support Queries",
            "scale_factor": "SF 0.01 (~60,000 rows)"
        }
    }


def convert_to_web_demo_format(parsed_data: Dict) -> Dict:
    """Convert parsed Criterion output to web demo format."""
    # Group benchmarks by query and database
    # Expected format: test_<operation>_<database>
    # e.g., "tpch_q1_vibesql", "tpch_q1_sqlite", "tpch_q1_duckdb"

    reformatted_benchmarks = []

    for bench in parsed_data["benchmarks"]:
        name = bench["name"]

        # Parse: "tpch_q1_vibesql" -> operation="tpch_q1", database="vibesql"
        parts = name.split('_')
        if len(parts) < 3:
            continue

        # Extract query number and database
        # Format: tpch_qN_database or tpch_qN/database/SFX.XX
        query = None
        database = None

        if '/vibesql/' in name or name.endswith('_vibesql'):
            database = 'vibesql'
        elif '/sqlite/' in name or name.endswith('_sqlite'):
            database = 'sqlite'
        elif '/duckdb/' in name or name.endswith('_duckdb'):
            database = 'duckdb'

        # Extract query number (q1, q2, etc.)
        for q_num in range(1, 23):
            if f'_q{q_num}_' in name or f'/q{q_num}/' in name:
                query = f'q{q_num}'
                break

        if not query or not database:
            print(f"⚠️  Warning: Could not parse benchmark name: {name}")
            continue

        # Get query description
        query_desc = TPCH_QUERIES.get(query, f"Query {query}")

        # Create formatted name for web demo
        # Format: "tpch_q1_pricing_summary_vibesql"
        operation_name = f"tpch_{query}_{query_desc.lower().replace(' ', '_').replace('/', '_')}"
        formatted_name = f"{operation_name}_{database}"

        reformatted_benchmarks.append({
            "name": formatted_name,
            "stats": bench["stats"]
        })

    return {
        "benchmarks": reformatted_benchmarks,
        "datetime": parsed_data["datetime"],
        "machine_info": parsed_data.get("machine_info", {})
    }


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Run TPC-H benchmarks")
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Run quick benchmarks (only Q1, Q3, Q6)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="benchmark_results.json",
        help="Output file path (default: benchmark_results.json)"
    )

    args = parser.parse_args()

    # Run benchmarks
    parsed_data = run_tpch_benchmarks(quick=args.quick)

    # Convert to web demo format
    web_demo_data = convert_to_web_demo_format(parsed_data)

    # Write output
    output_path = Path(args.output)
    with open(output_path, 'w') as f:
        json.dump(web_demo_data, f, indent=2)

    print(f"\n✅ Benchmark results written to: {output_path}")
    print(f"   Total benchmarks: {len(web_demo_data['benchmarks'])}")
    print(f"   Queries tested: {len(set(b['name'].split('_')[2] for b in web_demo_data['benchmarks']))}")
    print(f"   Databases: {', '.join(set(b['name'].split('_')[-1] for b in web_demo_data['benchmarks']))}")


if __name__ == "__main__":
    main()
