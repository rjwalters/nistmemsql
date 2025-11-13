"""Benchmark result formatting per BENCHMARK_STRATEGY.md specifications"""
import json
from datetime import datetime
from pathlib import Path

def calculate_ratio(nistmemsql_time, sqlite_time):
    """Calculate performance ratio (nistmemsql vs SQLite)"""
    if sqlite_time == 0:
        return float('inf')
    return round(nistmemsql_time / sqlite_time, 2)

def format_benchmark_results(benchmark_data, hardware_metadata):
    """Format results matching BENCHMARK_STRATEGY.md JSON schema

    Expected output format:
    {
        "timestamp": "2025-10-30T10:00:00Z",
        "hardware": {...},
        "benchmarks": {
            "micro": {...},
            "tpch": {...}
        },
        "summary": {
            "geometric_mean_ratio": 1.8,
            "median_ratio": 1.6,
            "range": [1.1, 3.2]
        }
    }
    """
    results = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "hardware": hardware_metadata,
        "benchmarks": {},
        "summary": {}
    }

    # Process benchmark data from pytest-benchmark
    # (Implementation depends on pytest-benchmark output structure)

    return results

def save_results_json(results, output_path="benchmark_results.json"):
    """Save results to JSON file"""
    output_file = Path(output_path)
    with output_file.open('w') as f:
        json.dump(results, f, indent=2)
    print(f"Results saved to {output_path}")

def generate_comparison_table(results):
    """Generate markdown table from results"""
    table = "| Benchmark | nistmemsql | SQLite | Ratio |\n"
    table += "|-----------|------------|--------|-------|\n"

    for name, data in results.get('benchmarks', {}).items():
        nist_time = data.get('nistmemsql_ms', 0)
        sqlite_time = data.get('sqlite_ms', 0)
        ratio = data.get('ratio', 0)
        table += f"| {name} | {nist_time:.2f} ms | {sqlite_time:.2f} ms | {ratio:.2f}x |\n"

    return table
