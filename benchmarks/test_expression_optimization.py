#!/usr/bin/env python3
"""
Benchmark expression optimization performance for Phase 2 validation.

Target: 5-15% reduction in expression evaluation overhead
"""

import time
import sys
import os

# Add the Rust crate to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'target', 'release'))

try:
    import nistmemsql
except ImportError:
    print("Failed to import nistmemsql. Build the Python bindings first:")
    print("cd crates/python-bindings && maturin build --release")
    print("pip3 install target/wheels/nistmemsql-*.whl")
    sys.exit(1)


def setup_data(db, size=10000):
    """Create table with test data."""
    print(f"Creating table with {size} rows...")

    cursor = db.cursor()
    cursor.execute("CREATE TABLE data (id INTEGER, value INTEGER)")

    for i in range(size):
        cursor.execute(f"INSERT INTO data VALUES ({i}, {i * 2})")

    print("Data setup complete.")


def benchmark_constant_folding(db, iterations=100):
    """Benchmark queries with constant expressions."""
    print("Benchmarking constant folding...")

    # Query with constant expressions that should be folded
    query = "SELECT * FROM data WHERE value > 100 AND 1 = 1 AND 5 > 3"

    cursor = db.cursor()
    times = []
    for _ in range(iterations):
        start = time.time()
        cursor.execute(query)
        result = cursor.fetchall()
        elapsed = time.time() - start
        times.append(elapsed)

    avg_time = sum(times) / len(times)
    print(".6f")

    return avg_time


def benchmark_dead_code_elimination(db):
    """Benchmark WHERE false elimination."""
    print("Benchmarking dead code elimination...")

    cursor = db.cursor()

    # This should return instantly (no table scan)
    start = time.time()
    cursor.execute("SELECT * FROM data WHERE false")
    result = cursor.fetchall()
    elapsed = time.time() - start

    result_count = len(result)
    print(".6f")

    # Should be very fast (<0.001s) and return 0 rows
    target_met = elapsed < 0.001 and result_count == 0
    print(f"Target met (<0.001s, 0 rows): {'✓' if target_met else '✗'}")

    return elapsed, result_count, target_met


def benchmark_where_true_elimination(db):
    """Benchmark WHERE true predicate removal."""
    print("Benchmarking WHERE true elimination...")

    cursor = db.cursor()

    # WHERE true should be equivalent to no WHERE clause (full scan)
    start1 = time.time()
    cursor.execute("SELECT * FROM data WHERE true")
    result1 = cursor.fetchall()
    elapsed1 = time.time() - start1

    start2 = time.time()
    cursor.execute("SELECT * FROM data")
    result2 = cursor.fetchall()
    elapsed2 = time.time() - start2

    # Should be approximately the same time
    ratio = elapsed1 / elapsed2 if elapsed2 > 0 else float('inf')
    print(".6f")

    # Should be very close (ratio < 1.1)
    target_met = ratio < 1.1 and len(result1) == len(result2)
    print(f"Target met (ratio <1.1): {'✓' if target_met else '✗'}")

    return ratio, target_met


def main():
    print("=== Phase 2 Expression Optimization Benchmark ===\n")

    # Create database
    db = nistmemsql.Database()
    setup_data(db)

    # Run benchmarks
    const_fold_time = benchmark_constant_folding(db)
    print()

    dead_code_time, dead_code_rows, dead_code_met = benchmark_dead_code_elimination(db)
    print()

    where_true_ratio, where_true_met = benchmark_where_true_elimination(db)
    print()

    print("=== Summary ===")
    print(".6f")
    print(".6f")
    print(".2f")
    print(f"WHERE false optimization: {'✓' if dead_code_met else '✗'}")
    print(f"WHERE true optimization: {'✓' if where_true_met else '✗'}")

    # Overall success: both dead code and where true optimizations working
    success = dead_code_met and where_true_met
    if success:
        print("\n🎉 Expression optimization targets ACHIEVED!")
        return 0
    else:
        print("\n❌ Expression optimization targets NOT fully met")
        return 1


if __name__ == "__main__":
    sys.exit(main())
