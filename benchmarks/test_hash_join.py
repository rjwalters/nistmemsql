#!/usr/bin/env python3
"""
Benchmark hash join performance for Phase 2 validation.

Target: 10K×10K equi-join should complete in <1 second
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


def create_large_tables(db, size=10000):
    """Create tables with specified number of rows."""
    print(f"Creating tables with {size} rows each...")

    cursor = db.cursor()

    # Create tables
    cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50))")
    cursor.execute("CREATE TABLE orders (user_id INTEGER, amount INTEGER)")

    # Insert data
    for i in range(size):
        cursor.execute(f"INSERT INTO users VALUES ({i}, 'User{i}')")
        cursor.execute(f"INSERT INTO orders VALUES ({i % size}, {i * 100})")

    print("Tables created and populated.")


def benchmark_hash_join(db, size=10000):
    """Benchmark equi-join performance."""
    print(f"Benchmarking {size}×{size} equi-join...")

    cursor = db.cursor()
    start = time.time()
    cursor.execute("SELECT * FROM users u JOIN orders o ON u.id = o.user_id")
    result = cursor.fetchall()
    elapsed = time.time() - start

    result_count = len(result)
    print(".3f")

    # Target: <1 second for 10K×10K join
    target_met = elapsed < 1.0
    print(f"Target met (<1s): {'✓' if target_met else '✗'}")

    return elapsed, result_count, target_met


def main():
    print("=== Phase 2 Hash Join Benchmark ===\n")

    # Create database
    db = nistmemsql.Database()

    # Test with different sizes
    sizes = [1000, 5000, 10000]

    results = []
    for size in sizes:
        create_large_tables(db, size)
        elapsed, count, target_met = benchmark_hash_join(db, size)
        results.append((size, elapsed, count, target_met))

        # Clean up for next test
        cursor = db.cursor()
        cursor.execute("DROP TABLE users")
        cursor.execute("DROP TABLE orders")

    print("\n=== Summary ===")
    print("Size | Time (s) | Rows | Target Met")
    print("-----|----------|------|-----------")
    for size, elapsed, count, target_met in results:
        status = "✓" if target_met else "✗"
        print("5d")

    # Check if 10K×10K target was met
    tenk_result = next((r for r in results if r[0] == 10000), None)
    if tenk_result and tenk_result[3]:
        print("\n🎉 Phase 2 hash join target ACHIEVED!")
        return 0
    else:
        print("\n❌ Phase 2 hash join target NOT met")
        return 1


if __name__ == "__main__":
    sys.exit(main())
