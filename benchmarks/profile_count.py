#!/usr/bin/env python3
"""
Profile COUNT aggregate performance to identify bottlenecks.
"""
import time
import nistmemsql
import sqlite3

def profile_nistmemsql_count(num_rows=1000):
    """Profile nistmemsql COUNT operation."""
    db = nistmemsql.connect()
    cursor = db.cursor()

    # Setup
    cursor.execute("CREATE TABLE test_table (id INT, name VARCHAR(100), value INT)")

    # Insert data
    print(f"Inserting {num_rows} rows into nistmemsql...")
    insert_start = time.perf_counter()
    for i in range(num_rows):
        cursor.execute(f"INSERT INTO test_table (id, name, value) VALUES ({i}, 'name_{i % 100}', {i * 10})")
    insert_time = time.perf_counter() - insert_start
    print(f"  Insert time: {insert_time*1000:.2f} ms")

    # Warm up
    cursor.execute("SELECT COUNT(*) FROM test_table")
    result = cursor.fetchone()
    print(f"  Warmup result: {result}")

    # Profile COUNT
    print(f"\nProfiling COUNT on {num_rows} rows...")
    times = []
    for i in range(10):
        start = time.perf_counter()
        cursor.execute("SELECT COUNT(*) FROM test_table")
        result = cursor.fetchone()
        elapsed = time.perf_counter() - start
        times.append(elapsed * 1_000_000)  # Convert to microseconds
        if i == 0:
            print(f"  Result: {result}")

    avg_time = sum(times) / len(times)
    min_time = min(times)
    max_time = max(times)

    print(f"\nNISTMemSQL COUNT Statistics:")
    print(f"  Min:  {min_time:.2f} μs")
    print(f"  Max:  {max_time:.2f} μs")
    print(f"  Mean: {avg_time:.2f} μs")

    return avg_time

def profile_sqlite_count(num_rows=1000):
    """Profile SQLite COUNT operation."""
    db = sqlite3.connect(":memory:")
    cursor = db.cursor()

    # Setup
    cursor.execute("CREATE TABLE test_table (id INT, name VARCHAR(100), value INT)")

    # Insert data
    print(f"\nInserting {num_rows} rows into SQLite...")
    insert_start = time.perf_counter()
    for i in range(num_rows):
        cursor.execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
                      (i, f'name_{i % 100}', i * 10))
    db.commit()
    insert_time = time.perf_counter() - insert_start
    print(f"  Insert time: {insert_time*1000:.2f} ms")

    # Warm up
    cursor.execute("SELECT COUNT(*) FROM test_table")
    result = cursor.fetchone()
    print(f"  Warmup result: {result}")

    # Profile COUNT
    print(f"\nProfiling COUNT on {num_rows} rows...")
    times = []
    for i in range(10):
        start = time.perf_counter()
        cursor.execute("SELECT COUNT(*) FROM test_table")
        result = cursor.fetchone()
        elapsed = time.perf_counter() - start
        times.append(elapsed * 1_000_000)  # Convert to microseconds
        if i == 0:
            print(f"  Result: {result}")

    avg_time = sum(times) / len(times)
    min_time = min(times)
    max_time = max(times)

    print(f"\nSQLite COUNT Statistics:")
    print(f"  Min:  {min_time:.2f} μs")
    print(f"  Max:  {max_time:.2f} μs")
    print(f"  Mean: {avg_time:.2f} μs")

    return avg_time

if __name__ == "__main__":
    print("=" * 70)
    print("COUNT Aggregate Performance Profiling (After HashMap Optimization)")
    print("=" * 70)

    nist_time = profile_nistmemsql_count(1000)
    sqlite_time = profile_sqlite_count(1000)

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"nistmemsql COUNT: {nist_time:.2f} μs")
    print(f"SQLite COUNT:     {sqlite_time:.2f} μs")
    print(f"Slowdown factor:  {nist_time/sqlite_time:.1f}x")
    print("=" * 70)
