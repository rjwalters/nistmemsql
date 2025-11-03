#!/usr/bin/env python3
"""
Profile UPDATE operations to identify bottlenecks.
"""
import time
import nistmemsql
import sqlite3

def profile_nistmemsql_update(num_rows=1000):
    """Profile nistmemsql UPDATE operation."""
    db = nistmemsql.connect()
    cursor = db.cursor()

    # Setup
    print(f"Setting up table with {num_rows} rows...")
    setup_start = time.perf_counter()
    cursor.execute("CREATE TABLE test_table (id INT, name VARCHAR(100), value INT)")
    for i in range(num_rows):
        cursor.execute(f"INSERT INTO test_table (id, name, value) VALUES ({i}, 'name_{i % 100}', {i * 10})")
    setup_time = time.perf_counter() - setup_start
    print(f"  Setup time: {setup_time*1000:.2f} ms")

    # Profile UPDATE with WHERE
    print(f"\nProfiling {num_rows} individual UPDATEs...")
    update_start = time.perf_counter()
    for i in range(num_rows):
        cursor.execute(f"UPDATE test_table SET value = value + 1 WHERE id = {i}")
    update_time = time.perf_counter() - update_start

    print(f"\nNISTMemSQL UPDATE Statistics:")
    print(f"  Total time: {update_time*1000:.2f} ms")
    print(f"  Time per UPDATE: {(update_time/num_rows)*1000:.2f} ms")
    print(f"  UPDATEs per second: {num_rows/update_time:.2f}")

    return update_time

def profile_sqlite_update(num_rows=1000):
    """Profile SQLite UPDATE operation."""
    db = sqlite3.connect(":memory:")
    cursor = db.cursor()

    # Setup
    print(f"\nSetting up SQLite table with {num_rows} rows...")
    setup_start = time.perf_counter()
    cursor.execute("CREATE TABLE test_table (id INT, name VARCHAR(100), value INT)")
    for i in range(num_rows):
        cursor.execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
                      (i, f'name_{i % 100}', i * 10))
    db.commit()
    setup_time = time.perf_counter() - setup_start
    print(f"  Setup time: {setup_time*1000:.2f} ms")

    # Profile UPDATE with WHERE
    print(f"\nProfiling {num_rows} individual UPDATEs...")
    update_start = time.perf_counter()
    for i in range(num_rows):
        cursor.execute("UPDATE test_table SET value = value + 1 WHERE id = ?", (i,))
    db.commit()
    update_time = time.perf_counter() - update_start

    print(f"\nSQLite UPDATE Statistics:")
    print(f"  Total time: {update_time*1000:.2f} ms")
    print(f"  Time per UPDATE: {(update_time/num_rows)*1000:.2f} ms")
    print(f"  UPDATEs per second: {num_rows/update_time:.2f}")

    return update_time

def test_single_update_overhead():
    """Test overhead of a single UPDATE to identify if it's per-operation cost."""
    print("\n" + "="*70)
    print("Single UPDATE Overhead Test")
    print("="*70)

    # nistmemsql
    db = nistmemsql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE test (id INT, value INT)")
    cursor.execute("INSERT INTO test (id, value) VALUES (1, 100)")

    times = []
    for i in range(10):
        start = time.perf_counter()
        cursor.execute("UPDATE test SET value = value + 1 WHERE id = 1")
        elapsed = time.perf_counter() - start
        times.append(elapsed * 1000)

    print(f"nistmemsql single UPDATE: {sum(times)/len(times):.3f} ms (avg of 10)")

    # SQLite
    db = sqlite3.connect(":memory:")
    cursor = db.cursor()
    cursor.execute("CREATE TABLE test (id INT, value INT)")
    cursor.execute("INSERT INTO test (id, value) VALUES (?, ?)", (1, 100))
    db.commit()

    times = []
    for i in range(10):
        start = time.perf_counter()
        cursor.execute("UPDATE test SET value = value + 1 WHERE id = ?", (1,))
        elapsed = time.perf_counter() - start
        times.append(elapsed * 1000)

    print(f"SQLite single UPDATE: {sum(times)/len(times):.3f} ms (avg of 10)")

if __name__ == "__main__":
    print("=" * 70)
    print("UPDATE Performance Profiling")
    print("=" * 70)

    test_single_update_overhead()

    print("\n" + "=" * 70)
    print("Bulk UPDATE Test (1000 rows)")
    print("=" * 70)

    nist_time = profile_nistmemsql_update(1000)
    sqlite_time = profile_sqlite_update(1000)

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"nistmemsql total: {nist_time*1000:.2f} ms")
    print(f"SQLite total:     {sqlite_time*1000:.2f} ms")
    print(f"Slowdown factor:  {nist_time/sqlite_time:.1f}x")
    print("=" * 70)
