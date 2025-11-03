#!/usr/bin/env python3
"""Profile DELETE operations to understand performance characteristics.

This script helps identify bottlenecks in DELETE WHERE id = ? operations
by comparing with and without PRIMARY KEY optimization.
"""
import time
import sqlite3
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.venv/lib/python3.12/site-packages'))

import nistmemsql


def profile_nistmemsql_delete(num_rows=1000):
    """Profile nistmemsql DELETE operation."""
    db = nistmemsql.connect()
    cursor = db.cursor()

    # Setup with PRIMARY KEY
    cursor.execute("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100),
            value INTEGER
        )
    """)

    for i in range(num_rows):
        cursor.execute(f"INSERT INTO test_table (id, name, value) VALUES ({i}, 'name_{i % 100}', {i * 10})")

    print(f"nistmemsql: Inserted {num_rows} rows")

    # Profile DELETE with WHERE on PRIMARY KEY
    delete_start = time.perf_counter()
    for i in range(num_rows):
        cursor.execute(f"DELETE FROM test_table WHERE id = {i}")
    delete_time = time.perf_counter() - delete_start

    print(f"nistmemsql DELETE time: {delete_time * 1000:.2f}ms for {num_rows} deletes")
    print(f"nistmemsql per-delete: {delete_time * 1000 / num_rows:.4f}ms")

    return delete_time


def profile_sqlite_delete(num_rows=1000):
    """Profile SQLite DELETE operation."""
    db = sqlite3.connect(":memory:")
    cursor = db.cursor()

    # Setup with PRIMARY KEY
    cursor.execute("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100),
            value INTEGER
        )
    """)

    for i in range(num_rows):
        cursor.execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
                      (i, f'name_{i % 100}', i * 10))
    db.commit()

    print(f"\nSQLite: Inserted {num_rows} rows")

    # Profile DELETE with WHERE on PRIMARY KEY
    delete_start = time.perf_counter()
    for i in range(num_rows):
        cursor.execute("DELETE FROM test_table WHERE id = ?", (i,))
    db.commit()
    delete_time = time.perf_counter() - delete_start

    print(f"SQLite DELETE time: {delete_time * 1000:.2f}ms for {num_rows} deletes")
    print(f"SQLite per-delete: {delete_time * 1000 / num_rows:.4f}ms")

    return delete_time


def main():
    print("=" * 60)
    print("DELETE Performance Profiling")
    print("=" * 60)

    num_rows = 1000

    nist_time = profile_nistmemsql_delete(num_rows)
    sqlite_time = profile_sqlite_delete(num_rows)

    print(f"\n{'=' * 60}")
    print("Summary:")
    print(f"{'=' * 60}")
    print(f"nistmemsql: {nist_time * 1000:.2f}ms ({nist_time / num_rows * 1_000_000:.2f}μs per delete)")
    print(f"SQLite:     {sqlite_time * 1000:.2f}ms ({sqlite_time / num_rows * 1_000_000:.2f}μs per delete)")
    print(f"Ratio:      {nist_time / sqlite_time:.2f}x slower")

    print(f"\nIf PRIMARY KEY index is working, nistmemsql should be close to SQLite speed.")
    print(f"Current gap suggests: {'✅ Optimization working' if nist_time / sqlite_time < 5 else '❌ Optimization not working or other overhead'}")


if __name__ == "__main__":
    main()
