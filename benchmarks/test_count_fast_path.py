#!/usr/bin/env python3
"""
Test to verify COUNT(*) fast path is working.
"""
import time
import nistmemsql

# Create database and table
db = nistmemsql.connect()
cursor = db.cursor()
cursor.execute("CREATE TABLE test_table (id INT, name VARCHAR(100), value INT)")

# Insert 1000 rows
for i in range(1000):
    cursor.execute(f"INSERT INTO test_table (id, name, value) VALUES ({i}, 'name_{i % 100}', {i * 10})")

# Test 1: Simple COUNT(*) - should use fast path
times = []
for i in range(10):
    start = time.perf_counter()
    cursor.execute("SELECT COUNT(*) FROM test_table")
    result = cursor.fetchone()
    elapsed = time.perf_counter() - start
    times.append(elapsed * 1_000_000)

avg_fast_path = sum(times) / len(times)
print(f"Simple COUNT(*) [FAST PATH]: {avg_fast_path:.2f} μs")

# Test 2: COUNT(*) with GROUP BY - should NOT use fast path
times = []
for i in range(10):
    start = time.perf_counter()
    cursor.execute("SELECT COUNT(*) FROM test_table GROUP BY value")
    result = cursor.fetchall()
    elapsed = time.perf_counter() - start
    times.append(elapsed * 1_000_000)

avg_no_fast_path = sum(times) / len(times)
print(f"COUNT(*) with GROUP BY [NO FAST PATH]: {avg_no_fast_path:.2f} μs")

# Test 3: COUNT(*) with WHERE - should NOT use fast path
times = []
for i in range(10):
    start = time.perf_counter()
    cursor.execute("SELECT COUNT(*) FROM test_table WHERE id < 500")
    result = cursor.fetchone()
    elapsed = time.perf_counter() - start
    times.append(elapsed * 1_000_000)

avg_with_where = sum(times) / len(times)
print(f"COUNT(*) with WHERE [NO FAST PATH]: {avg_with_where:.2f} μs")

print(f"\nFast path speedup vs GROUP BY: {avg_no_fast_path / avg_fast_path:.1f}x")
print(f"Fast path speedup vs WHERE: {avg_with_where / avg_fast_path:.1f}x")

# If fast path is working, it should be 5-10x faster than the other two
if avg_fast_path * 5 > avg_no_fast_path:
    print("\n❌ WARNING: Fast path may not be working! It's not significantly faster than GROUP BY.")
else:
    print("\n✅ Fast path appears to be working correctly!")
