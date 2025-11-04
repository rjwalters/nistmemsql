#!/usr/bin/env python3
"""
Simple profiling test to validate instrumentation and measure operation breakdown
"""

import vibesql

# Enable profiling
vibesql.enable_profiling()

# Create connection and cursor
conn = vibesql.connect()
cursor = conn.cursor()

print("=" * 80)
print("Testing CREATE TABLE")
print("=" * 80)
cursor.execute("CREATE TABLE test (id INTEGER, value INTEGER)")

print("\n" + "=" * 80)
print("Testing INSERT (single row)")
print("=" * 80)
cursor.execute("INSERT INTO test VALUES (1, 100)")

print("\n" + "=" * 80)
print("Testing INSERT (10 rows)")
print("=" * 80)
for i in range(2, 12):
    cursor.execute(f"INSERT INTO test VALUES ({i}, {i * 100})")

print("\n" + "=" * 80)
print("Testing COUNT(*)")
print("=" * 80)
cursor.execute("SELECT COUNT(*) FROM test")
result = cursor.fetchall()
print(f"Count result: {result}")

print("\n" + "=" * 80)
print("Testing SELECT (all rows)")
print("=" * 80)
cursor.execute("SELECT * FROM test")
result = cursor.fetchall()
print(f"Selected {len(result)} rows")

print("\n" + "=" * 80)
print("Testing UPDATE")
print("=" * 80)
cursor.execute("UPDATE test SET value = value + 1 WHERE id = 1")

print("\n" + "=" * 80)
print("Testing DELETE")
print("=" * 80)
cursor.execute("DELETE FROM test WHERE id = 1")

# Disable profiling
nistmemsql.disable_profiling()

print("\n" + "=" * 80)
print("Profiling test complete!")
print("=" * 80)
