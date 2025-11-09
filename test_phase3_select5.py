#!/usr/bin/env python3
"""
Minimal test of select5 pathological case with Phase 3.1 optimization
"""

import vibesql
import time
import tracemalloc

def setup_table(db, table_num):
    """Create a table with 10 rows"""
    table_name = f"t{table_num}"
    
    # Create table
    db.execute(f"""
        CREATE TABLE {table_name} (
            a{table_num} INT,
            b{table_num} INT
        )
    """)
    
    # Insert 10 rows
    for i in range(1, 11):
        db.execute(f"INSERT INTO {table_name} VALUES ({i}, {i + table_num})")

def test_cascading_joins():
    """Test cascading joins similar to select5.test"""
    db = vibesql.Database()
    
    # Setup 4 tables with 10 rows each
    for i in range(1, 5):
        setup_table(db, i)
    
    # Query with cascading equijoins in WHERE clause
    # This tests Phase 3.1 hash join optimization
    query = """
    SELECT COUNT(*) as cnt
    FROM t1, t2, t3, t4
    WHERE a1 = 5
      AND a1 = b2
      AND b2 = a3
      AND a3 = b4
    """
    
    print(f"Testing cascading 4-table join with Phase 3.1...")
    
    tracemalloc.start()
    start = time.time()
    result = db.execute(query)
    elapsed = time.time() - start
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    print(f"  Result rows: {len(result)}")
    if result:
        print(f"  First row: {result[0]}")
    print(f"  Time: {elapsed:.3f}s")
    print(f"  Memory (peak): {peak / (1024*1024):.2f} MB")
    
    # We expect the query to complete quickly with minimal memory
    # Without Phase 3.1: Would build many large intermediate results
    # With Phase 3.1: Hash joins keep intermediate results small
    assert len(result) == 1, f"Expected 1 result row, got {len(result)}"
    assert result[0][0] == 1, f"Expected count=1, got {result[0][0]}"
    
    # Memory should be reasonable (< 50 MB for this simple case)
    assert peak < 50_000_000, f"Memory usage too high: {peak / (1024*1024):.2f} MB"
    
    print("✓ Test passed!")

def test_simple_equijoin():
    """Test basic equijoin from WHERE clause"""
    db = vibesql.Database()
    
    db.execute("""
        CREATE TABLE users (
            id INT,
            name VARCHAR(50)
        )
    """)
    db.execute("INSERT INTO users VALUES (1, 'Alice')")
    db.execute("INSERT INTO users VALUES (2, 'Bob')")
    
    db.execute("""
        CREATE TABLE orders (
            id INT,
            user_id INT,
            amount INT
        )
    """)
    db.execute("INSERT INTO orders VALUES (1, 1, 100)")
    db.execute("INSERT INTO orders VALUES (2, 2, 200)")
    
    # Query with equijoin in WHERE clause (no ON clause)
    result = db.execute("""
        SELECT u.id, u.name, o.amount
        FROM users u, orders o
        WHERE u.id = o.user_id
        ORDER BY u.id
    """)
    
    print(f"Testing simple equijoin from WHERE clause...")
    print(f"  Result: {result}")
    assert len(result) == 2, f"Expected 2 rows, got {len(result)}"
    assert result[0][0] == 1, f"Expected first id=1, got {result[0][0]}"
    assert result[0][1] == "Alice", f"Expected first name=Alice, got {result[0][1]}"
    print("✓ Test passed!")

if __name__ == "__main__":
    print("=" * 60)
    print("Phase 3.1 Join Optimization Tests")
    print("=" * 60)
    
    test_simple_equijoin()
    print()
    
    test_cascading_joins()
    
    print()
    print("=" * 60)
    print("All tests passed!")
    print("=" * 60)
