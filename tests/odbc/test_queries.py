#!/usr/bin/env python3
"""
ODBC Query Tests for VibeSQL

Requires:
    pip install pyodbc

Usage:
    python test_queries.py
"""

import sys

try:
    import pyodbc
except ImportError:
    print("Error: pyodbc not installed")
    print("Install with: pip install pyodbc")
    sys.exit(1)


def test_connection():
    """Test basic connection"""
    print("Testing connection...")

    try:
        conn = pyodbc.connect(
            'Driver={PostgreSQL Unicode};'
            'Server=localhost;'
            'Port=5432;'
            'Database=testdb;'
            'Uid=testuser;'
            'Pwd=;'
            'SSLMode=disable;'
        )
        print("✓ Connection successful")
        return conn
    except pyodbc.Error as e:
        print(f"✗ Connection failed: {e}")
        sys.exit(1)


def test_simple_query(conn):
    """Test simple SELECT query"""
    print("\nTesting simple query...")

    cursor = conn.cursor()
    cursor.execute("SELECT 1 as test")
    row = cursor.fetchone()

    assert row[0] == 1, f"Expected 1, got {row[0]}"
    print("✓ Simple query successful")


def test_create_table(conn):
    """Test CREATE TABLE"""
    print("\nTesting CREATE TABLE...")

    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS odbc_test")
    cursor.execute("""
        CREATE TABLE odbc_test (
            id INTEGER,
            name VARCHAR(100),
            age INTEGER
        )
    """)
    conn.commit()

    print("✓ CREATE TABLE successful")


def test_insert(conn):
    """Test INSERT"""
    print("\nTesting INSERT...")

    cursor = conn.cursor()
    cursor.execute("INSERT INTO odbc_test VALUES (1, 'Alice', 30)")
    cursor.execute("INSERT INTO odbc_test VALUES (2, 'Bob', 25)")
    conn.commit()

    print("✓ INSERT successful")


def test_select(conn):
    """Test SELECT"""
    print("\nTesting SELECT...")

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM odbc_test ORDER BY id")
    rows = cursor.fetchall()

    assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"
    assert rows[0][1] == 'Alice', f"Expected 'Alice', got {rows[0][1]}"
    assert rows[1][1] == 'Bob', f"Expected 'Bob', got {rows[1][1]}"

    print("✓ SELECT successful")


def test_update(conn):
    """Test UPDATE"""
    print("\nTesting UPDATE...")

    cursor = conn.cursor()
    cursor.execute("UPDATE odbc_test SET age = 31 WHERE name = 'Alice'")
    conn.commit()

    cursor.execute("SELECT age FROM odbc_test WHERE name = 'Alice'")
    age = cursor.fetchone()[0]

    assert age == 31, f"Expected 31, got {age}"
    print("✓ UPDATE successful")


def test_delete(conn):
    """Test DELETE"""
    print("\nTesting DELETE...")

    cursor = conn.cursor()
    cursor.execute("DELETE FROM odbc_test WHERE name = 'Bob'")
    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM odbc_test")
    count = cursor.fetchone()[0]

    assert count == 1, f"Expected 1 row, got {count}"
    print("✓ DELETE successful")


def test_cleanup(conn):
    """Clean up test table"""
    print("\nCleaning up...")

    cursor = conn.cursor()
    cursor.execute("DROP TABLE odbc_test")
    conn.commit()

    print("✓ Cleanup successful")


def main():
    print("=== ODBC Query Tests for VibeSQL ===\n")

    conn = test_connection()

    try:
        test_simple_query(conn)
        test_create_table(conn)
        test_insert(conn)
        test_select(conn)
        test_update(conn)
        test_delete(conn)
        test_cleanup(conn)

        print("\n=== All ODBC tests passed ===")

    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        sys.exit(1)

    finally:
        conn.close()


if __name__ == '__main__':
    main()
