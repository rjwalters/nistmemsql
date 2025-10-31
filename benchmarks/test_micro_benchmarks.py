"""
Tier 1 Micro-Benchmarks: Basic operations comparison between nistmemsql and SQLite.

These benchmarks establish fundamental performance characteristics for single-table operations.
Implements the first micro-benchmark as specified in issue #650.
"""
import pytest
import sqlite3
import nistmemsql


def setup_test_table(conn, table_name, num_rows, is_sqlite=True):
    """
    Helper function to create and populate test table.

    Creates a table with the schema specified in issue #650:
    - id INTEGER PRIMARY KEY: Sequential IDs
    - name VARCHAR/TEXT NOT NULL: Random 10-20 character strings
    - value INTEGER NOT NULL: Random integers 0-999999

    Args:
        conn: Database connection (SQLite or nistmemsql)
        table_name: Name of the table to create
        num_rows: Number of rows to insert
        is_sqlite: If True, use SQLite-compatible TEXT type and parameterized queries
    """
    cursor = conn.cursor()

    # Create table with appropriate schema
    # nistmemsql uses VARCHAR instead of TEXT
    text_type = "TEXT" if is_sqlite else "VARCHAR(20)"
    cursor.execute(f"""
        CREATE TABLE {table_name} (
            id INTEGER PRIMARY KEY,
            name {text_type} NOT NULL,
            value INTEGER NOT NULL
        )
    """)

    # Insert test data
    # Using deterministic random data for reproducibility
    import random
    random.seed(42)  # Same seed for both databases

    for i in range(num_rows):
        # Generate random name (10-20 characters)
        name_length = 10 + (i % 11)  # Cycles through 10-20
        name = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for _ in range(name_length))

        # Generate random value (0-999999)
        value = random.randint(0, 999999)

        if is_sqlite:
            # SQLite supports parameterized queries
            cursor.execute(
                f"INSERT INTO {table_name} (id, name, value) VALUES (?, ?, ?)",
                (i, name, value)
            )
        else:
            # nistmemsql doesn't support parameterized queries yet
            # Need to escape single quotes in strings
            escaped_name = name.replace("'", "''")
            cursor.execute(
                f"INSERT INTO {table_name} (id, name, value) VALUES ({i}, '{escaped_name}', {value})"
            )


# Test simple SELECT at 1K scale
def test_simple_select_1k_sqlite(benchmark, sqlite_db):
    """Benchmark SELECT * FROM table_1k on SQLite."""
    setup_test_table(sqlite_db, 'table_1k', 1000, is_sqlite=True)

    def run_query():
        cursor = sqlite_db.cursor()
        cursor.execute("SELECT * FROM table_1k")
        rows = cursor.fetchall()
        return rows

    result = benchmark(run_query)
    assert len(result) == 1000


def test_simple_select_1k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark SELECT * FROM table_1k on nistmemsql."""
    setup_test_table(nistmemsql_db, 'table_1k', 1000, is_sqlite=False)

    def run_query():
        cursor = nistmemsql_db.cursor()
        cursor.execute("SELECT * FROM table_1k")
        rows = cursor.fetchall()
        return rows

    result = benchmark(run_query)
    assert len(result) == 1000


# Test simple SELECT at 10K scale
def test_simple_select_10k_sqlite(benchmark, sqlite_db):
    """Benchmark SELECT * FROM table_10k on SQLite."""
    setup_test_table(sqlite_db, 'table_10k', 10000, is_sqlite=True)

    def run_query():
        cursor = sqlite_db.cursor()
        cursor.execute("SELECT * FROM table_10k")
        rows = cursor.fetchall()
        return rows

    result = benchmark(run_query)
    assert len(result) == 10000


def test_simple_select_10k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark SELECT * FROM table_10k on nistmemsql."""
    setup_test_table(nistmemsql_db, 'table_10k', 10000, is_sqlite=False)

    def run_query():
        cursor = nistmemsql_db.cursor()
        cursor.execute("SELECT * FROM table_10k")
        rows = cursor.fetchall()
        return rows

    result = benchmark(run_query)
    assert len(result) == 10000


# Test simple SELECT at 100K scale
def test_simple_select_100k_sqlite(benchmark, sqlite_db):
    """Benchmark SELECT * FROM table_100k on SQLite."""
    setup_test_table(sqlite_db, 'table_100k', 100000, is_sqlite=True)

    def run_query():
        cursor = sqlite_db.cursor()
        cursor.execute("SELECT * FROM table_100k")
        rows = cursor.fetchall()
        return rows

    result = benchmark(run_query)
    assert len(result) == 100000


def test_simple_select_100k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark SELECT * FROM table_100k on nistmemsql."""
    setup_test_table(nistmemsql_db, 'table_100k', 100000, is_sqlite=False)

    def run_query():
        cursor = nistmemsql_db.cursor()
        cursor.execute("SELECT * FROM table_100k")
        rows = cursor.fetchall()
        return rows

    result = benchmark(run_query)
    assert len(result) == 100000
