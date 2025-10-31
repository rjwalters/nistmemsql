"""
Tier 3 Conformance Benchmarks: NIST SQL:1999 test suite performance.

Measure execution time for SQL:1999 conformance tests to understand
performance impact of standards compliance features.
"""
import pytest
import sqlite3
import time


def test_nist_e011_basic_aggregates(benchmark, sqlite_connection, nistmemsql_connection):
    """
    E011: Basic Aggregates (COUNT, SUM, AVG, MIN, MAX)

    Test performance of basic aggregation functions.
    """
    setup_nist_test_data(sqlite_connection)
    # setup_nist_test_data(nistmemsql_connection)

    def run_e011_tests():
        # Test queries from E011 feature
        sqlite_cursor = sqlite_connection.cursor()

        # COUNT(*)
        sqlite_cursor.execute("SELECT COUNT(*) FROM test_table")

        # SUM with GROUP BY
        sqlite_cursor.execute("""
            SELECT category, SUM(value), COUNT(*)
            FROM test_table
            GROUP BY category
        """)

        # AVG, MIN, MAX
        sqlite_cursor.execute("""
            SELECT AVG(value), MIN(value), MAX(value)
            FROM test_table
        """)

        # nistmemsql tests would go here
        # nistmemsql_cursor = nistmemsql_connection.cursor()
        # ... same queries ...

    benchmark(run_e011_tests)


def test_nist_f031_basic_outer_join(benchmark, sqlite_connection, nistmemsql_connection):
    """
    F031: Basic Outer Join (LEFT, RIGHT, FULL)

    Test performance of outer join operations.
    """
    setup_nist_join_data(sqlite_connection)
    # setup_nist_join_data(nistmemsql_connection)

    def run_f031_tests():
        sqlite_cursor = sqlite_connection.cursor()

        # LEFT OUTER JOIN
        sqlite_cursor.execute("""
            SELECT t1.id, t1.value, t2.data
            FROM table1 t1
            LEFT OUTER JOIN table2 t2 ON t1.id = t2.id
        """)

        # RIGHT OUTER JOIN
        sqlite_cursor.execute("""
            SELECT t1.id, t2.id, t1.value, t2.data
            FROM table1 t1
            RIGHT OUTER JOIN table2 t2 ON t1.id = t2.id
        """)

        # FULL OUTER JOIN (if supported)
        try:
            sqlite_cursor.execute("""
                SELECT t1.id, t2.id, t1.value, t2.data
                FROM table1 t1
                FULL OUTER JOIN table2 t2 ON t1.id = t2.id
            """)
        except sqlite3.OperationalError:
            # SQLite doesn't support FULL OUTER JOIN
            pass

        # nistmemsql tests would go here
        # nistmemsql_cursor = nistmemsql_connection.cursor()
        # ... same queries ...

    benchmark(run_f031_tests)


def test_nist_f041_basic_join(benchmark, sqlite_connection, nistmemsql_connection):
    """
    F041: Basic Join (INNER, various join conditions)

    Test performance of inner join operations.
    """
    setup_nist_join_data(sqlite_connection)
    # setup_nist_join_data(nistmemsql_connection)

    def run_f041_tests():
        sqlite_cursor = sqlite_connection.cursor()

        # Simple INNER JOIN
        sqlite_cursor.execute("""
            SELECT t1.id, t1.value, t2.data
            FROM table1 t1
            INNER JOIN table2 t2 ON t1.id = t2.id
        """)

        # Multi-table join
        sqlite_cursor.execute("""
            SELECT t1.id, t2.data, t3.info
            FROM table1 t1
            INNER JOIN table2 t2 ON t1.id = t2.id
            INNER JOIN table3 t3 ON t2.id = t3.id
        """)

        # Join with WHERE conditions
        sqlite_cursor.execute("""
            SELECT t1.id, t1.value, t2.data
            FROM table1 t1
            INNER JOIN table2 t2 ON t1.id = t2.id
            WHERE t1.value > 50 AND t2.data IS NOT NULL
        """)

        # nistmemsql tests would go here

    benchmark(run_f041_tests)


def setup_nist_test_data(connection):
    """Create test data for NIST E011 aggregate tests."""
    cursor = connection.cursor()

    cursor.execute("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            category TEXT,
            value REAL
        )
    """)

    # Insert test data
    for i in range(1000):
        cursor.execute(
            "INSERT INTO test_table (id, category, value) VALUES (?, ?, ?)",
            (i, f'CAT{i%10}', float(i))
        )

    connection.commit()


def setup_nist_join_data(connection):
    """Create test data for NIST join tests."""
    cursor = connection.cursor()

    # Create tables
    cursor.execute("""
        CREATE TABLE table1 (
            id INTEGER PRIMARY KEY,
            value INTEGER
        )
    """)

    cursor.execute("""
        CREATE TABLE table2 (
            id INTEGER PRIMARY KEY,
            data TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE table3 (
            id INTEGER PRIMARY KEY,
            info TEXT
        )
    """)

    # Insert test data
    for i in range(500):
        cursor.execute("INSERT INTO table1 VALUES (?, ?)", (i, i * 10))

    for i in range(300):  # Some missing IDs for outer join testing
        cursor.execute("INSERT INTO table2 VALUES (?, ?)", (i*2, f'data_{i}'))

    for i in range(200):  # Even more sparse
        cursor.execute("INSERT INTO table3 VALUES (?, ?)", (i*3, f'info_{i}'))

    connection.commit()
