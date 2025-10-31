"""
Tier 1 Micro-Benchmarks: Basic operations comparison between nistmemsql and SQLite.

These benchmarks establish fundamental performance characteristics for single-table operations.
"""
import pytest


def test_simple_select_1k(benchmark, sqlite_connection, nistmemsql_connection):
    """Benchmark simple SELECT * FROM table_1k."""
    # Setup test data in both databases
    setup_test_table(sqlite_connection, 1000)
    # setup_test_table(nistmemsql_connection, 1000)  # TODO: Implement when bindings exist

    def run_queries():
        # SQLite query
        sqlite_cursor = sqlite_connection.cursor()
        sqlite_cursor.execute("SELECT * FROM test_table")

        # nistmemsql query (placeholder)
        # nistmemsql_cursor = nistmemsql_connection.cursor()
        # nistmemsql_cursor.execute("SELECT * FROM test_table")
        pass

    benchmark(run_queries)


def test_simple_select_10k(benchmark, sqlite_connection, nistmemsql_connection):
    """Benchmark simple SELECT * FROM table_10k."""
    setup_test_table(sqlite_connection, 10000)
    # setup_test_table(nistmemsql_connection, 10000)

    def run_queries():
        sqlite_cursor = sqlite_connection.cursor()
        sqlite_cursor.execute("SELECT * FROM test_table")

        # nistmemsql_cursor = nistmemsql_connection.cursor()
        # nistmemsql_cursor.execute("SELECT * FROM test_table")
        pass

    benchmark(run_queries)


def test_aggregation_count(benchmark, sqlite_connection, nistmemsql_connection):
    """Benchmark COUNT(*) aggregation."""
    setup_test_table(sqlite_connection, 10000)
    # setup_test_table(nistmemsql_connection, 10000)

    def run_queries():
        sqlite_cursor = sqlite_connection.cursor()
        sqlite_cursor.execute("SELECT COUNT(*) FROM test_table")

        # nistmemsql_cursor = nistmemsql_connection.cursor()
        # nistmemsql_cursor.execute("SELECT COUNT(*) FROM test_table")
        pass

    benchmark(run_queries)


def test_aggregation_sum(benchmark, sqlite_connection, nistmemsql_connection):
    """Benchmark SUM(value) aggregation."""
    setup_test_table(sqlite_connection, 10000)
    # setup_test_table(nistmemsql_connection, 10000)

    def run_queries():
        sqlite_cursor = sqlite_connection.cursor()
        sqlite_cursor.execute("SELECT SUM(value) FROM test_table")

        # nistmemsql_cursor = nistmemsql_connection.cursor()
        # nistmemsql_cursor.execute("SELECT SUM(value) FROM test_table")
        pass

    benchmark(run_queries)


def test_where_clause(benchmark, sqlite_connection, nistmemsql_connection):
    """Benchmark SELECT with WHERE clause."""
    setup_test_table(sqlite_connection, 10000)
    # setup_test_table(nistmemsql_connection, 10000)

    def run_queries():
        sqlite_cursor = sqlite_connection.cursor()
        sqlite_cursor.execute("SELECT * FROM test_table WHERE id < 1000")

        # nistmemsql_cursor = nistmemsql_connection.cursor()
        # nistmemsql_cursor.execute("SELECT * FROM test_table WHERE id < 1000")
        pass

    benchmark(run_queries)


def setup_test_table(connection, num_rows):
    """Helper function to create and populate test table."""
    cursor = connection.cursor()

    # Create table
    cursor.execute("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name TEXT,
            value INTEGER,
            data REAL
        )
    """)

    # Insert test data
    for i in range(num_rows):
        cursor.execute(
            "INSERT INTO test_table (id, name, value, data) VALUES (?, ?, ?, ?)",
            (i, f"name_{i}", i * 10, float(i) / 100.0)
        )

    connection.commit()
