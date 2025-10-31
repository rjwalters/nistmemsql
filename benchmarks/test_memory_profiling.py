"""
Tier 4 Memory Profiling: Memory usage analysis.

Track memory allocation patterns, peak usage, and efficiency
characteristics of both databases under various workloads.
"""
import pytest
import psutil
import os


def test_memory_baseline_empty_db(benchmark, sqlite_connection, nistmemsql_connection):
    """Measure memory usage of empty databases."""
    def measure_memory():
        process = psutil.Process(os.getpid())
        memory_before = process.memory_info().rss

        # Create connections (memory measurement point)
        sqlite_conn = sqlite_connection
        # nistmemsql_conn = nistmemsql_connection

        memory_after = process.memory_info().rss
        return memory_after - memory_before

    benchmark(measure_memory)


def test_memory_simple_select(benchmark, sqlite_connection, nistmemsql_connection):
    """Measure memory usage during simple SELECT operations."""
    setup_memory_test_data(sqlite_connection)
    # setup_memory_test_data(nistmemsql_connection)

    def measure_query_memory():
        process = psutil.Process(os.getpid())
        memory_before = process.memory_info().rss

        # Execute queries
        sqlite_cursor = sqlite_connection.cursor()
        sqlite_cursor.execute("SELECT * FROM memory_test_table")

        # nistmemsql_cursor = nistmemsql_connection.cursor()
        # nistmemsql_cursor.execute("SELECT * FROM memory_test_table")

        memory_after = process.memory_info().rss
        return memory_after - memory_before

    benchmark(measure_query_memory)


def test_memory_complex_join(benchmark, sqlite_connection, nistmemsql_connection):
    """Measure memory usage during complex multi-table joins."""
    setup_memory_join_data(sqlite_connection)
    # setup_memory_join_data(nistmemsql_connection)

    def measure_join_memory():
        process = psutil.Process(os.getpid())
        memory_before = process.memory_info().rss

        # Execute complex join
        sqlite_cursor = sqlite_connection.cursor()
        sqlite_cursor.execute("""
            SELECT t1.id, t2.data, t3.value
            FROM table_a t1
            INNER JOIN table_b t2 ON t1.id = t2.id
            INNER JOIN table_c t3 ON t2.ref_id = t3.id
            WHERE t1.category = 'ACTIVE'
        """)

        # nistmemsql_cursor = nistmemsql_connection.cursor()
        # nistmemsql_cursor.execute(...)  # Same query

        memory_after = process.memory_info().rss
        return memory_after - memory_before

    benchmark(measure_join_memory)


def test_memory_aggregation_workload(benchmark, sqlite_connection, nistmemsql_connection):
    """Measure memory usage during aggregation operations."""
    setup_memory_aggregation_data(sqlite_connection)
    # setup_memory_aggregation_data(nistmemsql_connection)

    def measure_aggregation_memory():
        process = psutil.Process(os.getpid())
        memory_before = process.memory_info().rss

        # Execute aggregation queries
        sqlite_cursor = sqlite_connection.cursor()
        sqlite_cursor.execute("""
            SELECT
                category,
                COUNT(*) as count,
                SUM(value) as total,
                AVG(value) as average,
                MIN(value) as minimum,
                MAX(value) as maximum
            FROM agg_test_table
            GROUP BY category
            HAVING COUNT(*) > 10
            ORDER BY total DESC
        """)

        # nistmemsql_cursor = nistmemsql_connection.cursor()
        # nistmemsql_cursor.execute(...)  # Same query

        memory_after = process.memory_info().rss
        return memory_after - memory_before

    benchmark(measure_aggregation_memory)


def test_memory_peak_usage_during_load(benchmark, sqlite_connection, nistmemsql_connection):
    """Measure peak memory usage during bulk data loading."""
    def measure_load_memory():
        process = psutil.Process(os.getpid())
        peak_memory = 0

        # Monitor memory during bulk insert
        for batch in range(10):
            memory_before = process.memory_info().rss

            sqlite_cursor = sqlite_connection.cursor()
            # Insert 1000 rows
            for i in range(1000):
                row_id = batch * 1000 + i
                sqlite_cursor.execute(
                    "INSERT INTO bulk_test_table VALUES (?, ?, ?)",
                    (row_id, f'name_{row_id}', row_id * 1.5)
                )

            memory_after = process.memory_info().rss
            peak_memory = max(peak_memory, memory_after - memory_before)

        return peak_memory

    benchmark(measure_load_memory)


def setup_memory_test_data(connection):
    """Create test data for memory profiling."""
    cursor = connection.cursor()

    cursor.execute("""
        CREATE TABLE memory_test_table (
            id INTEGER PRIMARY KEY,
            name TEXT,
            value REAL,
            data BLOB
        )
    """)

    # Insert test data with some larger blobs
    for i in range(1000):
        cursor.execute(
            "INSERT INTO memory_test_table VALUES (?, ?, ?, ?)",
            (i, f'name_{i}', float(i), b'x' * (i % 100))  # Variable blob sizes
        )

    connection.commit()


def setup_memory_join_data(connection):
    """Create multi-table data for join memory testing."""
    cursor = connection.cursor()

    cursor.execute("""
        CREATE TABLE table_a (
            id INTEGER PRIMARY KEY,
            category TEXT,
            data TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE table_b (
            id INTEGER PRIMARY KEY,
            ref_id INTEGER,
            data TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE table_c (
            id INTEGER PRIMARY KEY,
            value REAL
        )
    """)

    # Insert test data
    for i in range(500):
        cursor.execute("INSERT INTO table_a VALUES (?, ?, ?)",
                      (i, 'ACTIVE' if i % 2 == 0 else 'INACTIVE', f'data_{i}'))

    for i in range(400):
        cursor.execute("INSERT INTO table_b VALUES (?, ?, ?)",
                      (i, i % 500, f'join_data_{i}'))

    for i in range(300):
        cursor.execute("INSERT INTO table_c VALUES (?, ?)",
                      (i, float(i) * 2.5))

    connection.commit()


def setup_memory_aggregation_data(connection):
    """Create data for aggregation memory testing."""
    cursor = connection.cursor()

    cursor.execute("""
        CREATE TABLE agg_test_table (
            id INTEGER PRIMARY KEY,
            category TEXT,
            value REAL,
            timestamp TEXT
        )
    """)

    # Insert test data with various categories
    categories = ['A', 'B', 'C', 'D', 'E']
    for i in range(5000):
        cursor.execute(
            "INSERT INTO agg_test_table VALUES (?, ?, ?, ?)",
            (i, categories[i % len(categories)], float(i) * 1.1, f'2023-01-{i%28+1:02d}')
        )

    connection.commit()
