"""
INSERT Benchmarks: Compare INSERT performance across databases at various scales.

Tests INSERT operations for SQLite, vibesql, and DuckDB at:
- 1k, 2k, 5k, 10k, and 20k rows
"""
import pytest
import random


# Test sizes to benchmark
SIZES = [1000, 2000, 5000, 10000, 20000]


def test_insert_1k_sqlite(benchmark, sqlite_db):
    """Benchmark INSERT operations on SQLite (1k rows)."""
    _run_insert_test(benchmark, sqlite_db, 1000, 'sqlite')


def test_insert_1k_vibesql(benchmark, vibesql_db):
    """Benchmark INSERT operations on vibesql (1k rows)."""
    _run_insert_test(benchmark, vibesql_db, 1000, 'vibesql')


def test_insert_1k_duckdb(benchmark, duckdb_db):
    """Benchmark INSERT operations on DuckDB (1k rows)."""
    _run_insert_test(benchmark, duckdb_db, 1000, 'duckdb')


# def test_insert_2k_sqlite(benchmark, sqlite_db):
#     """Benchmark INSERT operations on SQLite (2k rows)."""
#     _run_insert_test(benchmark, sqlite_db, 2000, 'sqlite')


# def test_insert_2k_nistmemsql(benchmark, nistmemsql_db):
#     """Benchmark INSERT operations on nistmemsql (2k rows)."""
#     _run_insert_test(benchmark, nistmemsql_db, 2000, 'nistmemsql')


# def test_insert_2k_duckdb(benchmark, duckdb_db):
#     """Benchmark INSERT operations on DuckDB (2k rows)."""
#     _run_insert_test(benchmark, duckdb_db, 2000, 'duckdb')


# def test_insert_5k_sqlite(benchmark, sqlite_db):
#     """Benchmark INSERT operations on SQLite (5k rows)."""
#     _run_insert_test(benchmark, sqlite_db, 5000, 'sqlite')


# def test_insert_5k_nistmemsql(benchmark, nistmemsql_db):
#     """Benchmark INSERT operations on nistmemsql (5k rows)."""
#     _run_insert_test(benchmark, nistmemsql_db, 5000, 'nistmemsql')


# def test_insert_5k_duckdb(benchmark, duckdb_db):
#     """Benchmark INSERT operations on DuckDB (5k rows)."""
#     _run_insert_test(benchmark, duckdb_db, 5000, 'duckdb')


# def test_insert_10k_sqlite(benchmark, sqlite_db):
#     """Benchmark INSERT operations on SQLite (10k rows)."""
#     _run_insert_test(benchmark, sqlite_db, 10000, 'sqlite')


# def test_insert_10k_nistmemsql(benchmark, nistmemsql_db):
#     """Benchmark INSERT operations on nistmemsql (10k rows)."""
#     _run_insert_test(benchmark, nistmemsql_db, 10000, 'nistmemsql')


# def test_insert_10k_duckdb(benchmark, duckdb_db):
#     """Benchmark INSERT operations on DuckDB (10k rows)."""
#     _run_insert_test(benchmark, duckdb_db, 10000, 'duckdb')


# def test_insert_20k_sqlite(benchmark, sqlite_db):
#     """Benchmark INSERT operations on SQLite (20k rows)."""
#     _run_insert_test(benchmark, sqlite_db, 20000, 'sqlite')


# def test_insert_20k_nistmemsql(benchmark, nistmemsql_db):
#     """Benchmark INSERT operations on nistmemsql (20k rows)."""
#     _run_insert_test(benchmark, nistmemsql_db, 20000, 'nistmemsql')


# def test_insert_20k_duckdb(benchmark, duckdb_db):
#     """Benchmark INSERT operations on DuckDB (20k rows)."""
#     _run_insert_test(benchmark, duckdb_db, 20000, 'duckdb')


def _run_insert_test(benchmark, connection, num_rows, db_type):
    """Helper to run INSERT benchmark for a given database and row count."""
    # Setup: Create empty table (once)
    cursor = connection.cursor()
    cursor.execute("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR(20) NOT NULL,
            value INTEGER NOT NULL
        )
    """)

    # Track next ID to insert
    next_id = [0]  # Use list to allow modification in closure

    def run_inserts():
        # Reset random seed for deterministic data
        random.seed(42)
        cursor = connection.cursor()
        start_id = next_id[0]

        # Insert rows
        for i in range(num_rows):
            if db_type in ['sqlite', 'duckdb']:
                cursor.execute(
                    "INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
                    (start_id + i, f"name_{i % 100}", random.randint(1, 1000))
                )
            else:  # vibesql
                value = random.randint(1, 1000)
                cursor.execute(
                    f"INSERT INTO test_table (id, name, value) VALUES ({start_id + i}, 'name_{i % 100}', {value})"
                )

        next_id[0] += num_rows
        if db_type in ['sqlite', 'duckdb']:
            connection.commit()

    benchmark(run_inserts)
