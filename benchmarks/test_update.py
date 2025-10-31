"""
UPDATE Benchmarks: Compare UPDATE performance across databases at various scales.

Tests UPDATE operations for SQLite, nistmemsql, and DuckDB at:
- 1k, 2k, 5k, 10k, and 20k rows
"""
import pytest
from utils.database_setup import setup_test_table


def test_update_1k_sqlite(benchmark, sqlite_db):
    """Benchmark UPDATE operations on SQLite (1k rows)."""
    setup_test_table(sqlite_db, 1000, 'sqlite')
    _run_update_test(benchmark, sqlite_db, 1000, 'sqlite')


def test_update_1k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark UPDATE operations on nistmemsql (1k rows)."""
    setup_test_table(nistmemsql_db, 1000, 'nistmemsql')
    _run_update_test(benchmark, nistmemsql_db, 1000, 'nistmemsql')


def test_update_1k_duckdb(benchmark, duckdb_db):
    """Benchmark UPDATE operations on DuckDB (1k rows)."""
    setup_test_table(duckdb_db, 1000, 'duckdb')
    _run_update_test(benchmark, duckdb_db, 1000, 'duckdb')


def test_update_2k_sqlite(benchmark, sqlite_db):
    """Benchmark UPDATE operations on SQLite (2k rows)."""
    setup_test_table(sqlite_db, 2000, 'sqlite')
    _run_update_test(benchmark, sqlite_db, 2000, 'sqlite')


def test_update_2k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark UPDATE operations on nistmemsql (2k rows)."""
    setup_test_table(nistmemsql_db, 2000, 'nistmemsql')
    _run_update_test(benchmark, nistmemsql_db, 2000, 'nistmemsql')


def test_update_2k_duckdb(benchmark, duckdb_db):
    """Benchmark UPDATE operations on DuckDB (2k rows)."""
    setup_test_table(duckdb_db, 2000, 'duckdb')
    _run_update_test(benchmark, duckdb_db, 2000, 'duckdb')


def test_update_5k_sqlite(benchmark, sqlite_db):
    """Benchmark UPDATE operations on SQLite (5k rows)."""
    setup_test_table(sqlite_db, 5000, 'sqlite')
    _run_update_test(benchmark, sqlite_db, 5000, 'sqlite')


def test_update_5k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark UPDATE operations on nistmemsql (5k rows)."""
    setup_test_table(nistmemsql_db, 5000, 'nistmemsql')
    _run_update_test(benchmark, nistmemsql_db, 5000, 'nistmemsql')


def test_update_5k_duckdb(benchmark, duckdb_db):
    """Benchmark UPDATE operations on DuckDB (5k rows)."""
    setup_test_table(duckdb_db, 5000, 'duckdb')
    _run_update_test(benchmark, duckdb_db, 5000, 'duckdb')


def test_update_10k_sqlite(benchmark, sqlite_db):
    """Benchmark UPDATE operations on SQLite (10k rows)."""
    setup_test_table(sqlite_db, 10000, 'sqlite')
    _run_update_test(benchmark, sqlite_db, 10000, 'sqlite')


def test_update_10k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark UPDATE operations on nistmemsql (10k rows)."""
    setup_test_table(nistmemsql_db, 10000, 'nistmemsql')
    _run_update_test(benchmark, nistmemsql_db, 10000, 'nistmemsql')


def test_update_10k_duckdb(benchmark, duckdb_db):
    """Benchmark UPDATE operations on DuckDB (10k rows)."""
    setup_test_table(duckdb_db, 10000, 'duckdb')
    _run_update_test(benchmark, duckdb_db, 10000, 'duckdb')


def test_update_20k_sqlite(benchmark, sqlite_db):
    """Benchmark UPDATE operations on SQLite (20k rows)."""
    setup_test_table(sqlite_db, 20000, 'sqlite')
    _run_update_test(benchmark, sqlite_db, 20000, 'sqlite')


def test_update_20k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark UPDATE operations on nistmemsql (20k rows)."""
    setup_test_table(nistmemsql_db, 20000, 'nistmemsql')
    _run_update_test(benchmark, nistmemsql_db, 20000, 'nistmemsql')


def test_update_20k_duckdb(benchmark, duckdb_db):
    """Benchmark UPDATE operations on DuckDB (20k rows)."""
    setup_test_table(duckdb_db, 20000, 'duckdb')
    _run_update_test(benchmark, duckdb_db, 20000, 'duckdb')


def _run_update_test(benchmark, connection, num_rows, db_type):
    """Helper to run UPDATE benchmark for a given database and row count."""
    def run_updates():
        cursor = connection.cursor()
        for i in range(num_rows):
            if db_type in ['sqlite', 'duckdb']:
                cursor.execute(
                    "UPDATE test_table SET value = value + 1 WHERE id = ?",
                    (i,)
                )
            else:  # nistmemsql
                cursor.execute(
                    f"UPDATE test_table SET value = value + 1 WHERE id = {i}"
                )

        if db_type in ['sqlite', 'duckdb']:
            connection.commit()

    benchmark(run_updates)
