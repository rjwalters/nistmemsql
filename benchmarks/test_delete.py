"""
DELETE Benchmarks: Compare DELETE performance across databases at various scales.

Tests DELETE operations for SQLite, nistmemsql, and DuckDB at:
- 1k, 2k, 5k, 10k, and 20k rows
"""
import pytest
from utils.database_setup import setup_test_table


SIZES = [1000, 2000, 5000, 10000, 20000]
DATABASES = ['sqlite', 'nistmemsql', 'duckdb']


def test_delete_1k_sqlite(benchmark, sqlite_db):
    """Benchmark DELETE operations on sqlite (1k rows)."""
    setup_test_table(sqlite_db, 1000, 'sqlite')
    _run_delete_test(benchmark, sqlite_db, 1000, 'sqlite')

def test_delete_1k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark DELETE operations on nistmemsql (1k rows)."""
    setup_test_table(nistmemsql_db, 1000, 'nistmemsql')
    _run_delete_test(benchmark, nistmemsql_db, 1000, 'nistmemsql')

def test_delete_1k_duckdb(benchmark, duckdb_db):
    """Benchmark DELETE operations on duckdb (1k rows)."""
    setup_test_table(duckdb_db, 1000, 'duckdb')
    _run_delete_test(benchmark, duckdb_db, 1000, 'duckdb')

# def test_delete_2k_sqlite(benchmark, sqlite_db):
#     """Benchmark DELETE operations on sqlite (2k rows)."""
#     setup_test_table(sqlite_db, 2000, 'sqlite')
#     _run_delete_test(benchmark, sqlite_db, 2000, 'sqlite')

# def test_delete_2k_nistmemsql(benchmark, nistmemsql_db):
#     """Benchmark DELETE operations on nistmemsql (2k rows)."""
#     setup_test_table(nistmemsql_db, 2000, 'nistmemsql')
#     _run_delete_test(benchmark, nistmemsql_db, 2000, 'nistmemsql')

# def test_delete_2k_duckdb(benchmark, duckdb_db):
#     """Benchmark DELETE operations on duckdb (2k rows)."""
#     setup_test_table(duckdb_db, 2000, 'duckdb')
#     _run_delete_test(benchmark, duckdb_db, 2000, 'duckdb')

# def test_delete_5k_sqlite(benchmark, sqlite_db):
#     """Benchmark DELETE operations on sqlite (5k rows)."""
#     setup_test_table(sqlite_db, 5000, 'sqlite')
#     _run_delete_test(benchmark, sqlite_db, 5000, 'sqlite')

# def test_delete_5k_nistmemsql(benchmark, nistmemsql_db):
#     """Benchmark DELETE operations on nistmemsql (5k rows)."""
#     setup_test_table(nistmemsql_db, 5000, 'nistmemsql')
#     _run_delete_test(benchmark, nistmemsql_db, 5000, 'nistmemsql')

# def test_delete_5k_duckdb(benchmark, duckdb_db):
#     """Benchmark DELETE operations on duckdb (5k rows)."""
#     setup_test_table(duckdb_db, 5000, 'duckdb')
#     _run_delete_test(benchmark, duckdb_db, 5000, 'duckdb')

# def test_delete_10k_sqlite(benchmark, sqlite_db):
#     """Benchmark DELETE operations on sqlite (10k rows)."""
#     setup_test_table(sqlite_db, 10000, 'sqlite')
#     _run_delete_test(benchmark, sqlite_db, 10000, 'sqlite')

# def test_delete_10k_nistmemsql(benchmark, nistmemsql_db):
#     """Benchmark DELETE operations on nistmemsql (10k rows)."""
#     setup_test_table(nistmemsql_db, 10000, 'nistmemsql')
#     _run_delete_test(benchmark, nistmemsql_db, 10000, 'nistmemsql')

# def test_delete_10k_duckdb(benchmark, duckdb_db):
#     """Benchmark DELETE operations on duckdb (10k rows)."""
#     setup_test_table(duckdb_db, 10000, 'duckdb')
#     _run_delete_test(benchmark, duckdb_db, 10000, 'duckdb')

# def test_delete_20k_sqlite(benchmark, sqlite_db):
#     """Benchmark DELETE operations on sqlite (20k rows)."""
#     setup_test_table(sqlite_db, 20000, 'sqlite')
#     _run_delete_test(benchmark, sqlite_db, 20000, 'sqlite')

# def test_delete_20k_nistmemsql(benchmark, nistmemsql_db):
#     """Benchmark DELETE operations on nistmemsql (20k rows)."""
#     setup_test_table(nistmemsql_db, 20000, 'nistmemsql')
#     _run_delete_test(benchmark, nistmemsql_db, 20000, 'nistmemsql')

# def test_delete_20k_duckdb(benchmark, duckdb_db):
#     """Benchmark DELETE operations on duckdb (20k rows)."""
#     setup_test_table(duckdb_db, 20000, 'duckdb')
#     _run_delete_test(benchmark, duckdb_db, 20000, 'duckdb')


def _run_delete_test(benchmark, connection, num_rows, db_type):
    """Helper to run DELETE benchmark for a given database and row count."""
    def run_deletes():
        cursor = connection.cursor()
        for i in range(num_rows):
            if db_type in ['sqlite', 'duckdb']:
                cursor.execute(
                    "DELETE FROM test_table WHERE id = ?",
                    (i,)
                )
            else:  # nistmemsql
                cursor.execute(
                    f"DELETE FROM test_table WHERE id = {i}"
                )
        
        if db_type in ['sqlite', 'duckdb']:
            connection.commit()
    
    benchmark(run_deletes)
