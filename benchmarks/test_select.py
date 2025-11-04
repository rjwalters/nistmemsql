"""
SELECT Benchmarks: Compare SELECT WHERE performance across databases at various scales.

Tests SELECT with WHERE clause for SQLite, vibesql, and DuckDB at:
- 1k, 2k, 5k, 10k, and 20k rows (filtering 10% of rows)
"""
import pytest
from utils.database_setup import setup_test_table



def test_select_where_1k_sqlite(benchmark, sqlite_db):
    """Benchmark SELECT WHERE on sqlite (filter 10% of 1k rows)."""
    setup_test_table(sqlite_db, 1000, 'sqlite')
    _run_select_test(benchmark, sqlite_db, 100, 'sqlite')

def test_select_where_1k_vibesql(benchmark, vibesql_db):
    """Benchmark SELECT WHERE on vibesql (filter 10% of 1k rows)."""
    setup_test_table(vibesql_db, 1000, 'vibesql')
    _run_select_test(benchmark, vibesql_db, 100, 'vibesql')

def test_select_where_1k_duckdb(benchmark, duckdb_db):
    """Benchmark SELECT WHERE on duckdb (filter 10% of 1k rows)."""
    setup_test_table(duckdb_db, 1000, 'duckdb')
    _run_select_test(benchmark, duckdb_db, 100, 'duckdb')

# def test_select_where_2k_sqlite(benchmark, sqlite_db):
#     """Benchmark SELECT WHERE on sqlite (filter 10% of 2k rows)."""
#     setup_test_table(sqlite_db, 2000, 'sqlite')
#     _run_select_test(benchmark, sqlite_db, 200, 'sqlite')

# def test_select_where_2k_nistmemsql(benchmark, nistmemsql_db):
#     """Benchmark SELECT WHERE on nistmemsql (filter 10% of 2k rows)."""
#     setup_test_table(nistmemsql_db, 2000, 'nistmemsql')
#     _run_select_test(benchmark, nistmemsql_db, 200, 'nistmemsql')

# def test_select_where_2k_duckdb(benchmark, duckdb_db):
#     """Benchmark SELECT WHERE on duckdb (filter 10% of 2k rows)."""
#     setup_test_table(duckdb_db, 2000, 'duckdb')
#     _run_select_test(benchmark, duckdb_db, 200, 'duckdb')

# def test_select_where_5k_sqlite(benchmark, sqlite_db):
#     """Benchmark SELECT WHERE on sqlite (filter 10% of 5k rows)."""
#     setup_test_table(sqlite_db, 5000, 'sqlite')
#     _run_select_test(benchmark, sqlite_db, 500, 'sqlite')

# def test_select_where_5k_nistmemsql(benchmark, nistmemsql_db):
#     """Benchmark SELECT WHERE on nistmemsql (filter 10% of 5k rows)."""
#     setup_test_table(nistmemsql_db, 5000, 'nistmemsql')
#     _run_select_test(benchmark, nistmemsql_db, 500, 'nistmemsql')

# def test_select_where_5k_duckdb(benchmark, duckdb_db):
#     """Benchmark SELECT WHERE on duckdb (filter 10% of 5k rows)."""
#     setup_test_table(duckdb_db, 5000, 'duckdb')
#     _run_select_test(benchmark, duckdb_db, 500, 'duckdb')

# def test_select_where_10k_sqlite(benchmark, sqlite_db):
#     """Benchmark SELECT WHERE on sqlite (filter 10% of 10k rows)."""
#     setup_test_table(sqlite_db, 10000, 'sqlite')
#     _run_select_test(benchmark, sqlite_db, 1000, 'sqlite')

# def test_select_where_10k_nistmemsql(benchmark, nistmemsql_db):
#     """Benchmark SELECT WHERE on nistmemsql (filter 10% of 10k rows)."""
#     setup_test_table(nistmemsql_db, 10000, 'nistmemsql')
#     _run_select_test(benchmark, nistmemsql_db, 1000, 'nistmemsql')

# def test_select_where_10k_duckdb(benchmark, duckdb_db):
#     """Benchmark SELECT WHERE on duckdb (filter 10% of 10k rows)."""
#     setup_test_table(duckdb_db, 10000, 'duckdb')
#     _run_select_test(benchmark, duckdb_db, 1000, 'duckdb')

# def test_select_where_20k_sqlite(benchmark, sqlite_db):
#     """Benchmark SELECT WHERE on sqlite (filter 10% of 20k rows)."""
#     setup_test_table(sqlite_db, 20000, 'sqlite')
#     _run_select_test(benchmark, sqlite_db, 2000, 'sqlite')

# def test_select_where_20k_nistmemsql(benchmark, nistmemsql_db):
#     """Benchmark SELECT WHERE on nistmemsql (filter 10% of 20k rows)."""
#     setup_test_table(nistmemsql_db, 20000, 'nistmemsql')
#     _run_select_test(benchmark, nistmemsql_db, 2000, 'nistmemsql')

# def test_select_where_20k_duckdb(benchmark, duckdb_db):
#     """Benchmark SELECT WHERE on duckdb (filter 10% of 20k rows)."""
#     setup_test_table(duckdb_db, 20000, 'duckdb')
#     _run_select_test(benchmark, duckdb_db, 2000, 'duckdb')


def _run_select_test(benchmark, connection, filter_limit, db_type):
    """Helper to run SELECT WHERE benchmark."""
    def run_query():
        cursor = connection.cursor()
        cursor.execute(f"SELECT * FROM test_table WHERE id < {filter_limit}")
        rows = cursor.fetchall()
        assert len(rows) == filter_limit
    
    benchmark(run_query)
