"""
Aggregate Benchmarks: Compare COUNT/SUM/AVG performance across databases at various scales.

Tests aggregate operations for SQLite, vibesql, and DuckDB at:
- 1k, 2k, 5k, 10k, and 20k rows
"""
import pytest
from utils.database_setup import setup_test_table



def test_count_1k_sqlite(benchmark, sqlite_db):
    """Benchmark COUNT(*) on sqlite (1k rows)."""
    setup_test_table(sqlite_db, 1000, 'sqlite')
    _run_count_test(benchmark, sqlite_db, 1000)

def test_count_1k_vibesql(benchmark, vibesql_db):
    """Benchmark COUNT(*) on vibesql (1k rows)."""
    setup_test_table(vibesql_db, 1000, 'vibesql')
    _run_count_test(benchmark, vibesql_db, 1000)

def test_count_1k_duckdb(benchmark, duckdb_db):
    """Benchmark COUNT(*) on duckdb (1k rows)."""
    setup_test_table(duckdb_db, 1000, 'duckdb')
    _run_count_test(benchmark, duckdb_db, 1000)

# def test_count_2k_sqlite(benchmark, sqlite_db):
#     """Benchmark COUNT(*) on sqlite (2k rows)."""
#     setup_test_table(sqlite_db, 2000, 'sqlite')
#     _run_count_test(benchmark, sqlite_db, 2000)

# def test_count_2k_nistmemsql(benchmark, nistmemsql_db):
#     """Benchmark COUNT(*) on nistmemsql (2k rows)."""
#     setup_test_table(nistmemsql_db, 2000, 'nistmemsql')
#     _run_count_test(benchmark, nistmemsql_db, 2000)

# def test_count_2k_duckdb(benchmark, duckdb_db):
#     """Benchmark COUNT(*) on duckdb (2k rows)."""
#     setup_test_table(duckdb_db, 2000, 'duckdb')
#     _run_count_test(benchmark, duckdb_db, 2000)

# def test_count_5k_sqlite(benchmark, sqlite_db):
#     """Benchmark COUNT(*) on sqlite (5k rows)."""
#     setup_test_table(sqlite_db, 5000, 'sqlite')
#     _run_count_test(benchmark, sqlite_db, 5000)

# def test_count_5k_nistmemsql(benchmark, nistmemsql_db):
#     """Benchmark COUNT(*) on nistmemsql (5k rows)."""
#     setup_test_table(nistmemsql_db, 5000, 'nistmemsql')
#     _run_count_test(benchmark, nistmemsql_db, 5000)

# def test_count_5k_duckdb(benchmark, duckdb_db):
#     """Benchmark COUNT(*) on duckdb (5k rows)."""
#     setup_test_table(duckdb_db, 5000, 'duckdb')
#     _run_count_test(benchmark, duckdb_db, 5000)

# def test_count_10k_sqlite(benchmark, sqlite_db):
#     """Benchmark COUNT(*) on sqlite (10k rows)."""
#     setup_test_table(sqlite_db, 10000, 'sqlite')
#     _run_count_test(benchmark, sqlite_db, 10000)

# def test_count_10k_nistmemsql(benchmark, nistmemsql_db):
#     """Benchmark COUNT(*) on nistmemsql (10k rows)."""
#     setup_test_table(nistmemsql_db, 10000, 'nistmemsql')
#     _run_count_test(benchmark, nistmemsql_db, 10000)

# def test_count_10k_duckdb(benchmark, duckdb_db):
#     """Benchmark COUNT(*) on duckdb (10k rows)."""
#     setup_test_table(duckdb_db, 10000, 'duckdb')
#     _run_count_test(benchmark, duckdb_db, 10000)

# def test_count_20k_sqlite(benchmark, sqlite_db):
#     """Benchmark COUNT(*) on sqlite (20k rows)."""
#     setup_test_table(sqlite_db, 20000, 'sqlite')
#     _run_count_test(benchmark, sqlite_db, 20000)

# def test_count_20k_nistmemsql(benchmark, nistmemsql_db):
#     """Benchmark COUNT(*) on nistmemsql (20k rows)."""
#     setup_test_table(nistmemsql_db, 20000, 'nistmemsql')
#     _run_count_test(benchmark, nistmemsql_db, 20000)

# def test_count_20k_duckdb(benchmark, duckdb_db):
#     """Benchmark COUNT(*) on duckdb (20k rows)."""
#     setup_test_table(duckdb_db, 20000, 'duckdb')
#     _run_count_test(benchmark, duckdb_db, 20000)

def test_sum_1k_sqlite(benchmark, sqlite_db):
    """Benchmark SUM(value) on sqlite (1k rows)."""
    setup_test_table(sqlite_db, 1000, 'sqlite')
    _run_sum_test(benchmark, sqlite_db, 1000)

def test_sum_1k_vibesql(benchmark, vibesql_db):
    """Benchmark SUM(value) on vibesql (1k rows)."""
    setup_test_table(vibesql_db, 1000, 'vibesql')
    _run_sum_test(benchmark, vibesql_db, 1000)

def test_sum_1k_duckdb(benchmark, duckdb_db):
    """Benchmark SUM(value) on duckdb (1k rows)."""
    setup_test_table(duckdb_db, 1000, 'duckdb')
    _run_sum_test(benchmark, duckdb_db, 1000)

# def test_sum_2k_sqlite(benchmark, sqlite_db):
#     """Benchmark SUM(value) on sqlite (2k rows)."""
#     setup_test_table(sqlite_db, 2000, 'sqlite')
#     _run_sum_test(benchmark, sqlite_db, 2000)

# def test_sum_2k_nistmemsql(benchmark, nistmemsql_db):
#     """Benchmark SUM(value) on nistmemsql (2k rows)."""
#     setup_test_table(nistmemsql_db, 2000, 'nistmemsql')
#     _run_sum_test(benchmark, nistmemsql_db, 2000)

# def test_sum_2k_duckdb(benchmark, duckdb_db):
#     """Benchmark SUM(value) on duckdb (2k rows)."""
#     setup_test_table(duckdb_db, 2000, 'duckdb')
#     _run_sum_test(benchmark, duckdb_db, 2000)

# def test_sum_5k_sqlite(benchmark, sqlite_db):
#     """Benchmark SUM(value) on sqlite (5k rows)."""
#     setup_test_table(sqlite_db, 5000, 'sqlite')
#     _run_sum_test(benchmark, sqlite_db, 5000)

# def test_sum_5k_nistmemsql(benchmark, nistmemsql_db):
#     """Benchmark SUM(value) on nistmemsql (5k rows)."""
#     setup_test_table(nistmemsql_db, 5000, 'nistmemsql')
#     _run_sum_test(benchmark, nistmemsql_db, 5000)

# def test_sum_5k_duckdb(benchmark, duckdb_db):
#     """Benchmark SUM(value) on duckdb (5k rows)."""
#     setup_test_table(duckdb_db, 5000, 'duckdb')
#     _run_sum_test(benchmark, duckdb_db, 5000)

# def test_sum_10k_sqlite(benchmark, sqlite_db):
#     """Benchmark SUM(value) on sqlite (10k rows)."""
#     setup_test_table(sqlite_db, 10000, 'sqlite')
#     _run_sum_test(benchmark, sqlite_db, 10000)

# def test_sum_10k_nistmemsql(benchmark, nistmemsql_db):
#     """Benchmark SUM(value) on nistmemsql (10k rows)."""
#     setup_test_table(nistmemsql_db, 10000, 'nistmemsql')
#     _run_sum_test(benchmark, nistmemsql_db, 10000)

# def test_sum_10k_duckdb(benchmark, duckdb_db):
#     """Benchmark SUM(value) on duckdb (10k rows)."""
#     setup_test_table(duckdb_db, 10000, 'duckdb')
#     _run_sum_test(benchmark, duckdb_db, 10000)

# def test_sum_20k_sqlite(benchmark, sqlite_db):
#     """Benchmark SUM(value) on sqlite (20k rows)."""
#     setup_test_table(sqlite_db, 20000, 'sqlite')
#     _run_sum_test(benchmark, sqlite_db, 20000)

# def test_sum_20k_nistmemsql(benchmark, nistmemsql_db):
#     """Benchmark SUM(value) on nistmemsql (20k rows)."""
#     setup_test_table(nistmemsql_db, 20000, 'nistmemsql')
#     _run_sum_test(benchmark, nistmemsql_db, 20000)

# def test_sum_20k_duckdb(benchmark, duckdb_db):
#     """Benchmark SUM(value) on duckdb (20k rows)."""
#     setup_test_table(duckdb_db, 20000, 'duckdb')
#     _run_sum_test(benchmark, duckdb_db, 20000)

def test_avg_1k_sqlite(benchmark, sqlite_db):
    """Benchmark AVG(value) on sqlite (1k rows)."""
    setup_test_table(sqlite_db, 1000, 'sqlite')
    _run_avg_test(benchmark, sqlite_db, 1000)

def test_avg_1k_vibesql(benchmark, vibesql_db):
    """Benchmark AVG(value) on vibesql (1k rows)."""
    setup_test_table(vibesql_db, 1000, 'vibesql')
    _run_avg_test(benchmark, vibesql_db, 1000)

def test_avg_1k_duckdb(benchmark, duckdb_db):
    """Benchmark AVG(value) on duckdb (1k rows)."""
    setup_test_table(duckdb_db, 1000, 'duckdb')
    _run_avg_test(benchmark, duckdb_db, 1000)

# def test_avg_2k_sqlite(benchmark, sqlite_db):
#     """Benchmark AVG(value) on sqlite (2k rows)."""
#     setup_test_table(sqlite_db, 2000, 'sqlite')
#     _run_avg_test(benchmark, sqlite_db, 2000)

# def test_avg_2k_nistmemsql(benchmark, nistmemsql_db):
#     """Benchmark AVG(value) on nistmemsql (2k rows)."""
#     setup_test_table(nistmemsql_db, 2000, 'nistmemsql')
#     _run_avg_test(benchmark, nistmemsql_db, 2000)

# def test_avg_2k_duckdb(benchmark, duckdb_db):
#     """Benchmark AVG(value) on duckdb (2k rows)."""
#     setup_test_table(duckdb_db, 2000, 'duckdb')
#     _run_avg_test(benchmark, duckdb_db, 2000)

# def test_avg_5k_sqlite(benchmark, sqlite_db):
#     """Benchmark AVG(value) on sqlite (5k rows)."""
#     setup_test_table(sqlite_db, 5000, 'sqlite')
#     _run_avg_test(benchmark, sqlite_db, 5000)

# def test_avg_5k_nistmemsql(benchmark, nistmemsql_db):
#     """Benchmark AVG(value) on nistmemsql (5k rows)."""
#     setup_test_table(nistmemsql_db, 5000, 'nistmemsql')
#     _run_avg_test(benchmark, nistmemsql_db, 5000)

# def test_avg_5k_duckdb(benchmark, duckdb_db):
#     """Benchmark AVG(value) on duckdb (5k rows)."""
#     setup_test_table(duckdb_db, 5000, 'duckdb')
#     _run_avg_test(benchmark, duckdb_db, 5000)

# def test_avg_10k_sqlite(benchmark, sqlite_db):
#     """Benchmark AVG(value) on sqlite (10k rows)."""
#     setup_test_table(sqlite_db, 10000, 'sqlite')
#     _run_avg_test(benchmark, sqlite_db, 10000)

# def test_avg_10k_nistmemsql(benchmark, nistmemsql_db):
#     """Benchmark AVG(value) on nistmemsql (10k rows)."""
#     setup_test_table(nistmemsql_db, 10000, 'nistmemsql')
#     _run_avg_test(benchmark, nistmemsql_db, 10000)

# def test_avg_10k_duckdb(benchmark, duckdb_db):
#     """Benchmark AVG(value) on duckdb (10k rows)."""
#     setup_test_table(duckdb_db, 10000, 'duckdb')
#     _run_avg_test(benchmark, duckdb_db, 10000)

# def test_avg_20k_sqlite(benchmark, sqlite_db):
#     """Benchmark AVG(value) on sqlite (20k rows)."""
#     setup_test_table(sqlite_db, 20000, 'sqlite')
#     _run_avg_test(benchmark, sqlite_db, 20000)

# def test_avg_20k_nistmemsql(benchmark, nistmemsql_db):
#     """Benchmark AVG(value) on nistmemsql (20k rows)."""
#     setup_test_table(nistmemsql_db, 20000, 'nistmemsql')
#     _run_avg_test(benchmark, nistmemsql_db, 20000)

# def test_avg_20k_duckdb(benchmark, duckdb_db):
#     """Benchmark AVG(value) on duckdb (20k rows)."""
#     setup_test_table(duckdb_db, 20000, 'duckdb')
#     _run_avg_test(benchmark, duckdb_db, 20000)


def _run_count_test(benchmark, connection, expected_count):
    """Helper to run COUNT benchmark."""
    def run_query():
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM test_table")
        result = cursor.fetchone()[0]
        assert result == expected_count
    
    benchmark(run_query)


def _run_sum_test(benchmark, connection, num_rows):
    """Helper to run SUM benchmark."""
    def run_query():
        cursor = connection.cursor()
        cursor.execute("SELECT SUM(value) FROM test_table")
        result = cursor.fetchone()[0]
        assert result is not None
    
    benchmark(run_query)


def _run_avg_test(benchmark, connection, num_rows):
    """Helper to run AVG benchmark."""
    def run_query():
        cursor = connection.cursor()
        cursor.execute("SELECT AVG(value) FROM test_table")
        result = cursor.fetchone()[0]
        assert result is not None
    
    benchmark(run_query)
