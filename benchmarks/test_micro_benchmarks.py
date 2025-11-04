"""
Tier 1 Micro-Benchmarks: Basic operations comparison between vibesql, SQLite, and DuckDB.

These benchmarks establish fundamental performance characteristics for single-table operations.
Following the pattern from #650, expanded to cover INSERT, UPDATE, DELETE, WHERE, and aggregates.
Now includes DuckDB as an industry-standard baseline for in-memory analytical databases.

Refactored to use helper modules for reduced duplication and improved maintainability.
"""
import pytest

from .backend_setup import create_test_table, insert_test_data, setup_database_for_benchmark
from .benchmark_scenarios import (
    run_insert_scenario, run_update_scenario, run_delete_scenario,
    run_select_where_scenario, run_aggregate_scenario
)
from .assertion_helpers import assert_row_count, assert_aggregate_result


# ============================================================================
# INSERT Benchmarks
# ============================================================================

def test_insert_1k_sqlite(benchmark, sqlite_db):
    """Benchmark INSERT operations on SQLite (1K rows)."""
    cursor = sqlite_db.cursor()
    create_test_table(cursor, 'sqlite')
    next_id = [0]

    def run_inserts():
        cursor = sqlite_db.cursor()
        run_insert_scenario(cursor, next_id[0], 1000, 'sqlite')
        next_id[0] += 1000
        sqlite_db.commit()

    benchmark(run_inserts)


def test_insert_1k_vibesql(benchmark, vibesql_db):
    """Benchmark INSERT operations on vibesql (1K rows)."""
    cursor = vibesql_db.cursor()
    create_test_table(cursor, 'vibesql')
    next_id = [0]

    def run_inserts():
        cursor = vibesql_db.cursor()
        run_insert_scenario(cursor, next_id[0], 1000, 'vibesql')
        next_id[0] += 1000

    benchmark(run_inserts)


def test_insert_1k_duckdb(benchmark, duckdb_db):
    """Benchmark INSERT operations on DuckDB (1K rows)."""
    cursor = duckdb_db.cursor()
    create_test_table(cursor, 'duckdb')
    next_id = [0]

    def run_inserts():
        cursor = duckdb_db.cursor()
        run_insert_scenario(cursor, next_id[0], 1000, 'duckdb')
        next_id[0] += 1000
        duckdb_db.commit()

    benchmark(run_inserts)


def test_insert_10k_sqlite(benchmark, sqlite_db):
    """Benchmark INSERT operations on SQLite (10K rows)."""
    cursor = sqlite_db.cursor()
    create_test_table(cursor, 'sqlite')
    next_id = [0]

    def run_inserts():
        cursor = sqlite_db.cursor()
        run_insert_scenario(cursor, next_id[0], 10000, 'sqlite')
        next_id[0] += 10000
        sqlite_db.commit()

    benchmark(run_inserts)


def test_insert_10k_vibesql(benchmark, vibesql_db):
    """Benchmark INSERT operations on vibesql (10K rows)."""
    cursor = vibesql_db.cursor()
    create_test_table(cursor, 'vibesql')
    next_id = [0]

    def run_inserts():
        cursor = vibesql_db.cursor()
        run_insert_scenario(cursor, next_id[0], 10000, 'vibesql')
        next_id[0] += 10000

    benchmark(run_inserts)


def test_insert_10k_duckdb(benchmark, duckdb_db):
    """Benchmark INSERT operations on DuckDB (10K rows)."""
    cursor = duckdb_db.cursor()
    create_test_table(cursor, 'duckdb')
    next_id = [0]

    def run_inserts():
        cursor = duckdb_db.cursor()
        run_insert_scenario(cursor, next_id[0], 10000, 'duckdb')
        next_id[0] += 10000
        duckdb_db.commit()

    benchmark(run_inserts)


def test_insert_100k_sqlite(benchmark, sqlite_db):
    """Benchmark INSERT operations on SQLite (100K rows)."""
    cursor = sqlite_db.cursor()
    create_test_table(cursor, 'sqlite')
    next_id = [0]

    def run_inserts():
        cursor = sqlite_db.cursor()
        run_insert_scenario(cursor, next_id[0], 100000, 'sqlite')
        next_id[0] += 100000
        sqlite_db.commit()

    benchmark(run_inserts)


def test_insert_100k_vibesql(benchmark, vibesql_db):
    """Benchmark INSERT operations on vibesql (100K rows)."""
    cursor = vibesql_db.cursor()
    create_test_table(cursor, 'vibesql')
    next_id = [0]

    def run_inserts():
        cursor = vibesql_db.cursor()
        run_insert_scenario(cursor, next_id[0], 100000, 'vibesql')
        next_id[0] += 100000

    benchmark(run_inserts)


def test_insert_100k_duckdb(benchmark, duckdb_db):
    """Benchmark INSERT operations on DuckDB (100K rows)."""
    cursor = duckdb_db.cursor()
    create_test_table(cursor, 'duckdb')
    next_id = [0]

    def run_inserts():
        cursor = duckdb_db.cursor()
        run_insert_scenario(cursor, next_id[0], 100000, 'duckdb')
        next_id[0] += 100000
        duckdb_db.commit()

    benchmark(run_inserts)


# ============================================================================
# UPDATE Benchmarks
# ============================================================================

def test_update_1k_sqlite(benchmark, sqlite_db):
    """Benchmark UPDATE operations on SQLite (1K rows)."""
    setup_database_for_benchmark(sqlite_db, 'sqlite', 1000)

    def run_updates():
        cursor = sqlite_db.cursor()
        run_update_scenario(cursor, 1000, 'sqlite')
        sqlite_db.commit()

    benchmark(run_updates)


def test_update_1k_vibesql(benchmark, vibesql_db):
    """Benchmark UPDATE operations on vibesql (1K rows)."""
    setup_database_for_benchmark(vibesql_db, 'vibesql', 1000)

    def run_updates():
        cursor = vibesql_db.cursor()
        run_update_scenario(cursor, 1000, 'vibesql')

    benchmark(run_updates)


def test_update_1k_duckdb(benchmark, duckdb_db):
    """Benchmark UPDATE operations on DuckDB (1K rows)."""
    setup_database_for_benchmark(duckdb_db, 'duckdb', 1000)

    def run_updates():
        cursor = duckdb_db.cursor()
        run_update_scenario(cursor, 1000, 'duckdb')
        duckdb_db.commit()

    benchmark(run_updates)


def test_update_10k_sqlite(benchmark, sqlite_db):
    """Benchmark UPDATE operations on SQLite (10K rows)."""
    setup_database_for_benchmark(sqlite_db, 'sqlite', 10000)

    def run_updates():
        cursor = sqlite_db.cursor()
        run_update_scenario(cursor, 10000, 'sqlite')
        sqlite_db.commit()

    benchmark(run_updates)


def test_update_10k_vibesql(benchmark, vibesql_db):
    """Benchmark UPDATE operations on vibesql (10K rows)."""
    setup_database_for_benchmark(vibesql_db, 'vibesql', 10000)

    def run_updates():
        cursor = vibesql_db.cursor()
        run_update_scenario(cursor, 10000, 'vibesql')

    benchmark(run_updates)


def test_update_10k_duckdb(benchmark, duckdb_db):
    """Benchmark UPDATE operations on DuckDB (10K rows)."""
    setup_database_for_benchmark(duckdb_db, 'duckdb', 10000)

    def run_updates():
        cursor = duckdb_db.cursor()
        run_update_scenario(cursor, 10000, 'duckdb')
        duckdb_db.commit()

    benchmark(run_updates)


def test_update_100k_sqlite(benchmark, sqlite_db):
    """Benchmark UPDATE operations on SQLite (100K rows)."""
    setup_database_for_benchmark(sqlite_db, 'sqlite', 100000)

    def run_updates():
        cursor = sqlite_db.cursor()
        run_update_scenario(cursor, 100000, 'sqlite')
        sqlite_db.commit()

    benchmark(run_updates)


def test_update_100k_vibesql(benchmark, vibesql_db):
    """Benchmark UPDATE operations on vibesql (100K rows)."""
    setup_database_for_benchmark(vibesql_db, 'vibesql', 100000)

    def run_updates():
        cursor = vibesql_db.cursor()
        run_update_scenario(cursor, 100000, 'vibesql')

    benchmark(run_updates)


def test_update_100k_duckdb(benchmark, duckdb_db):
    """Benchmark UPDATE operations on DuckDB (100K rows)."""
    setup_database_for_benchmark(duckdb_db, 'duckdb', 100000)

    def run_updates():
        cursor = duckdb_db.cursor()
        run_update_scenario(cursor, 100000, 'duckdb')
        duckdb_db.commit()

    benchmark(run_updates)


# ============================================================================
# DELETE Benchmarks
# ============================================================================

def test_delete_1k_sqlite(benchmark, sqlite_db):
    """Benchmark DELETE operations on SQLite (1K rows)."""
    setup_database_for_benchmark(sqlite_db, 'sqlite', 1000)

    def run_deletes():
        cursor = sqlite_db.cursor()
        run_delete_scenario(cursor, 1000, 'sqlite')
        sqlite_db.commit()

    benchmark(run_deletes)


def test_delete_1k_vibesql(benchmark, vibesql_db):
    """Benchmark DELETE operations on vibesql (1K rows)."""
    setup_database_for_benchmark(vibesql_db, 'vibesql', 1000)

    def run_deletes():
        cursor = vibesql_db.cursor()
        run_delete_scenario(cursor, 1000, 'vibesql')

    benchmark(run_deletes)


def test_delete_1k_duckdb(benchmark, duckdb_db):
    """Benchmark DELETE operations on DuckDB (1K rows)."""
    setup_database_for_benchmark(duckdb_db, 'duckdb', 1000)

    def run_deletes():
        cursor = duckdb_db.cursor()
        run_delete_scenario(cursor, 1000, 'duckdb')
        duckdb_db.commit()

    benchmark(run_deletes)


def test_delete_10k_sqlite(benchmark, sqlite_db):
    """Benchmark DELETE operations on SQLite (10K rows)."""
    setup_database_for_benchmark(sqlite_db, 'sqlite', 10000)

    def run_deletes():
        cursor = sqlite_db.cursor()
        run_delete_scenario(cursor, 10000, 'sqlite')
        sqlite_db.commit()

    benchmark(run_deletes)


def test_delete_10k_vibesql(benchmark, vibesql_db):
    """Benchmark DELETE operations on vibesql (10K rows)."""
    setup_database_for_benchmark(vibesql_db, 'vibesql', 10000)

    def run_deletes():
        cursor = vibesql_db.cursor()
        run_delete_scenario(cursor, 10000, 'vibesql')

    benchmark(run_deletes)


def test_delete_10k_duckdb(benchmark, duckdb_db):
    """Benchmark DELETE operations on DuckDB (10K rows)."""
    setup_database_for_benchmark(duckdb_db, 'duckdb', 10000)

    def run_deletes():
        cursor = duckdb_db.cursor()
        run_delete_scenario(cursor, 10000, 'duckdb')
        duckdb_db.commit()

    benchmark(run_deletes)


def test_delete_100k_sqlite(benchmark, sqlite_db):
    """Benchmark DELETE operations on SQLite (100K rows)."""
    setup_database_for_benchmark(sqlite_db, 'sqlite', 100000)

    def run_deletes():
        cursor = sqlite_db.cursor()
        run_delete_scenario(cursor, 100000, 'sqlite')
        sqlite_db.commit()

    benchmark(run_deletes)


def test_delete_100k_vibesql(benchmark, vibesql_db):
    """Benchmark DELETE operations on vibesql (100K rows)."""
    setup_database_for_benchmark(vibesql_db, 'vibesql', 100000)

    def run_deletes():
        cursor = vibesql_db.cursor()
        run_delete_scenario(cursor, 100000, 'vibesql')

    benchmark(run_deletes)


def test_delete_100k_duckdb(benchmark, duckdb_db):
    """Benchmark DELETE operations on DuckDB (100K rows)."""
    setup_database_for_benchmark(duckdb_db, 'duckdb', 100000)

    def run_deletes():
        cursor = duckdb_db.cursor()
        run_delete_scenario(cursor, 100000, 'duckdb')
        duckdb_db.commit()

    benchmark(run_deletes)


# ============================================================================
# SELECT with WHERE Clause Benchmarks
# ============================================================================

def test_select_where_1k_sqlite(benchmark, sqlite_db):
    """Benchmark SELECT with WHERE clause on SQLite (filter 10% of 1K rows)."""
    setup_database_for_benchmark(sqlite_db, 'sqlite', 1000)

    def run_query():
        cursor = sqlite_db.cursor()
        rows = run_select_where_scenario(cursor, 100, 'sqlite')
        assert_row_count(rows, 100)

    benchmark(run_query)


def test_select_where_1k_vibesql(benchmark, vibesql_db):
    """Benchmark SELECT with WHERE clause on vibesql (filter 10% of 1K rows)."""
    setup_database_for_benchmark(vibesql_db, 'vibesql', 1000)

    def run_query():
        cursor = vibesql_db.cursor()
        rows = run_select_where_scenario(cursor, 100, 'vibesql')
        assert_row_count(rows, 100)

    benchmark(run_query)


def test_select_where_1k_duckdb(benchmark, duckdb_db):
    """Benchmark SELECT with WHERE clause on DuckDB (filter 10% of 1K rows)."""
    setup_database_for_benchmark(duckdb_db, 'duckdb', 1000)

    def run_query():
        cursor = duckdb_db.cursor()
        rows = run_select_where_scenario(cursor, 100, 'duckdb')
        assert_row_count(rows, 100)

    benchmark(run_query)


def test_select_where_10k_sqlite(benchmark, sqlite_db):
    """Benchmark SELECT with WHERE clause on SQLite (filter 10% of 10K rows)."""
    setup_database_for_benchmark(sqlite_db, 'sqlite', 10000)

    def run_query():
        cursor = sqlite_db.cursor()
        rows = run_select_where_scenario(cursor, 1000, 'sqlite')
        assert_row_count(rows, 1000)

    benchmark(run_query)


def test_select_where_10k_vibesql(benchmark, vibesql_db):
    """Benchmark SELECT with WHERE clause on vibesql (filter 10% of 10K rows)."""
    setup_database_for_benchmark(vibesql_db, 'vibesql', 10000)

    def run_query():
        cursor = vibesql_db.cursor()
        rows = run_select_where_scenario(cursor, 1000, 'vibesql')
        assert_row_count(rows, 1000)

    benchmark(run_query)


def test_select_where_10k_duckdb(benchmark, duckdb_db):
    """Benchmark SELECT with WHERE clause on DuckDB (filter 10% of 10K rows)."""
    setup_database_for_benchmark(duckdb_db, 'duckdb', 10000)

    def run_query():
        cursor = duckdb_db.cursor()
        rows = run_select_where_scenario(cursor, 1000, 'duckdb')
        assert_row_count(rows, 1000)

    benchmark(run_query)


def test_select_where_100k_sqlite(benchmark, sqlite_db):
    """Benchmark SELECT with WHERE clause on SQLite (filter 10% of 100K rows)."""
    setup_database_for_benchmark(sqlite_db, 'sqlite', 100000)

    def run_query():
        cursor = sqlite_db.cursor()
        rows = run_select_where_scenario(cursor, 10000, 'sqlite')
        assert_row_count(rows, 10000)

    benchmark(run_query)


def test_select_where_100k_vibesql(benchmark, vibesql_db):
    """Benchmark SELECT with WHERE clause on vibesql (filter 10% of 100K rows)."""
    setup_database_for_benchmark(vibesql_db, 'vibesql', 100000)

    def run_query():
        cursor = vibesql_db.cursor()
        rows = run_select_where_scenario(cursor, 10000, 'vibesql')
        assert_row_count(rows, 10000)

    benchmark(run_query)


def test_select_where_100k_duckdb(benchmark, duckdb_db):
    """Benchmark SELECT with WHERE clause on DuckDB (filter 10% of 100K rows)."""
    setup_database_for_benchmark(duckdb_db, 'duckdb', 100000)

    def run_query():
        cursor = duckdb_db.cursor()
        rows = run_select_where_scenario(cursor, 10000, 'duckdb')
        assert_row_count(rows, 10000)

    benchmark(run_query)


# ============================================================================
# Aggregate Operation Benchmarks (COUNT, SUM, AVG)
# ============================================================================

def test_count_1k_sqlite(benchmark, sqlite_db):
    """Benchmark COUNT(*) aggregation on SQLite (1K rows)."""
    setup_database_for_benchmark(sqlite_db, 'sqlite', 1000)

    def run_query():
        cursor = sqlite_db.cursor()
        result = run_aggregate_scenario(cursor, 'COUNT', 'sqlite')
        assert_aggregate_result(result, 'COUNT', 1000)

    benchmark(run_query)


def test_count_1k_vibesql(benchmark, vibesql_db):
    """Benchmark COUNT(*) aggregation on vibesql (1K rows)."""
    setup_database_for_benchmark(vibesql_db, 'vibesql', 1000)

    def run_query():
        cursor = vibesql_db.cursor()
        result = run_aggregate_scenario(cursor, 'COUNT', 'vibesql')
        assert_aggregate_result(result, 'COUNT', 1000)

    benchmark(run_query)


def test_count_1k_duckdb(benchmark, duckdb_db):
    """Benchmark COUNT(*) aggregation on DuckDB (1K rows)."""
    setup_database_for_benchmark(duckdb_db, 'duckdb', 1000)

    def run_query():
        cursor = duckdb_db.cursor()
        result = run_aggregate_scenario(cursor, 'COUNT', 'duckdb')
        assert_aggregate_result(result, 'COUNT', 1000)

    benchmark(run_query)


def test_count_10k_sqlite(benchmark, sqlite_db):
    """Benchmark COUNT(*) aggregation on SQLite (10K rows)."""
    setup_database_for_benchmark(sqlite_db, 'sqlite', 10000)

    def run_query():
        cursor = sqlite_db.cursor()
        result = run_aggregate_scenario(cursor, 'COUNT', 'sqlite')
        assert_aggregate_result(result, 'COUNT', 10000)

    benchmark(run_query)


def test_count_10k_vibesql(benchmark, vibesql_db):
    """Benchmark COUNT(*) aggregation on vibesql (10K rows)."""
    setup_database_for_benchmark(vibesql_db, 'vibesql', 10000)

    def run_query():
        cursor = vibesql_db.cursor()
        result = run_aggregate_scenario(cursor, 'COUNT', 'vibesql')
        assert_aggregate_result(result, 'COUNT', 10000)

    benchmark(run_query)


def test_count_10k_duckdb(benchmark, duckdb_db):
    """Benchmark COUNT(*) aggregation on DuckDB (10K rows)."""
    setup_database_for_benchmark(duckdb_db, 'duckdb', 10000)

    def run_query():
        cursor = duckdb_db.cursor()
        result = run_aggregate_scenario(cursor, 'COUNT', 'duckdb')
        assert_aggregate_result(result, 'COUNT', 10000)

    benchmark(run_query)


def test_count_100k_sqlite(benchmark, sqlite_db):
    """Benchmark COUNT(*) aggregation on SQLite (100K rows)."""
    setup_database_for_benchmark(sqlite_db, 'sqlite', 100000)

    def run_query():
        cursor = sqlite_db.cursor()
        result = run_aggregate_scenario(cursor, 'COUNT', 'sqlite')
        assert_aggregate_result(result, 'COUNT', 100000)

    benchmark(run_query)


def test_count_100k_vibesql(benchmark, vibesql_db):
    """Benchmark COUNT(*) aggregation on vibesql (100K rows)."""
    setup_database_for_benchmark(vibesql_db, 'vibesql', 100000)

    def run_query():
        cursor = vibesql_db.cursor()
        result = run_aggregate_scenario(cursor, 'COUNT', 'vibesql')
        assert_aggregate_result(result, 'COUNT', 100000)

    benchmark(run_query)


def test_count_100k_duckdb(benchmark, duckdb_db):
    """Benchmark COUNT(*) aggregation on DuckDB (100K rows)."""
    setup_database_for_benchmark(duckdb_db, 'duckdb', 100000)

    def run_query():
        cursor = duckdb_db.cursor()
        result = run_aggregate_scenario(cursor, 'COUNT', 'duckdb')
        assert_aggregate_result(result, 'COUNT', 100000)

    benchmark(run_query)


def test_sum_1k_sqlite(benchmark, sqlite_db):
    """Benchmark SUM(value) aggregation on SQLite (1K rows)."""
    setup_database_for_benchmark(sqlite_db, 'sqlite', 1000)

    def run_query():
        cursor = sqlite_db.cursor()
        result = run_aggregate_scenario(cursor, 'SUM', 'sqlite')
        assert_aggregate_result(result, 'SUM')

    benchmark(run_query)


def test_sum_1k_vibesql(benchmark, vibesql_db):
    """Benchmark SUM(value) aggregation on vibesql (1K rows)."""
    setup_database_for_benchmark(vibesql_db, 'vibesql', 1000)

    def run_query():
        cursor = vibesql_db.cursor()
        result = run_aggregate_scenario(cursor, 'SUM', 'vibesql')
        assert_aggregate_result(result, 'SUM')

    benchmark(run_query)


def test_sum_1k_duckdb(benchmark, duckdb_db):
    """Benchmark SUM(value) aggregation on DuckDB (1K rows)."""
    setup_database_for_benchmark(duckdb_db, 'duckdb', 1000)

    def run_query():
        cursor = duckdb_db.cursor()
        result = run_aggregate_scenario(cursor, 'SUM', 'duckdb')
        assert_aggregate_result(result, 'SUM')

    benchmark(run_query)


def test_sum_10k_sqlite(benchmark, sqlite_db):
    """Benchmark SUM(value) aggregation on SQLite (10K rows)."""
    setup_database_for_benchmark(sqlite_db, 'sqlite', 10000)

    def run_query():
        cursor = sqlite_db.cursor()
        result = run_aggregate_scenario(cursor, 'SUM', 'sqlite')
        assert_aggregate_result(result, 'SUM')

    benchmark(run_query)


def test_sum_10k_vibesql(benchmark, vibesql_db):
    """Benchmark SUM(value) aggregation on vibesql (10K rows)."""
    setup_database_for_benchmark(vibesql_db, 'vibesql', 10000)

    def run_query():
        cursor = vibesql_db.cursor()
        result = run_aggregate_scenario(cursor, 'SUM', 'vibesql')
        assert_aggregate_result(result, 'SUM')

    benchmark(run_query)


def test_sum_10k_duckdb(benchmark, duckdb_db):
    """Benchmark SUM(value) aggregation on DuckDB (10K rows)."""
    setup_database_for_benchmark(duckdb_db, 'duckdb', 10000)

    def run_query():
        cursor = duckdb_db.cursor()
        result = run_aggregate_scenario(cursor, 'SUM', 'duckdb')
        assert_aggregate_result(result, 'SUM')

    benchmark(run_query)


def test_sum_100k_sqlite(benchmark, sqlite_db):
    """Benchmark SUM(value) aggregation on SQLite (100K rows)."""
    setup_database_for_benchmark(sqlite_db, 'sqlite', 100000)

    def run_query():
        cursor = sqlite_db.cursor()
        result = run_aggregate_scenario(cursor, 'SUM', 'sqlite')
        assert_aggregate_result(result, 'SUM')

    benchmark(run_query)


def test_sum_100k_vibesql(benchmark, vibesql_db):
    """Benchmark SUM(value) aggregation on vibesql (100K rows)."""
    setup_database_for_benchmark(vibesql_db, 'vibesql', 100000)

    def run_query():
        cursor = vibesql_db.cursor()
        result = run_aggregate_scenario(cursor, 'SUM', 'vibesql')
        assert_aggregate_result(result, 'SUM')

    benchmark(run_query)


def test_sum_100k_duckdb(benchmark, duckdb_db):
    """Benchmark SUM(value) aggregation on DuckDB (100K rows)."""
    setup_database_for_benchmark(duckdb_db, 'duckdb', 100000)

    def run_query():
        cursor = duckdb_db.cursor()
        result = run_aggregate_scenario(cursor, 'SUM', 'duckdb')
        assert_aggregate_result(result, 'SUM')

    benchmark(run_query)


def test_avg_1k_sqlite(benchmark, sqlite_db):
    """Benchmark AVG(value) aggregation on SQLite (1K rows)."""
    setup_database_for_benchmark(sqlite_db, 'sqlite', 1000)

    def run_query():
        cursor = sqlite_db.cursor()
        result = run_aggregate_scenario(cursor, 'AVG', 'sqlite')
        assert_aggregate_result(result, 'AVG')

    benchmark(run_query)


def test_avg_1k_vibesql(benchmark, vibesql_db):
    """Benchmark AVG(value) aggregation on vibesql (1K rows)."""
    setup_database_for_benchmark(vibesql_db, 'vibesql', 1000)

    def run_query():
        cursor = vibesql_db.cursor()
        result = run_aggregate_scenario(cursor, 'AVG', 'vibesql')
        assert_aggregate_result(result, 'AVG')

    benchmark(run_query)


def test_avg_1k_duckdb(benchmark, duckdb_db):
    """Benchmark AVG(value) aggregation on DuckDB (1K rows)."""
    setup_database_for_benchmark(duckdb_db, 'duckdb', 1000)

    def run_query():
        cursor = duckdb_db.cursor()
        result = run_aggregate_scenario(cursor, 'AVG', 'duckdb')
        assert_aggregate_result(result, 'AVG')

    benchmark(run_query)


def test_avg_10k_sqlite(benchmark, sqlite_db):
    """Benchmark AVG(value) aggregation on SQLite (10K rows)."""
    setup_database_for_benchmark(sqlite_db, 'sqlite', 10000)

    def run_query():
        cursor = sqlite_db.cursor()
        result = run_aggregate_scenario(cursor, 'AVG', 'sqlite')
        assert_aggregate_result(result, 'AVG')

    benchmark(run_query)


def test_avg_10k_vibesql(benchmark, vibesql_db):
    """Benchmark AVG(value) aggregation on vibesql (10K rows)."""
    setup_database_for_benchmark(vibesql_db, 'vibesql', 10000)

    def run_query():
        cursor = vibesql_db.cursor()
        result = run_aggregate_scenario(cursor, 'AVG', 'vibesql')
        assert_aggregate_result(result, 'AVG')

    benchmark(run_query)


def test_avg_10k_duckdb(benchmark, duckdb_db):
    """Benchmark AVG(value) aggregation on DuckDB (10K rows)."""
    setup_database_for_benchmark(duckdb_db, 'duckdb', 10000)

    def run_query():
        cursor = duckdb_db.cursor()
        result = run_aggregate_scenario(cursor, 'AVG', 'duckdb')
        assert_aggregate_result(result, 'AVG')

    benchmark(run_query)


def test_avg_100k_sqlite(benchmark, sqlite_db):
    """Benchmark AVG(value) aggregation on SQLite (100K rows)."""
    setup_database_for_benchmark(sqlite_db, 'sqlite', 100000)

    def run_query():
        cursor = sqlite_db.cursor()
        result = run_aggregate_scenario(cursor, 'AVG', 'sqlite')
        assert_aggregate_result(result, 'AVG')

    benchmark(run_query)


def test_avg_100k_vibesql(benchmark, vibesql_db):
    """Benchmark AVG(value) aggregation on vibesql (100K rows)."""
    setup_database_for_benchmark(vibesql_db, 'vibesql', 100000)

    def run_query():
        cursor = vibesql_db.cursor()
        result = run_aggregate_scenario(cursor, 'AVG', 'vibesql')
        assert_aggregate_result(result, 'AVG')

    benchmark(run_query)


def test_avg_100k_duckdb(benchmark, duckdb_db):
    """Benchmark AVG(value) aggregation on DuckDB (100K rows)."""
    setup_database_for_benchmark(duckdb_db, 'duckdb', 100000)

    def run_query():
        cursor = duckdb_db.cursor()
        result = run_aggregate_scenario(cursor, 'AVG', 'duckdb')
        assert_aggregate_result(result, 'AVG')

    benchmark(run_query)
