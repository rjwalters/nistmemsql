"""
Tier 1 Micro-Benchmarks: Basic operations comparison between nistmemsql, SQLite, and DuckDB.

These benchmarks establish fundamental performance characteristics for single-table operations.
Following the pattern from #650, expanded to cover INSERT, UPDATE, DELETE, WHERE, and aggregates.
Now includes DuckDB as an industry-standard baseline for in-memory analytical databases.
"""
import pytest
import random


# ============================================================================
# Helper Functions
# ============================================================================

def setup_test_table(connection, num_rows, db_type='sqlite'):
    """Helper function to create and populate test table.

    Creates table with schema matching #650 requirements:
    - id: INTEGER PRIMARY KEY
    - name: VARCHAR(20) NOT NULL
    - value: INTEGER NOT NULL

    Uses deterministic random data (seed=42) for reproducibility.

    Args:
        connection: Database connection
        num_rows: Number of rows to insert
        db_type: One of 'sqlite', 'nistmemsql', or 'duckdb'
    """
    cursor = connection.cursor()

    # Create table
    cursor.execute("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR(20) NOT NULL,
            value INTEGER NOT NULL
        )
    """)

    # Use deterministic seed for reproducibility
    random.seed(42)

    # Insert test data based on database type
    for i in range(num_rows):
        if db_type in ['sqlite', 'duckdb']:
            # SQLite and DuckDB both support parameterized queries
            cursor.execute(
                "INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
                (i, f"name_{i % 100}", random.randint(1, 1000))
            )
        else:
            # nistmemsql doesn't support parameterized queries yet, use string formatting
            value = random.randint(1, 1000)
            cursor.execute(
                f"INSERT INTO test_table (id, name, value) VALUES ({i}, 'name_{i % 100}', {value})"
            )

    # Commit for SQLite and DuckDB
    if db_type in ['sqlite', 'duckdb']:
        connection.commit()


# ============================================================================
# INSERT Benchmarks
# ============================================================================

def test_insert_1k_sqlite(benchmark, sqlite_db):
    """Benchmark INSERT operations on SQLite (1K rows)."""
    # Setup: Create empty table (once)
    cursor = sqlite_db.cursor()
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
        cursor = sqlite_db.cursor()
        start_id = next_id[0]
        # Insert 1K rows with incrementing IDs
        for i in range(1000):
            cursor.execute(
                "INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
                (start_id + i, f"name_{i % 100}", random.randint(1, 1000))
            )
        next_id[0] += 1000
        sqlite_db.commit()

    benchmark(run_inserts)


def test_insert_1k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark INSERT operations on nistmemsql (1K rows)."""
    # Setup: Create empty table (once)
    cursor = nistmemsql_db.cursor()
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
        cursor = nistmemsql_db.cursor()
        start_id = next_id[0]
        # Insert 1K rows with incrementing IDs
        for i in range(1000):
            value = random.randint(1, 1000)
            cursor.execute(
                f"INSERT INTO test_table (id, name, value) VALUES ({start_id + i}, 'name_{i % 100}', {value})"
            )
        next_id[0] += 1000

    benchmark(run_inserts)


def test_insert_1k_duckdb(benchmark, duckdb_db):
    """Benchmark INSERT operations on DuckDB (1K rows)."""
    # Setup: Create empty table (once)
    cursor = duckdb_db.cursor()
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
        cursor = duckdb_db.cursor()
        start_id = next_id[0]
        # Insert 1K rows with incrementing IDs
        for i in range(1000):
            cursor.execute(
                "INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
                (start_id + i, f"name_{i % 100}", random.randint(1, 1000))
            )
        next_id[0] += 1000
        duckdb_db.commit()

    benchmark(run_inserts)


def test_insert_10k_sqlite(benchmark, sqlite_db):
    """Benchmark INSERT operations on SQLite (10K rows)."""
    cursor = sqlite_db.cursor()
    cursor.execute("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR(20) NOT NULL,
            value INTEGER NOT NULL
        )
    """)

    next_id = [0]

    def run_inserts():
        random.seed(42)
        cursor = sqlite_db.cursor()
        start_id = next_id[0]
        for i in range(10000):
            cursor.execute(
                "INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
                (start_id + i, f"name_{i % 100}", random.randint(1, 1000))
            )
        next_id[0] += 10000
        sqlite_db.commit()

    benchmark(run_inserts)


def test_insert_10k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark INSERT operations on nistmemsql (10K rows)."""
    cursor = nistmemsql_db.cursor()
    cursor.execute("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR(20) NOT NULL,
            value INTEGER NOT NULL
        )
    """)

    next_id = [0]

    def run_inserts():
        random.seed(42)
        cursor = nistmemsql_db.cursor()
        start_id = next_id[0]
        for i in range(10000):
            value = random.randint(1, 1000)
            cursor.execute(
                f"INSERT INTO test_table (id, name, value) VALUES ({start_id + i}, 'name_{i % 100}', {value})"
            )
        next_id[0] += 10000

    benchmark(run_inserts)


def test_insert_10k_duckdb(benchmark, duckdb_db):
    """Benchmark INSERT operations on DuckDB (10K rows)."""
    cursor = duckdb_db.cursor()
    cursor.execute("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR(20) NOT NULL,
            value INTEGER NOT NULL
        )
    """)

    next_id = [0]

    def run_inserts():
        random.seed(42)
        cursor = duckdb_db.cursor()
        start_id = next_id[0]
        for i in range(10000):
            cursor.execute(
                "INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
                (start_id + i, f"name_{i % 100}", random.randint(1, 1000))
            )
        next_id[0] += 10000
        duckdb_db.commit()

    benchmark(run_inserts)


def test_insert_100k_sqlite(benchmark, sqlite_db):
    """Benchmark INSERT operations on SQLite (100K rows)."""
    cursor = sqlite_db.cursor()
    cursor.execute("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR(20) NOT NULL,
            value INTEGER NOT NULL
        )
    """)

    next_id = [0]

    def run_inserts():
        random.seed(42)
        cursor = sqlite_db.cursor()
        start_id = next_id[0]
        for i in range(100000):
            cursor.execute(
                "INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
                (start_id + i, f"name_{i % 100}", random.randint(1, 1000))
            )
        next_id[0] += 100000
        sqlite_db.commit()

    benchmark(run_inserts)


def test_insert_100k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark INSERT operations on nistmemsql (100K rows)."""
    cursor = nistmemsql_db.cursor()
    cursor.execute("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR(20) NOT NULL,
            value INTEGER NOT NULL
        )
    """)

    next_id = [0]

    def run_inserts():
        random.seed(42)
        cursor = nistmemsql_db.cursor()
        start_id = next_id[0]
        for i in range(100000):
            value = random.randint(1, 1000)
            cursor.execute(
                f"INSERT INTO test_table (id, name, value) VALUES ({start_id + i}, 'name_{i % 100}', {value})"
            )
        next_id[0] += 100000

    benchmark(run_inserts)


# ============================================================================
# UPDATE Benchmarks
# ============================================================================

def test_update_1k_sqlite(benchmark, sqlite_db):
    """Benchmark UPDATE operations on SQLite (1K rows)."""
    # Setup: Pre-populate table
    setup_test_table(sqlite_db, 1000, db_type='sqlite')

    def run_updates():
        cursor = sqlite_db.cursor()
        for i in range(1000):
            cursor.execute(
                "UPDATE test_table SET value = value + 1 WHERE id = ?",
                (i,)
            )
        sqlite_db.commit()

    benchmark(run_updates)


def test_update_1k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark UPDATE operations on nistmemsql (1K rows)."""
    # Setup: Pre-populate table
    setup_test_table(nistmemsql_db, 1000, db_type='nistmemsql')

    def run_updates():
        cursor = nistmemsql_db.cursor()
        for i in range(1000):
            cursor.execute(
                f"UPDATE test_table SET value = value + 1 WHERE id = {i}"
            )

    benchmark(run_updates)


def test_update_10k_sqlite(benchmark, sqlite_db):
    """Benchmark UPDATE operations on SQLite (10K rows)."""
    setup_test_table(sqlite_db, 10000, db_type='sqlite')

    def run_updates():
        cursor = sqlite_db.cursor()
        for i in range(10000):
            cursor.execute(
                "UPDATE test_table SET value = value + 1 WHERE id = ?",
                (i,)
            )
        sqlite_db.commit()

    benchmark(run_updates)


def test_update_10k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark UPDATE operations on nistmemsql (10K rows)."""
    setup_test_table(nistmemsql_db, 10000, db_type='nistmemsql')

    def run_updates():
        cursor = nistmemsql_db.cursor()
        for i in range(10000):
            cursor.execute(
                f"UPDATE test_table SET value = value + 1 WHERE id = {i}"
            )

    benchmark(run_updates)


def test_update_100k_sqlite(benchmark, sqlite_db):
    """Benchmark UPDATE operations on SQLite (100K rows)."""
    setup_test_table(sqlite_db, 100000, db_type='sqlite')

    def run_updates():
        cursor = sqlite_db.cursor()
        for i in range(100000):
            cursor.execute(
                "UPDATE test_table SET value = value + 1 WHERE id = ?",
                (i,)
            )
        sqlite_db.commit()

    benchmark(run_updates)


def test_update_100k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark UPDATE operations on nistmemsql (100K rows)."""
    setup_test_table(nistmemsql_db, 100000, db_type='nistmemsql')

    def run_updates():
        cursor = nistmemsql_db.cursor()
        for i in range(100000):
            cursor.execute(
                f"UPDATE test_table SET value = value + 1 WHERE id = {i}"
            )

    benchmark(run_updates)


# ============================================================================
# DELETE Benchmarks
# ============================================================================

def test_delete_1k_sqlite(benchmark, sqlite_db):
    """Benchmark DELETE operations on SQLite (1K rows)."""
    # Setup: Pre-populate table
    setup_test_table(sqlite_db, 1000, db_type='sqlite')

    def run_deletes():
        cursor = sqlite_db.cursor()
        for i in range(1000):
            cursor.execute(
                "DELETE FROM test_table WHERE id = ?",
                (i,)
            )
        sqlite_db.commit()

    benchmark(run_deletes)


def test_delete_1k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark DELETE operations on nistmemsql (1K rows)."""
    setup_test_table(nistmemsql_db, 1000, db_type='nistmemsql')

    def run_deletes():
        cursor = nistmemsql_db.cursor()
        for i in range(1000):
            cursor.execute(
                f"DELETE FROM test_table WHERE id = {i}"
            )

    benchmark(run_deletes)


def test_delete_10k_sqlite(benchmark, sqlite_db):
    """Benchmark DELETE operations on SQLite (10K rows)."""
    setup_test_table(sqlite_db, 10000, db_type='sqlite')

    def run_deletes():
        cursor = sqlite_db.cursor()
        for i in range(10000):
            cursor.execute(
                "DELETE FROM test_table WHERE id = ?",
                (i,)
            )
        sqlite_db.commit()

    benchmark(run_deletes)


def test_delete_10k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark DELETE operations on nistmemsql (10K rows)."""
    setup_test_table(nistmemsql_db, 10000, db_type='nistmemsql')

    def run_deletes():
        cursor = nistmemsql_db.cursor()
        for i in range(10000):
            cursor.execute(
                f"DELETE FROM test_table WHERE id = {i}"
            )

    benchmark(run_deletes)


def test_delete_100k_sqlite(benchmark, sqlite_db):
    """Benchmark DELETE operations on SQLite (100K rows)."""
    setup_test_table(sqlite_db, 100000, db_type='sqlite')

    def run_deletes():
        cursor = sqlite_db.cursor()
        for i in range(100000):
            cursor.execute(
                "DELETE FROM test_table WHERE id = ?",
                (i,)
            )
        sqlite_db.commit()

    benchmark(run_deletes)


def test_delete_100k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark DELETE operations on nistmemsql (100K rows)."""
    setup_test_table(nistmemsql_db, 100000, db_type='nistmemsql')

    def run_deletes():
        cursor = nistmemsql_db.cursor()
        for i in range(100000):
            cursor.execute(
                f"DELETE FROM test_table WHERE id = {i}"
            )

    benchmark(run_deletes)


# ============================================================================
# SELECT with WHERE Clause Benchmarks
# ============================================================================

def test_select_where_1k_sqlite(benchmark, sqlite_db):
    """Benchmark SELECT with WHERE clause on SQLite (filter 10% of 1K rows)."""
    setup_test_table(sqlite_db, 1000, db_type='sqlite')

    def run_query():
        cursor = sqlite_db.cursor()
        cursor.execute("SELECT * FROM test_table WHERE id < 100")
        rows = cursor.fetchall()
        assert len(rows) == 100

    benchmark(run_query)


def test_select_where_1k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark SELECT with WHERE clause on nistmemsql (filter 10% of 1K rows)."""
    setup_test_table(nistmemsql_db, 1000, db_type='nistmemsql')

    def run_query():
        cursor = nistmemsql_db.cursor()
        cursor.execute("SELECT * FROM test_table WHERE id < 100")
        rows = cursor.fetchall()
        assert len(rows) == 100

    benchmark(run_query)


def test_select_where_10k_sqlite(benchmark, sqlite_db):
    """Benchmark SELECT with WHERE clause on SQLite (filter 10% of 10K rows)."""
    setup_test_table(sqlite_db, 10000, db_type='sqlite')

    def run_query():
        cursor = sqlite_db.cursor()
        cursor.execute("SELECT * FROM test_table WHERE id < 1000")
        rows = cursor.fetchall()
        assert len(rows) == 1000

    benchmark(run_query)


def test_select_where_10k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark SELECT with WHERE clause on nistmemsql (filter 10% of 10K rows)."""
    setup_test_table(nistmemsql_db, 10000, db_type='nistmemsql')

    def run_query():
        cursor = nistmemsql_db.cursor()
        cursor.execute("SELECT * FROM test_table WHERE id < 1000")
        rows = cursor.fetchall()
        assert len(rows) == 1000

    benchmark(run_query)


def test_select_where_100k_sqlite(benchmark, sqlite_db):
    """Benchmark SELECT with WHERE clause on SQLite (filter 10% of 100K rows)."""
    setup_test_table(sqlite_db, 100000, db_type='sqlite')

    def run_query():
        cursor = sqlite_db.cursor()
        cursor.execute("SELECT * FROM test_table WHERE id < 10000")
        rows = cursor.fetchall()
        assert len(rows) == 10000

    benchmark(run_query)


def test_select_where_100k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark SELECT with WHERE clause on nistmemsql (filter 10% of 100K rows)."""
    setup_test_table(nistmemsql_db, 100000, db_type='nistmemsql')

    def run_query():
        cursor = nistmemsql_db.cursor()
        cursor.execute("SELECT * FROM test_table WHERE id < 10000")
        rows = cursor.fetchall()
        assert len(rows) == 10000

    benchmark(run_query)


# ============================================================================
# Aggregate Operation Benchmarks (COUNT, SUM, AVG)
# ============================================================================

def test_count_1k_sqlite(benchmark, sqlite_db):
    """Benchmark COUNT(*) aggregation on SQLite (1K rows)."""
    setup_test_table(sqlite_db, 1000, db_type='sqlite')

    def run_query():
        cursor = sqlite_db.cursor()
        cursor.execute("SELECT COUNT(*) FROM test_table")
        result = cursor.fetchone()[0]
        assert result == 1000

    benchmark(run_query)


def test_count_1k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark COUNT(*) aggregation on nistmemsql (1K rows)."""
    setup_test_table(nistmemsql_db, 1000, db_type='nistmemsql')

    def run_query():
        cursor = nistmemsql_db.cursor()
        cursor.execute("SELECT COUNT(*) FROM test_table")
        result = cursor.fetchone()[0]
        assert result == 1000

    benchmark(run_query)


def test_count_10k_sqlite(benchmark, sqlite_db):
    """Benchmark COUNT(*) aggregation on SQLite (10K rows)."""
    setup_test_table(sqlite_db, 10000, db_type='sqlite')

    def run_query():
        cursor = sqlite_db.cursor()
        cursor.execute("SELECT COUNT(*) FROM test_table")
        result = cursor.fetchone()[0]
        assert result == 10000

    benchmark(run_query)


def test_count_10k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark COUNT(*) aggregation on nistmemsql (10K rows)."""
    setup_test_table(nistmemsql_db, 10000, db_type='nistmemsql')

    def run_query():
        cursor = nistmemsql_db.cursor()
        cursor.execute("SELECT COUNT(*) FROM test_table")
        result = cursor.fetchone()[0]
        assert result == 10000

    benchmark(run_query)


def test_count_100k_sqlite(benchmark, sqlite_db):
    """Benchmark COUNT(*) aggregation on SQLite (100K rows)."""
    setup_test_table(sqlite_db, 100000, db_type='sqlite')

    def run_query():
        cursor = sqlite_db.cursor()
        cursor.execute("SELECT COUNT(*) FROM test_table")
        result = cursor.fetchone()[0]
        assert result == 100000

    benchmark(run_query)


def test_count_100k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark COUNT(*) aggregation on nistmemsql (100K rows)."""
    setup_test_table(nistmemsql_db, 100000, db_type='nistmemsql')

    def run_query():
        cursor = nistmemsql_db.cursor()
        cursor.execute("SELECT COUNT(*) FROM test_table")
        result = cursor.fetchone()[0]
        assert result == 100000

    benchmark(run_query)


def test_sum_1k_sqlite(benchmark, sqlite_db):
    """Benchmark SUM(value) aggregation on SQLite (1K rows)."""
    setup_test_table(sqlite_db, 1000, db_type='sqlite')

    def run_query():
        cursor = sqlite_db.cursor()
        cursor.execute("SELECT SUM(value) FROM test_table")
        result = cursor.fetchone()[0]
        assert result is not None

    benchmark(run_query)


def test_sum_1k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark SUM(value) aggregation on nistmemsql (1K rows)."""
    setup_test_table(nistmemsql_db, 1000, db_type='nistmemsql')

    def run_query():
        cursor = nistmemsql_db.cursor()
        cursor.execute("SELECT SUM(value) FROM test_table")
        result = cursor.fetchone()[0]
        assert result is not None

    benchmark(run_query)


def test_sum_10k_sqlite(benchmark, sqlite_db):
    """Benchmark SUM(value) aggregation on SQLite (10K rows)."""
    setup_test_table(sqlite_db, 10000, db_type='sqlite')

    def run_query():
        cursor = sqlite_db.cursor()
        cursor.execute("SELECT SUM(value) FROM test_table")
        result = cursor.fetchone()[0]
        assert result is not None

    benchmark(run_query)


def test_sum_10k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark SUM(value) aggregation on nistmemsql (10K rows)."""
    setup_test_table(nistmemsql_db, 10000, db_type='nistmemsql')

    def run_query():
        cursor = nistmemsql_db.cursor()
        cursor.execute("SELECT SUM(value) FROM test_table")
        result = cursor.fetchone()[0]
        assert result is not None

    benchmark(run_query)


def test_sum_100k_sqlite(benchmark, sqlite_db):
    """Benchmark SUM(value) aggregation on SQLite (100K rows)."""
    setup_test_table(sqlite_db, 100000, db_type='sqlite')

    def run_query():
        cursor = sqlite_db.cursor()
        cursor.execute("SELECT SUM(value) FROM test_table")
        result = cursor.fetchone()[0]
        assert result is not None

    benchmark(run_query)


def test_sum_100k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark SUM(value) aggregation on nistmemsql (100K rows)."""
    setup_test_table(nistmemsql_db, 100000, db_type='nistmemsql')

    def run_query():
        cursor = nistmemsql_db.cursor()
        cursor.execute("SELECT SUM(value) FROM test_table")
        result = cursor.fetchone()[0]
        assert result is not None

    benchmark(run_query)


def test_avg_1k_sqlite(benchmark, sqlite_db):
    """Benchmark AVG(value) aggregation on SQLite (1K rows)."""
    setup_test_table(sqlite_db, 1000, db_type='sqlite')

    def run_query():
        cursor = sqlite_db.cursor()
        cursor.execute("SELECT AVG(value) FROM test_table")
        result = cursor.fetchone()[0]
        assert result is not None

    benchmark(run_query)


def test_avg_1k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark AVG(value) aggregation on nistmemsql (1K rows)."""
    setup_test_table(nistmemsql_db, 1000, db_type='nistmemsql')

    def run_query():
        cursor = nistmemsql_db.cursor()
        cursor.execute("SELECT AVG(value) FROM test_table")
        result = cursor.fetchone()[0]
        assert result is not None

    benchmark(run_query)


def test_avg_10k_sqlite(benchmark, sqlite_db):
    """Benchmark AVG(value) aggregation on SQLite (10K rows)."""
    setup_test_table(sqlite_db, 10000, db_type='sqlite')

    def run_query():
        cursor = sqlite_db.cursor()
        cursor.execute("SELECT AVG(value) FROM test_table")
        result = cursor.fetchone()[0]
        assert result is not None

    benchmark(run_query)


def test_avg_10k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark AVG(value) aggregation on nistmemsql (10K rows)."""
    setup_test_table(nistmemsql_db, 10000, db_type='nistmemsql')

    def run_query():
        cursor = nistmemsql_db.cursor()
        cursor.execute("SELECT AVG(value) FROM test_table")
        result = cursor.fetchone()[0]
        assert result is not None

    benchmark(run_query)


def test_avg_100k_sqlite(benchmark, sqlite_db):
    """Benchmark AVG(value) aggregation on SQLite (100K rows)."""
    setup_test_table(sqlite_db, 100000, db_type='sqlite')

    def run_query():
        cursor = sqlite_db.cursor()
        cursor.execute("SELECT AVG(value) FROM test_table")
        result = cursor.fetchone()[0]
        assert result is not None

    benchmark(run_query)


def test_avg_100k_nistmemsql(benchmark, nistmemsql_db):
    """Benchmark AVG(value) aggregation on nistmemsql (100K rows)."""
    setup_test_table(nistmemsql_db, 100000, db_type='nistmemsql')

    def run_query():
        cursor = nistmemsql_db.cursor()
        cursor.execute("SELECT AVG(value) FROM test_table")
        result = cursor.fetchone()[0]
        assert result is not None

    benchmark(run_query)


def test_insert_100k_duckdb(benchmark, duckdb_db):
    """Benchmark INSERT operations on DuckDB (100K rows)."""
    cursor = duckdb_db.cursor()
    cursor.execute("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR(20) NOT NULL,
            value INTEGER NOT NULL
        )
    """)

    next_id = [0]

    def run_inserts():
        random.seed(42)
        cursor = duckdb_db.cursor()
        start_id = next_id[0]
        for i in range(100000):
            cursor.execute(
                "INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
                (start_id + i, f"name_{i % 100}", random.randint(1, 1000))
            )
        next_id[0] += 100000
        duckdb_db.commit()

    benchmark(run_inserts)


# ============================================================================
# UPDATE Benchmarks - DuckDB
# ============================================================================

def test_update_1k_duckdb(benchmark, duckdb_db):
    """Benchmark UPDATE operations on DuckDB (1K rows)."""
    setup_test_table(duckdb_db, 1000, db_type='duckdb')

    def run_updates():
        cursor = duckdb_db.cursor()
        for i in range(1000):
            cursor.execute(
                "UPDATE test_table SET value = value + 1 WHERE id = ?",
                (i,)
            )
        duckdb_db.commit()

    benchmark(run_updates)


def test_update_10k_duckdb(benchmark, duckdb_db):
    """Benchmark UPDATE operations on DuckDB (10K rows)."""
    setup_test_table(duckdb_db, 10000, db_type='duckdb')

    def run_updates():
        cursor = duckdb_db.cursor()
        for i in range(10000):
            cursor.execute(
                "UPDATE test_table SET value = value + 1 WHERE id = ?",
                (i,)
            )
        duckdb_db.commit()

    benchmark(run_updates)


def test_update_100k_duckdb(benchmark, duckdb_db):
    """Benchmark UPDATE operations on DuckDB (100K rows)."""
    setup_test_table(duckdb_db, 100000, db_type='duckdb')

    def run_updates():
        cursor = duckdb_db.cursor()
        for i in range(100000):
            cursor.execute(
                "UPDATE test_table SET value = value + 1 WHERE id = ?",
                (i,)
            )
        duckdb_db.commit()

    benchmark(run_updates)


# ============================================================================
# DELETE Benchmarks - DuckDB
# ============================================================================

def test_delete_1k_duckdb(benchmark, duckdb_db):
    """Benchmark DELETE operations on DuckDB (1K rows)."""
    setup_test_table(duckdb_db, 1000, db_type='duckdb')

    def run_deletes():
        cursor = duckdb_db.cursor()
        for i in range(1000):
            cursor.execute(
                "DELETE FROM test_table WHERE id = ?",
                (i,)
            )
        duckdb_db.commit()

    benchmark(run_deletes)


def test_delete_10k_duckdb(benchmark, duckdb_db):
    """Benchmark DELETE operations on DuckDB (10K rows)."""
    setup_test_table(duckdb_db, 10000, db_type='duckdb')

    def run_deletes():
        cursor = duckdb_db.cursor()
        for i in range(10000):
            cursor.execute(
                "DELETE FROM test_table WHERE id = ?",
                (i,)
            )
        duckdb_db.commit()

    benchmark(run_deletes)


def test_delete_100k_duckdb(benchmark, duckdb_db):
    """Benchmark DELETE operations on DuckDB (100K rows)."""
    setup_test_table(duckdb_db, 100000, db_type='duckdb')

    def run_deletes():
        cursor = duckdb_db.cursor()
        for i in range(100000):
            cursor.execute(
                "DELETE FROM test_table WHERE id = ?",
                (i,)
            )
        duckdb_db.commit()

    benchmark(run_deletes)


# ============================================================================
# SELECT with WHERE Clause Benchmarks - DuckDB
# ============================================================================

def test_select_where_1k_duckdb(benchmark, duckdb_db):
    """Benchmark SELECT with WHERE clause on DuckDB (filter 10% of 1K rows)."""
    setup_test_table(duckdb_db, 1000, db_type='duckdb')

    def run_query():
        cursor = duckdb_db.cursor()
        cursor.execute("SELECT * FROM test_table WHERE id < 100")
        rows = cursor.fetchall()
        assert len(rows) == 100

    benchmark(run_query)


def test_select_where_10k_duckdb(benchmark, duckdb_db):
    """Benchmark SELECT with WHERE clause on DuckDB (filter 10% of 10K rows)."""
    setup_test_table(duckdb_db, 10000, db_type='duckdb')

    def run_query():
        cursor = duckdb_db.cursor()
        cursor.execute("SELECT * FROM test_table WHERE id < 1000")
        rows = cursor.fetchall()
        assert len(rows) == 1000

    benchmark(run_query)


def test_select_where_100k_duckdb(benchmark, duckdb_db):
    """Benchmark SELECT with WHERE clause on DuckDB (filter 10% of 100K rows)."""
    setup_test_table(duckdb_db, 100000, db_type='duckdb')

    def run_query():
        cursor = duckdb_db.cursor()
        cursor.execute("SELECT * FROM test_table WHERE id < 10000")
        rows = cursor.fetchall()
        assert len(rows) == 10000

    benchmark(run_query)


# ============================================================================
# Aggregate Operation Benchmarks (COUNT, SUM, AVG) - DuckDB
# ============================================================================

def test_count_1k_duckdb(benchmark, duckdb_db):
    """Benchmark COUNT(*) aggregation on DuckDB (1K rows)."""
    setup_test_table(duckdb_db, 1000, db_type='duckdb')

    def run_query():
        cursor = duckdb_db.cursor()
        cursor.execute("SELECT COUNT(*) FROM test_table")
        result = cursor.fetchone()[0]
        assert result == 1000

    benchmark(run_query)


def test_count_10k_duckdb(benchmark, duckdb_db):
    """Benchmark COUNT(*) aggregation on DuckDB (10K rows)."""
    setup_test_table(duckdb_db, 10000, db_type='duckdb')

    def run_query():
        cursor = duckdb_db.cursor()
        cursor.execute("SELECT COUNT(*) FROM test_table")
        result = cursor.fetchone()[0]
        assert result == 10000

    benchmark(run_query)


def test_count_100k_duckdb(benchmark, duckdb_db):
    """Benchmark COUNT(*) aggregation on DuckDB (100K rows)."""
    setup_test_table(duckdb_db, 100000, db_type='duckdb')

    def run_query():
        cursor = duckdb_db.cursor()
        cursor.execute("SELECT COUNT(*) FROM test_table")
        result = cursor.fetchone()[0]
        assert result == 100000

    benchmark(run_query)


def test_sum_1k_duckdb(benchmark, duckdb_db):
    """Benchmark SUM(value) aggregation on DuckDB (1K rows)."""
    setup_test_table(duckdb_db, 1000, db_type='duckdb')

    def run_query():
        cursor = duckdb_db.cursor()
        cursor.execute("SELECT SUM(value) FROM test_table")
        result = cursor.fetchone()[0]
        assert result is not None

    benchmark(run_query)


def test_sum_10k_duckdb(benchmark, duckdb_db):
    """Benchmark SUM(value) aggregation on DuckDB (10K rows)."""
    setup_test_table(duckdb_db, 10000, db_type='duckdb')

    def run_query():
        cursor = duckdb_db.cursor()
        cursor.execute("SELECT SUM(value) FROM test_table")
        result = cursor.fetchone()[0]
        assert result is not None

    benchmark(run_query)


def test_sum_100k_duckdb(benchmark, duckdb_db):
    """Benchmark SUM(value) aggregation on DuckDB (100K rows)."""
    setup_test_table(duckdb_db, 100000, db_type='duckdb')

    def run_query():
        cursor = duckdb_db.cursor()
        cursor.execute("SELECT SUM(value) FROM test_table")
        result = cursor.fetchone()[0]
        assert result is not None

    benchmark(run_query)


def test_avg_1k_duckdb(benchmark, duckdb_db):
    """Benchmark AVG(value) aggregation on DuckDB (1K rows)."""
    setup_test_table(duckdb_db, 1000, db_type='duckdb')

    def run_query():
        cursor = duckdb_db.cursor()
        cursor.execute("SELECT AVG(value) FROM test_table")
        result = cursor.fetchone()[0]
        assert result is not None

    benchmark(run_query)


def test_avg_10k_duckdb(benchmark, duckdb_db):
    """Benchmark AVG(value) aggregation on DuckDB (10K rows)."""
    setup_test_table(duckdb_db, 10000, db_type='duckdb')

    def run_query():
        cursor = duckdb_db.cursor()
        cursor.execute("SELECT AVG(value) FROM test_table")
        result = cursor.fetchone()[0]
        assert result is not None

    benchmark(run_query)


def test_avg_100k_duckdb(benchmark, duckdb_db):
    """Benchmark AVG(value) aggregation on DuckDB (100K rows)."""
    setup_test_table(duckdb_db, 100000, db_type='duckdb')

    def run_query():
        cursor = duckdb_db.cursor()
        cursor.execute("SELECT AVG(value) FROM test_table")
        result = cursor.fetchone()[0]
        assert result is not None

    benchmark(run_query)
