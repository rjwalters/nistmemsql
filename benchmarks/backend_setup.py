"""
Backend setup helpers for benchmark tests.

Provides standardized database initialization, table creation, and data insertion
across different backend types (SQLite, nistmemsql, DuckDB).
"""
import random


def create_test_table(cursor, db_type='sqlite'):
    """Create standardized test table schema.

    Creates table with schema:
    - id: INTEGER PRIMARY KEY
    - name: VARCHAR(20) NOT NULL
    - value: INTEGER NOT NULL

    Args:
        cursor: Database cursor
        db_type: One of 'sqlite', 'vibesql', or 'duckdb'
    """
    cursor.execute("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR(20) NOT NULL,
            value INTEGER NOT NULL
        )
    """)


def insert_test_data(cursor, start_id, count, db_type='sqlite'):
    """Insert deterministic test data into test_table.

    Uses deterministic random data (seed=42) for reproducibility.
    Handles backend-specific query syntax differences.

    Args:
        cursor: Database cursor
        start_id: Starting ID for inserts
        count: Number of rows to insert
        db_type: One of 'sqlite', 'vibesql', or 'duckdb'
    """
    # Use deterministic seed for reproducibility
    random.seed(42)

    # Insert data based on database type
    for i in range(count):
        if db_type in ['sqlite', 'duckdb']:
            # SQLite and DuckDB both support parameterized queries
            cursor.execute(
                "INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
                (start_id + i, f"name_{i % 100}", random.randint(1, 1000))
            )
        else:  # vibesql
            # vibesql doesn't support parameterized queries yet, use string formatting
            value = random.randint(1, 1000)
            cursor.execute(
                f"INSERT INTO test_table (id, name, value) VALUES ({start_id + i}, 'name_{i % 100}', {value})"
            )


def setup_database_for_benchmark(connection, db_type, row_count):
    """Complete database preparation for benchmarks.

    Creates table and populates with test data. This is a convenience
    function combining table creation and data insertion for setup phases.

    Args:
        connection: Database connection
        db_type: One of 'sqlite', 'vibesql', or 'duckdb'
        row_count: Number of rows to pre-populate
    """
    cursor = connection.cursor()
    create_test_table(cursor, db_type)
    insert_test_data(cursor, 0, row_count, db_type)

    # Commit for SQLite and DuckDB
    if db_type in ['sqlite', 'duckdb']:
        connection.commit()


def get_cursor_with_transaction(connection, db_type):
    """Get cursor with appropriate transaction handling.

    Returns a cursor configured for the specific database backend.
    This is primarily for consistency, as cursor creation is uniform.

    Args:
        connection: Database connection
        db_type: One of 'sqlite', 'vibesql', or 'duckdb'

    Returns:
        Database cursor
    """
    return connection.cursor()
