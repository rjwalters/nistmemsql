"""
Benchmark scenario helpers for database operations.

Provides reusable execution logic for INSERT, UPDATE, DELETE, SELECT WHERE,
and aggregate operations across different database backends.
"""
import random


def run_insert_scenario(cursor, start_id, count, db_type='sqlite'):
    """Execute INSERT benchmark scenario.

    Inserts rows with deterministic data for performance testing.
    Handles backend-specific query syntax.

    Args:
        cursor: Database cursor
        start_id: Starting ID for inserts
        count: Number of rows to insert
        db_type: One of 'sqlite', 'vibesql', or 'duckdb'
    """
    # Reset random seed for deterministic data
    random.seed(42)

    for i in range(count):
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


def run_update_scenario(cursor, count, db_type='sqlite'):
    """Execute UPDATE benchmark scenario.

    Updates rows by incrementing value column. Tests UPDATE performance
    with WHERE clause filtering by primary key.

    Args:
        cursor: Database cursor
        count: Number of rows to update
        db_type: One of 'sqlite', 'vibesql', or 'duckdb'
    """
    for i in range(count):
        if db_type in ['sqlite', 'duckdb']:
            cursor.execute(
                "UPDATE test_table SET value = value + 1 WHERE id = ?",
                (i,)
            )
        else:  # vibesql
            cursor.execute(
                f"UPDATE test_table SET value = value + 1 WHERE id = {i}"
            )


def run_delete_scenario(cursor, count, db_type='sqlite'):
    """Execute DELETE benchmark scenario.

    Deletes rows one at a time by primary key. Tests DELETE performance
    with WHERE clause filtering.

    Args:
        cursor: Database cursor
        count: Number of rows to delete
        db_type: One of 'sqlite', 'vibesql', or 'duckdb'
    """
    for i in range(count):
        if db_type in ['sqlite', 'duckdb']:
            cursor.execute(
                "DELETE FROM test_table WHERE id = ?",
                (i,)
            )
        else:  # vibesql
            cursor.execute(
                f"DELETE FROM test_table WHERE id = {i}"
            )


def run_select_where_scenario(cursor, row_limit, db_type='sqlite'):
    """Execute SELECT with WHERE clause benchmark scenario.

    Selects 10% of rows using id < threshold. Tests SELECT performance
    with filtering.

    Args:
        cursor: Database cursor
        row_limit: Threshold for WHERE clause (selects rows with id < row_limit)
        db_type: One of 'sqlite', 'vibesql', or 'duckdb'

    Returns:
        List of fetched rows
    """
    cursor.execute(f"SELECT * FROM test_table WHERE id < {row_limit}")
    return cursor.fetchall()


def run_aggregate_scenario(cursor, operation, db_type='sqlite'):
    """Execute aggregate operation benchmark scenario.

    Runs COUNT(*), SUM(value), or AVG(value) aggregation.
    Tests aggregate operation performance.

    Args:
        cursor: Database cursor
        operation: One of 'COUNT', 'SUM', or 'AVG'
        db_type: One of 'sqlite', 'vibesql', or 'duckdb'

    Returns:
        Aggregate result value
    """
    if operation == 'COUNT':
        cursor.execute("SELECT COUNT(*) FROM test_table")
    elif operation == 'SUM':
        cursor.execute("SELECT SUM(value) FROM test_table")
    elif operation == 'AVG':
        cursor.execute("SELECT AVG(value) FROM test_table")
    else:
        raise ValueError(f"Unknown aggregate operation: {operation}")

    return cursor.fetchone()[0]
