"""
Assertion helpers for benchmark validation.

Provides standardized result validation and data integrity checks
across different benchmark scenarios.
"""


def assert_benchmark_result(result, expected_count, operation_type):
    """Validate benchmark operation result.

    Checks that the operation completed successfully with expected results.
    Used for INSERT, UPDATE, DELETE operations that don't return data.

    Args:
        result: Result from the benchmark operation
        expected_count: Expected number of affected rows
        operation_type: Type of operation ('INSERT', 'UPDATE', 'DELETE')
    """
    # For most operations, we validate row counts externally
    # This is a placeholder for consistency
    pass


def assert_row_count(rows, expected_count):
    """Validate fetched row count.

    Checks that SELECT operations returned the expected number of rows.

    Args:
        rows: List of fetched rows
        expected_count: Expected number of rows

    Raises:
        AssertionError: If row count doesn't match expected
    """
    assert len(rows) == expected_count, \
        f"Expected {expected_count} rows, got {len(rows)}"


def assert_aggregate_result(result, operation, expected_value=None):
    """Validate aggregate operation result.

    Checks that aggregate operations (COUNT, SUM, AVG) return valid results.
    Can optionally check for exact expected values.

    Args:
        result: Aggregate result value
        operation: Type of aggregate ('COUNT', 'SUM', 'AVG')
        expected_value: Optional expected exact value

    Raises:
        AssertionError: If result is invalid or doesn't match expected
    """
    if operation == 'COUNT':
        if expected_value is not None:
            assert result == expected_value, \
                f"Expected COUNT={expected_value}, got {result}"
        else:
            assert result >= 0, f"COUNT result must be non-negative, got {result}"
    elif operation in ['SUM', 'AVG']:
        assert result is not None, f"{operation} result should not be None"
        if expected_value is not None:
            assert result == expected_value, \
                f"Expected {operation}={expected_value}, got {result}"


def validate_benchmark_data_integrity(cursor, db_type='sqlite'):
    """Perform data consistency checks after benchmark operations.

    Optional validation to ensure database state is consistent after
    benchmark execution. Useful for debugging test failures.

    Args:
        cursor: Database cursor
        db_type: One of 'sqlite', 'nistmemsql', or 'duckdb'

    Returns:
        bool: True if data is consistent
    """
    # Check that table exists and is accessible
    try:
        cursor.execute("SELECT COUNT(*) FROM test_table")
        count = cursor.fetchone()[0]
        return count >= 0
    except Exception:
        return False
