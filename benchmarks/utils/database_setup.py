"""Database connection and setup utilities"""
import sqlite3
import duckdb
import nistmemsql
import random

def create_sqlite_connection(in_memory=True):
    """Create SQLite connection"""
    db_path = ':memory:' if in_memory else 'test.db'
    return sqlite3.connect(db_path)

def create_nistmemsql_connection():
    """Create nistmemsql connection"""
    return nistmemsql.connect()

def create_duckdb_connection(in_memory=True):
    """Create DuckDB connection"""
    db_path = ':memory:' if in_memory else 'test.duckdb'
    return duckdb.connect(db_path)

def execute_sql_both(sqlite_conn, nistmemsql_conn, sql, params=None):
    """Execute SQL on both databases and return results"""
    results = {}

    # SQLite execution
    sqlite_cursor = sqlite_conn.cursor()
    if params:
        sqlite_cursor.execute(sql, params)
    else:
        sqlite_cursor.execute(sql)
    results['sqlite'] = sqlite_cursor.fetchall()

    # nistmemsql execution
    nistmemsql_cursor = nistmemsql_conn.cursor()
    if params:
        nistmemsql_cursor.execute(sql, params)
    else:
        nistmemsql_cursor.execute(sql)
    results['nistmemsql'] = nistmemsql_cursor.fetchall()

    return results

def execute_sql_all(sqlite_conn, nistmemsql_conn, duckdb_conn, sql, params=None):
    """Execute SQL on all three databases and return results"""
    results = {}

    # SQLite execution
    sqlite_cursor = sqlite_conn.cursor()
    if params:
        sqlite_cursor.execute(sql, params)
    else:
        sqlite_cursor.execute(sql)
    results['sqlite'] = sqlite_cursor.fetchall()

    # nistmemsql execution
    nistmemsql_cursor = nistmemsql_conn.cursor()
    if params:
        nistmemsql_cursor.execute(sql, params)
    else:
        nistmemsql_cursor.execute(sql)
    results['nistmemsql'] = nistmemsql_cursor.fetchall()

    # DuckDB execution
    duckdb_cursor = duckdb_conn.cursor()
    if params:
        duckdb_cursor.execute(sql, params)
    else:
        duckdb_cursor.execute(sql)
    results['duckdb'] = duckdb_cursor.fetchall()

    return results

def setup_test_table(connection, num_rows, db_type='sqlite'):
    """Helper function to create and populate test table.

    Creates table with schema:
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
        else:  # nistmemsql
            # nistmemsql doesn't support parameterized queries yet
            value = random.randint(1, 1000)
            cursor.execute(
                f"INSERT INTO test_table (id, name, value) VALUES ({i}, 'name_{i % 100}', {value})"
            )

    if db_type in ['sqlite', 'duckdb']:
        connection.commit()
