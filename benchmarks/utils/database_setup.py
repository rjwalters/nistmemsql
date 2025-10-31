"""Database connection and setup utilities"""
import sqlite3
import duckdb
import nistmemsql

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
