"""Example benchmark test demonstrating framework usage"""
import pytest

def test_simple_select_sqlite(benchmark, sqlite_db):
    """Baseline: Simple SELECT on SQLite"""
    # Setup
    sqlite_db.execute("CREATE TABLE test (id INTEGER, name VARCHAR(50))")
    sqlite_db.execute("INSERT INTO test VALUES (1, 'Alice')")
    sqlite_db.commit()

    # Benchmark
    def run_query():
        cursor = sqlite_db.execute("SELECT * FROM test")
        return cursor.fetchall()

    result = benchmark(run_query)
    assert len(result) == 1

def test_simple_select_vibesql(benchmark, vibesql_db):
    """Baseline: Simple SELECT on vibesql"""
    # Setup
    cursor = vibesql_db.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, name VARCHAR(50))")
    cursor.execute("INSERT INTO test VALUES (1, 'Alice')")

    # Benchmark
    def run_query():
        cursor = vibesql_db.cursor()
        cursor.execute("SELECT * FROM test")
        return cursor.fetchall()

    result = benchmark(run_query)
    assert len(result) == 1

@pytest.mark.parametrize("db_name", ["sqlite", "vibesql"])
def test_head_to_head_select(benchmark, both_databases, setup_test_table, db_name):
    """Head-to-head comparison: Simple SELECT"""
    db = both_databases[db_name]

    # Insert test data
    if db_name == "sqlite":
        db.execute("INSERT INTO test_users VALUES (1, 'Alice', 30, 50000.0, TRUE)")
        db.commit()

        def run_query():
            return db.execute("SELECT * FROM test_users WHERE id = 1").fetchall()
    else:
        cursor = db.cursor()
        cursor.execute("INSERT INTO test_users VALUES (1, 'Alice', 30, 50000.0, TRUE)")

        def run_query():
            cursor = db.cursor()
            cursor.execute("SELECT * FROM test_users WHERE id = 1")
            return cursor.fetchall()

    result = benchmark(run_query)
    assert len(result) == 1
