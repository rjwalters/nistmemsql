
import pytest
import sqlite3
import nistmemsql
import json
import platform
import psutil
from pathlib import Path

# pytest-benchmark configuration
def pytest_configure(config):
    """Configure pytest-benchmark for consistent measurements"""
    config.option.benchmark_min_rounds = 5
    config.option.benchmark_warmup = True
    config.option.benchmark_warmup_iterations = 2
    config.option.benchmark_disable_gc = True  # Disable GC during timing

@pytest.fixture(scope="session")
def hardware_metadata():
    """Collect hardware and environment metadata"""
    return {
        "cpu": platform.processor() or platform.machine(),
        "cpu_count": psutil.cpu_count(logical=False),
        "memory_gb": round(psutil.virtual_memory().total / (1024**3), 2),
        "os": f"{platform.system()} {platform.release()}",
        "python_version": platform.python_version(),
        "timestamp": None  # Set at runtime
    }

@pytest.fixture
def sqlite_db():
    """Create SQLite in-memory database connection"""
    conn = sqlite3.connect(':memory:')
    yield conn
    conn.close()

@pytest.fixture
def nistmemsql_db():
    """Create nistmemsql in-memory database connection"""
    # Assuming PyO3 bindings follow DB-API 2.0 pattern
    conn = nistmemsql.connect()  # In-memory by default
    yield conn
    conn.close()

@pytest.fixture
def both_databases(sqlite_db, nistmemsql_db):
    """Provide both databases for head-to-head comparison"""
    return {
        'sqlite': sqlite_db,
        'nistmemsql': nistmemsql_db
    }

@pytest.fixture
def setup_test_table(both_databases):
    """Create identical test table in both databases"""
    schema = """
        CREATE TABLE test_users (
            id INTEGER PRIMARY KEY,
            name VARCHAR(50),
            age INTEGER,
            salary FLOAT,
            active BOOLEAN
        )
    """

    # Create table in SQLite
    sqlite_db = both_databases['sqlite']
    sqlite_db.execute(schema)

    # Create table in nistmemsql
    nistmemsql_db = both_databases['nistmemsql']
    cursor = nistmemsql_db.cursor()
    cursor.execute(schema)

    return both_databases

@pytest.fixture
def insert_test_data(setup_test_table, request):
    """Insert test data into both databases

    Usage: @pytest.mark.parametrize("row_count", [1000, 10000])
    """
    row_count = getattr(request, 'param', 1000)  # Default 1K rows

    databases = setup_test_table

    # Generate test data
    test_data = [
        (i, f"User{i}", 20 + (i % 50), 30000.0 + (i * 100), i % 2 == 0)
        for i in range(row_count)
    ]

    # Insert into SQLite
    sqlite_db = databases['sqlite']
    sqlite_db.executemany(
        "INSERT INTO test_users VALUES (?, ?, ?, ?, ?)",
        test_data
    )
    sqlite_db.commit()

    # Insert into nistmemsql
    nistmemsql_db = databases['nistmemsql']
    cursor = nistmemsql_db.cursor()
    for row in test_data:
        cursor.execute(
            "INSERT INTO test_users VALUES (?, ?, ?, ?, ?)",
            row
        )

    return databases, row_count
