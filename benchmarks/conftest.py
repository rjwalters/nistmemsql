"""
pytest configuration and fixtures for nistmemsql benchmark suite.
"""
import pytest
import sqlite3
import sys
import os

# Add project root to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


@pytest.fixture(scope="session")
def benchmark_config():
    """Benchmark configuration settings."""
    return {
        'iterations': 100,
        'rounds': 10,
        'warmup_rounds': 5,
    }


@pytest.fixture(scope="function")
def sqlite_connection():
    """SQLite in-memory connection fixture."""
    conn = sqlite3.connect(':memory:')
    yield conn
    conn.close()


@pytest.fixture(scope="function")
def nistmemsql_connection():
    """nistmemsql connection fixture (placeholder for when bindings exist)."""
    # TODO: Replace with actual nistmemsql connection once PyO3 bindings are implemented
    # import nistmemsql
    # conn = nistmemsql.connect(':memory:')
    # yield conn
    # conn.close()

    # For now, raise NotImplementedError to indicate bindings don't exist yet
    pytest.skip("nistmemsql Python bindings not yet implemented")
