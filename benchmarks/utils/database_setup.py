"""
Database connection and setup utilities for benchmarks.

Handles connection management, schema creation, and database initialization
for both nistmemsql and SQLite databases.
"""
import sqlite3
import os
import tempfile
from typing import Optional, Any


class DatabaseManager:
    """Manages database connections and setup for benchmarking."""

    def __init__(self, db_type: str = 'sqlite', database_path: Optional[str] = None):
        """
        Initialize database manager.

        Args:
            db_type: 'sqlite' or 'nistmemsql'
            database_path: Path to database file, or None for in-memory
        """
        self.db_type = db_type
        self.database_path = database_path or ':memory:'
        self.connection = None

    def connect(self) -> Any:
        """Create and return database connection."""
        if self.db_type == 'sqlite':
            self.connection = sqlite3.connect(self.database_path)
            # Enable foreign keys for referential integrity
            self.connection.execute("PRAGMA foreign_keys = ON")
        elif self.db_type == 'nistmemsql':
            # TODO: Replace with actual nistmemsql connection once PyO3 bindings exist
            # import nistmemsql
            # self.connection = nistmemsql.connect(self.database_path)
            raise NotImplementedError("nistmemsql Python bindings not yet implemented")
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

        return self.connection

    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None

    def execute_script(self, script_path: str):
        """Execute SQL script from file."""
        if not self.connection:
            raise RuntimeError("No active connection")

        with open(script_path, 'r') as f:
            script = f.read()

        self.connection.executescript(script)
        self.connection.commit()

    def setup_schema(self, schema_type: str = 'basic'):
        """
        Set up database schema for benchmarking.

        Args:
            schema_type: Type of schema ('basic', 'tpch', 'conformance')
        """
        if not self.connection:
            self.connect()

        if schema_type == 'basic':
            self._setup_basic_schema()
        elif schema_type == 'tpch':
            self._setup_tpch_schema()
        elif schema_type == 'conformance':
            self._setup_conformance_schema()
        else:
            raise ValueError(f"Unknown schema type: {schema_type}")

    def _setup_basic_schema(self):
        """Create basic schema for micro-benchmarks."""
        schema_sql = """
        CREATE TABLE IF NOT EXISTS test_table (
            id INTEGER PRIMARY KEY,
            name TEXT,
            value INTEGER,
            data REAL
        );

        CREATE INDEX IF NOT EXISTS idx_test_table_value ON test_table(value);
        """
        self.connection.executescript(schema_sql)
        self.connection.commit()

    def _setup_tpch_schema(self):
        """Create TPC-H benchmark schema."""
        schema_sql = """
        -- Region table
        CREATE TABLE IF NOT EXISTS region (
            r_regionkey INTEGER PRIMARY KEY,
            r_name TEXT,
            r_comment TEXT
        );

        -- Nation table
        CREATE TABLE IF NOT EXISTS nation (
            n_nationkey INTEGER PRIMARY KEY,
            n_name TEXT,
            n_regionkey INTEGER,
            n_comment TEXT
        );

        -- Supplier table
        CREATE TABLE IF NOT EXISTS supplier (
            s_suppkey INTEGER PRIMARY KEY,
            s_name TEXT,
            s_address TEXT,
            s_nationkey INTEGER,
            s_phone TEXT,
            s_acctbal REAL,
            s_comment TEXT
        );

        -- Customer table
        CREATE TABLE IF NOT EXISTS customer (
            c_custkey INTEGER PRIMARY KEY,
            c_name TEXT,
            c_address TEXT,
            c_nationkey INTEGER,
            c_phone TEXT,
            c_acctbal REAL,
            c_mktsegment TEXT,
            c_comment TEXT
        );

        -- Orders table
        CREATE TABLE IF NOT EXISTS orders (
            o_orderkey INTEGER PRIMARY KEY,
            o_custkey INTEGER,
            o_orderstatus TEXT,
            o_totalprice REAL,
            o_orderdate TEXT,
            o_orderpriority TEXT,
            o_clerk TEXT,
            o_shippriority INTEGER,
            o_comment TEXT
        );

        -- Lineitem table
        CREATE TABLE IF NOT EXISTS lineitem (
            l_orderkey INTEGER,
            l_partkey INTEGER,
            l_suppkey INTEGER,
            l_linenumber INTEGER,
            l_quantity REAL,
            l_extendedprice REAL,
            l_discount REAL,
            l_tax REAL,
            l_returnflag TEXT,
            l_linestatus TEXT,
            l_shipdate TEXT,
            l_commitdate TEXT,
            l_receiptdate TEXT,
            l_shipinstruct TEXT,
            l_shipmode TEXT,
            l_comment TEXT,
            PRIMARY KEY (l_orderkey, l_linenumber)
        );

        -- Create indexes for better performance
        CREATE INDEX IF NOT EXISTS idx_customer_nationkey ON customer(c_nationkey);
        CREATE INDEX IF NOT EXISTS idx_orders_custkey ON orders(o_custkey);
        CREATE INDEX IF NOT EXISTS idx_orders_orderdate ON orders(o_orderdate);
        CREATE INDEX IF NOT EXISTS idx_lineitem_orderkey ON lineitem(l_orderkey);
        CREATE INDEX IF NOT EXISTS idx_lineitem_suppkey ON lineitem(l_suppkey);
        CREATE INDEX IF NOT EXISTS idx_lineitem_shipdate ON lineitem(l_shipdate);
        """
        self.connection.executescript(schema_sql)
        self.connection.commit()

    def _setup_conformance_schema(self):
        """Create schema for NIST SQL:1999 conformance testing."""
        schema_sql = """
        -- Basic test tables for E011 (aggregates)
        CREATE TABLE IF NOT EXISTS agg_test (
            id INTEGER PRIMARY KEY,
            category TEXT,
            value REAL
        );

        -- Tables for join testing (F031, F041)
        CREATE TABLE IF NOT EXISTS table1 (
            id INTEGER PRIMARY KEY,
            value INTEGER
        );

        CREATE TABLE IF NOT EXISTS table2 (
            id INTEGER PRIMARY KEY,
            data TEXT
        );

        CREATE TABLE IF NOT EXISTS table3 (
            id INTEGER PRIMARY KEY,
            info TEXT
        );

        -- Tables for subquery testing
        CREATE TABLE IF NOT EXISTS employees (
            emp_id INTEGER PRIMARY KEY,
            name TEXT,
            dept_id INTEGER,
            salary REAL
        );

        CREATE TABLE IF NOT EXISTS departments (
            dept_id INTEGER PRIMARY KEY,
            name TEXT,
            budget REAL
        );

        -- Tables for window function testing
        CREATE TABLE IF NOT EXISTS sales (
            sale_id INTEGER PRIMARY KEY,
            product_id INTEGER,
            salesperson_id INTEGER,
            amount REAL,
            sale_date TEXT
        );
        """
        self.connection.executescript(schema_sql)
        self.connection.commit()

    def load_data(self, data_type: str = 'basic', num_rows: int = 1000):
        """
        Load test data into database.

        Args:
            data_type: Type of data ('basic', 'tpch', 'random')
            num_rows: Number of rows to generate
        """
        if not self.connection:
            self.connect()

        if data_type == 'basic':
            self._load_basic_data(num_rows)
        elif data_type == 'tpch':
            self._load_tpch_data(num_rows)
        else:
            raise ValueError(f"Unknown data type: {data_type}")

    def _load_basic_data(self, num_rows: int):
        """Load basic test data."""
        cursor = self.connection.cursor()
        for i in range(num_rows):
            cursor.execute(
                "INSERT INTO test_table (id, name, value, data) VALUES (?, ?, ?, ?)",
                (i, f'name_{i}', i * 10, float(i) / 100.0)
            )
        self.connection.commit()

    def _load_tpch_data(self, num_rows: int):
        """Load simplified TPC-H test data."""
        # This would use data_generator.py in real implementation
        # For now, just load minimal sample data
        cursor = self.connection.cursor()

        # Sample regions
        regions = [
            (0, 'AFRICA', 'comment'),
            (1, 'AMERICA', 'comment'),
            (2, 'ASIA', 'comment'),
            (3, 'EUROPE', 'comment'),
            (4, 'MIDDLE EAST', 'comment')
        ]
        cursor.executemany("INSERT INTO region VALUES (?, ?, ?)", regions)

        # Sample nations
        nations = [
            (0, 'ALGERIA', 0, 'comment'),
            (1, 'ARGENTINA', 1, 'comment'),
            (15, 'CHINA', 2, 'comment')
        ]
        cursor.executemany("INSERT INTO nation VALUES (?, ?, ?, ?)", nations)

        # Sample customers, orders, lineitems (simplified)
        for i in range(min(num_rows, 100)):  # Limit for demo
            cursor.execute(
                "INSERT INTO customer VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (i, f'Customer#{i}', f'Address{i}', i % 3, f'Phone{i}', 1000.0, 'BUILDING', 'comment')
            )
            cursor.execute(
                "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (i, i, 'O', 1000.0, '1995-01-01', '1-URGENT', f'Clerk{i:09d}', 0, 'comment')
            )
            cursor.execute(
                "INSERT INTO lineitem VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (i, i*100, i, 1, 10.0, 100.0, 0.1, 0.0, 'N', 'O', '1995-01-01', '1995-01-01',
                 '1995-01-01', 'DELIVER IN PERSON', 'TRUCK', 'comment')
            )

        self.connection.commit()

    def get_connection_info(self) -> dict:
        """Get information about the current connection."""
        return {
            'db_type': self.db_type,
            'database_path': self.database_path,
            'connected': self.connection is not None
        }


def create_temp_database(db_type: str = 'sqlite') -> DatabaseManager:
    """Create a temporary database for testing."""
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_file.close()

    db = DatabaseManager(db_type, temp_file.name)

    # Register cleanup
    import atexit
    atexit.register(lambda: os.unlink(temp_file.name))

    return db


def benchmark_context(db_type: str = 'sqlite', database_path: Optional[str] = None):
    """
    Context manager for benchmark database connections.

    Usage:
        with benchmark_context('sqlite') as conn:
            # Use connection
            pass
    """
    db = DatabaseManager(db_type, database_path)
    try:
        conn = db.connect()
        yield conn
    finally:
        db.disconnect()
