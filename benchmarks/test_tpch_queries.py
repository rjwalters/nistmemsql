"""
Tier 2 Analytical Benchmarks: TPC-H inspired queries.

These benchmarks simulate real-world analytical workloads with complex joins,
aggregations, and subqueries adapted from the TPC-H benchmark suite.
"""
import pytest


def test_tpch_q1_pricing_summary(benchmark, sqlite_connection, nistmemsql_connection):
    """
    TPC-H Q1: Pricing Summary Report Query

    Original: Aggregate pricing summary for line items shipped within last 90 days.
    Adapted: Simplified version with date filtering and aggregations.
    """
    setup_tpch_schema(sqlite_connection)
    # setup_tpch_schema(nistmemsql_connection)

    def run_query():
        sqlite_cursor = sqlite_connection.cursor()
        sqlite_cursor.execute("""
            SELECT
                l_returnflag,
                l_linestatus,
                SUM(l_quantity) as sum_qty,
                SUM(l_extendedprice) as sum_base_price,
                COUNT(*) as count_order
            FROM lineitem
            WHERE l_shipdate <= '1998-09-01'
            GROUP BY l_returnflag, l_linestatus
            ORDER BY l_returnflag, l_linestatus
        """)

        # nistmemsql_cursor = nistmemsql_connection.cursor()
        # nistmemsql_cursor.execute(...)  # Same query
        pass

    benchmark(run_query)


def test_tpch_q3_shipping_priority(benchmark, sqlite_connection, nistmemsql_connection):
    """
    TPC-H Q3: Shipping Priority Query

    Original: Find the shipping priority and potential revenue for orders
    that have not been shipped yet.
    """
    setup_tpch_schema(sqlite_connection)
    # setup_tpch_schema(nistmemsql_connection)

    def run_query():
        sqlite_cursor = sqlite_connection.cursor()
        sqlite_cursor.execute("""
            SELECT
                l_orderkey,
                SUM(l_extendedprice * (1 - l_discount)) as revenue,
                o_orderdate,
                o_shippriority
            FROM customer, orders, lineitem
            WHERE c_mktsegment = 'BUILDING'
                AND c_custkey = o_custkey
                AND l_orderkey = o_orderkey
                AND o_orderdate < '1995-03-15'
                AND l_shipdate > '1995-03-15'
            GROUP BY l_orderkey, o_orderdate, o_shippriority
            ORDER BY revenue desc, o_orderdate
            LIMIT 10
        """)

        # nistmemsql_cursor = nistmemsql_connection.cursor()
        # nistmemsql_cursor.execute(...)  # Same query
        pass

    benchmark(run_query)


def test_tpch_q5_local_supplier_volume(benchmark, sqlite_connection, nistmemsql_connection):
    """
    TPC-H Q5: Local Supplier Volume Query

    Original: Find the revenue volume by nation and year for local suppliers.
    """
    setup_tpch_schema(sqlite_connection)
    # setup_tpch_schema(nistmemsql_connection)

    def run_query():
        sqlite_cursor = sqlite_connection.cursor()
        sqlite_cursor.execute("""
            SELECT
                n_name,
                SUM(l_extendedprice * (1 - l_discount)) as revenue
            FROM customer, orders, lineitem, supplier, nation, region
            WHERE c_custkey = o_custkey
                AND l_orderkey = o_orderkey
                AND l_suppkey = s_suppkey
                AND c_nationkey = s_nationkey
                AND s_nationkey = n_nationkey
                AND n_regionkey = r_regionkey
                AND r_name = 'ASIA'
                AND o_orderdate >= '1994-01-01'
                AND o_orderdate < '1995-01-01'
            GROUP BY n_name
            ORDER BY revenue desc
        """)

        # nistmemsql_cursor = nistmemsql_connection.cursor()
        # nistmemsql_cursor.execute(...)  # Same query
        pass

    benchmark(run_query)


def setup_tpch_schema(connection):
    """Create simplified TPC-H schema and load sample data."""
    cursor = connection.cursor()

    # Create tables (simplified schema)
    cursor.execute("""
        CREATE TABLE region (
            r_regionkey INTEGER PRIMARY KEY,
            r_name TEXT,
            r_comment TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE nation (
            n_nationkey INTEGER PRIMARY KEY,
            n_name TEXT,
            n_regionkey INTEGER,
            n_comment TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE supplier (
            s_suppkey INTEGER PRIMARY KEY,
            s_name TEXT,
            s_address TEXT,
            s_nationkey INTEGER,
            s_phone TEXT,
            s_acctbal REAL,
            s_comment TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE customer (
            c_custkey INTEGER PRIMARY KEY,
            c_name TEXT,
            c_address TEXT,
            c_nationkey INTEGER,
            c_phone TEXT,
            c_acctbal REAL,
            c_mktsegment TEXT,
            c_comment TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE orders (
            o_orderkey INTEGER PRIMARY KEY,
            o_custkey INTEGER,
            o_orderstatus TEXT,
            o_totalprice REAL,
            o_orderdate TEXT,
            o_orderpriority TEXT,
            o_clerk TEXT,
            o_shippriority INTEGER,
            o_comment TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE lineitem (
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
        )
    """)

    # Load sample data (small dataset for benchmarking)
    # In real implementation, this would load from data_generator.py
    load_sample_tpch_data(cursor)

    connection.commit()


def load_sample_tpch_data(cursor):
    """Load minimal sample data for TPC-H benchmarking."""
    # Insert sample regions
    cursor.executemany(
        "INSERT INTO region VALUES (?, ?, ?)",
        [
            (0, 'AFRICA', 'comment'),
            (1, 'AMERICA', 'comment'),
            (2, 'ASIA', 'comment'),
            (3, 'EUROPE', 'comment'),
            (4, 'MIDDLE EAST', 'comment')
        ]
    )

    # Insert sample nations
    cursor.executemany(
        "INSERT INTO nation VALUES (?, ?, ?, ?)",
        [
            (0, 'ALGERIA', 0, 'comment'),
            (1, 'ARGENTINA', 1, 'comment'),
            (2, 'BRAZIL', 1, 'comment'),
            (15, 'MOROCCO', 0, 'comment'),
            (16, 'MOZAMBIQUE', 0, 'comment')
        ]
    )

    # Insert sample suppliers, customers, orders, lineitems
    # (Simplified data loading - real implementation would use data_generator.py)
    for i in range(100):
        cursor.execute(
            "INSERT INTO supplier VALUES (?, ?, ?, ?, ?, ?, ?)",
            (i, f'Supplier#{i}', f'Address{i}', i % 5, f'Phone{i}', 1000.0, 'comment')
        )

        cursor.execute(
            "INSERT INTO customer VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (i, f'Customer#{i}', f'Address{i}', i % 5, f'Phone{i}', 1000.0, 'BUILDING', 'comment')
        )

        cursor.execute(
            "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (i, i, 'O', 1000.0, '1995-01-01', '1-URGENT', f'Clerk{i}', 0, 'comment')
        )

        cursor.execute(
            "INSERT INTO lineitem VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (i, i*100, i, 1, 10.0, 100.0, 0.1, 0.0, 'N', 'O', '1995-01-01', '1995-01-01', '1995-01-01', 'DELIVER IN PERSON', 'TRUCK', 'comment')
        )
