"""
Data generation utilities for benchmark tests.

Provides functions to generate synthetic datasets of various sizes
and complexity for performance testing.
"""
import random
import string


def generate_customer_data(num_customers, scale_factor=1.0):
    """
    Generate TPC-H style customer data.

    Args:
        num_customers: Number of customers to generate
        scale_factor: Scale factor for data volume

    Returns:
        List of tuples: [(custkey, name, address, nationkey, phone, acctbal, mktsegment, comment), ...]
    """
    customers = []

    for i in range(num_customers):
        custkey = i
        name = f'Customer#{custkey}'
        address = generate_random_string(25)
        nationkey = random.randint(0, 24)  # 25 nations
        phone = generate_phone_number(nationkey)
        acctbal = round(random.uniform(-999.99, 9999.99), 2)
        mktsegment = random.choice(['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'HOUSEHOLD', 'MACHINERY'])
        comment = generate_random_string(73)

        customers.append((custkey, name, address, nationkey, phone, acctbal, mktsegment, comment))

    return customers


def generate_orders_data(num_orders, max_customer_key, scale_factor=1.0):
    """
    Generate TPC-H style orders data.

    Args:
        num_orders: Number of orders to generate
        max_customer_key: Maximum customer key for foreign key references
        scale_factor: Scale factor for data volume

    Returns:
        List of tuples: [(orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment), ...]
    """
    orders = []
    start_date = "1992-01-01"
    end_date = "1998-12-31"

    for i in range(num_orders):
        orderkey = i
        custkey = random.randint(0, max_customer_key - 1)
        orderstatus = random.choice(['O', 'F', 'P'])  # Open, Fulfilled, Pending
        totalprice = round(random.uniform(100.00, 100000.00), 2)
        orderdate = generate_random_date(start_date, end_date)
        orderpriority = random.choice(['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW'])
        clerk = f'Clerk#{random.randint(1, 1000):09d}'
        shippriority = random.randint(0, 5)
        comment = generate_random_string(49)

        orders.append((orderkey, custkey, orderstatus, totalprice, orderdate,
                      orderpriority, clerk, shippriority, comment))

    return orders


def generate_lineitem_data(num_lineitems, max_order_key, max_part_key, scale_factor=1.0):
    """
    Generate TPC-H style lineitem data.

    Args:
        num_lineitems: Number of lineitems to generate
        max_order_key: Maximum order key for foreign key references
        max_part_key: Maximum part key for foreign key references
        scale_factor: Scale factor for data volume

    Returns:
        List of tuples: [(orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, returnflag, linestatus, shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment), ...]
    """
    lineitems = []

    for i in range(num_lineitems):
        orderkey = random.randint(0, max_order_key - 1)
        partkey = random.randint(0, max_part_key - 1)
        suppkey = random.randint(0, 9999)  # Assuming 10k suppliers
        linenumber = random.randint(1, 7)   # 1-7 lineitems per order typically
        quantity = random.randint(1, 50)
        extendedprice = round(random.uniform(100.00, 100000.00), 2)
        discount = round(random.uniform(0.00, 0.10), 2)
        tax = round(random.uniform(0.00, 0.08), 2)
        returnflag = random.choice(['A', 'N', 'R'])  # Advance, Normal, Return
        linestatus = random.choice(['F', 'O'])       # Fulfilled, Open
        shipdate = generate_random_date("1992-01-01", "1998-12-31")
        commitdate = generate_random_date("1992-01-01", "1998-12-31")
        receiptdate = generate_random_date(shipdate, "1998-12-31")
        shipinstruct = random.choice(['DELIVER IN PERSON', 'COLLECT COD', 'NONE', 'TAKE BACK RETURN'])
        shipmode = random.choice(['REG AIR', 'AIR', 'RAIL', 'TRUCK', 'MAIL', 'FOIL', 'SHIP'])
        comment = generate_random_string(27)

        lineitems.append((orderkey, partkey, suppkey, linenumber, quantity,
                         extendedprice, discount, tax, returnflag, linestatus,
                         shipdate, commitdate, receiptdate, shipinstruct,
                         shipmode, comment))

    return lineitems


def generate_micro_benchmark_data(num_rows, schema='simple'):
    """
    Generate data for micro-benchmarks.

    Args:
        num_rows: Number of rows to generate
        schema: Schema type ('simple', 'complex', 'wide')

    Returns:
        List of tuples appropriate for the schema
    """
    if schema == 'simple':
        return [(i, f'name_{i}', i * 10, float(i) / 100.0) for i in range(num_rows)]
    elif schema == 'complex':
        return [(i, f'name_{i}', i * 10, float(i) / 100.0, generate_random_string(50),
                random.choice(['A', 'B', 'C']), random.randint(1, 100)) for i in range(num_rows)]
    elif schema == 'wide':
        return [(i, f'name_{i}', i * 10, float(i) / 100.0, generate_random_string(100),
                generate_random_string(100), generate_random_string(100),
                random.randint(1, 1000), random.randint(1, 1000)) for i in range(num_rows)]
    else:
        raise ValueError(f"Unknown schema: {schema}")


def generate_random_string(length):
    """Generate a random string of given length."""
    return ''.join(random.choices(string.ascii_letters + string.digits + ' ', k=length))


def generate_phone_number(nation_key):
    """Generate a phone number based on nation key."""
    # Simplified phone number generation
    area_code = f"{nation_key:02d}"
    exchange = f"{random.randint(100, 999)}"
    number = f"{random.randint(1000, 9999)}"
    return f"{area_code}-{exchange}-{number}"


def generate_random_date(start_date, end_date):
    """Generate a random date between start_date and end_date."""
    from datetime import datetime, timedelta

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    delta = end - start
    random_days = random.randint(0, delta.days)
    random_date = start + timedelta(days=random_days)

    return random_date.strftime("%Y-%m-%d")


def create_tpch_database(connection, scale_factor=0.01):
    """
    Create and populate a complete TPC-H database.

    Args:
        connection: Database connection
        scale_factor: TPC-H scale factor (0.01 = 10MB, 1.0 = 1GB)
    """
    cursor = connection.cursor()

    # Calculate data sizes based on scale factor
    num_customers = int(150000 * scale_factor)
    num_orders = int(1500000 * scale_factor)
    num_lineitems = int(6000000 * scale_factor)

    # Create tables
    create_tpch_tables(cursor)

    # Generate and insert data
    print(f"Generating {num_customers} customers...")
    customers = generate_customer_data(num_customers, scale_factor)
    cursor.executemany("""
        INSERT INTO customer VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, customers)

    print(f"Generating {num_orders} orders...")
    orders = generate_orders_data(num_orders, num_customers, scale_factor)
    cursor.executemany("""
        INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, orders)

    print(f"Generating {num_lineitems} lineitems...")
    lineitems = generate_lineitem_data(num_lineitems, num_orders, int(200000 * scale_factor))
    cursor.executemany("""
        INSERT INTO lineitem VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, lineitems)

    connection.commit()
    print("TPC-H database created successfully!")


def create_tpch_tables(cursor):
    """Create TPC-H benchmark tables."""
    tables = [
        """
        CREATE TABLE region (
            r_regionkey INTEGER PRIMARY KEY,
            r_name TEXT,
            r_comment TEXT
        )
        """,
        """
        CREATE TABLE nation (
            n_nationkey INTEGER PRIMARY KEY,
            n_name TEXT,
            n_regionkey INTEGER,
            n_comment TEXT
        )
        """,
        """
        CREATE TABLE supplier (
            s_suppkey INTEGER PRIMARY KEY,
            s_name TEXT,
            s_address TEXT,
            s_nationkey INTEGER,
            s_phone TEXT,
            s_acctbal REAL,
            s_comment TEXT
        )
        """,
        """
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
        """,
        """
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
        """,
        """
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
        """
    ]

    for table_sql in tables:
        cursor.execute(table_sql)
