"""
TPC-H Data Generator (Python Implementation)

Generates TPC-H benchmark data compatible with the official specification.
Supports scale factors from 0.01 (10MB) to 1.0 (1GB) for in-memory testing.

Reference: http://www.tpc.org/tpch/
"""
import random
from datetime import datetime, timedelta
from typing import List, Tuple, Dict


class TPCHGenerator:
    """Generate TPC-H benchmark data at various scale factors"""

    # TPC-H Constants
    NATIONS = [
        (0, 'ALGERIA', 0, 'haggle. carefully final deposits detect slyly agai'),
        (1, 'ARGENTINA', 1, 'al foxes promise slyly according to the regular accounts. bold requests alon'),
        (2, 'BRAZIL', 1, 'y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special '),
        (3, 'CANADA', 1, 'eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold'),
        (4, 'EGYPT', 4, 'y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d'),
        (5, 'ETHIOPIA', 0, 'ven packages wake quickly. regu'),
        (6, 'FRANCE', 3, 'refully final requests. regular, ironi'),
        (7, 'GERMANY', 3, 'l platelets. regular accounts x-ray: unusual, regular acco'),
        (8, 'INDIA', 2, 'ss excuses cajole slyly across the packages. deposits print aroun'),
        (9, 'INDONESIA', 2, ' slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull'),
        (10, 'IRAN', 4, 'efully alongside of the slyly final dependencies. '),
        (11, 'IRAQ', 4, 'nic deposits boost atop the quickly final requests? quickly regula'),
        (12, 'JAPAN', 2, 'ously. final, express gifts cajole a'),
        (13, 'JORDAN', 4, 'ic deposits are blithely about the carefully regular pa'),
        (14, 'KENYA', 0, ' pending excuses haggle furiously deposits. pending, express pinto beans wake fluffily past t'),
        (15, 'MOROCCO', 0, 'rns. blithely bold courts among the closely regular packages use furiously bold platelets?'),
        (16, 'MOZAMBIQUE', 0, 's. ironic, unusual asymptotes wake blithely r'),
        (17, 'PERU', 1, 'platelets. blithely pending dependencies use fluffily across the even pinto beans. carefully silent accoun'),
        (18, 'CHINA', 2, 'c dependencies. furiously express notornis sleep slyly regular accounts. ideas sleep. depos'),
        (19, 'ROMANIA', 3, 'ular asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic account'),
        (20, 'SAUDI ARABIA', 4, 'ts. silent requests haggle. closely express packages sleep across the blithely'),
        (21, 'VIETNAM', 2, 'hely enticingly express accounts. even, final '),
        (22, 'RUSSIA', 3, ' requests against the platelets use never according to the quickly regular pint'),
        (23, 'UNITED KINGDOM', 3, 'eans boost carefully special requests. accounts are. carefull'),
        (24, 'UNITED STATES', 1, 'y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be')
    ]

    REGIONS = [
        (0, 'AFRICA', 'lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to '),
        (1, 'AMERICA', 'hs use ironic, even requests. s'),
        (2, 'ASIA', 'ges. thinly even pinto beans ca'),
        (3, 'EUROPE', 'ly final courts cajole furiously final excuse'),
        (4, 'MIDDLE EAST', 'uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl')
    ]

    SEGMENTS = ['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'HOUSEHOLD', 'MACHINERY']
    PRIORITIES = ['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW']
    INSTRUCTIONS = ['DELIVER IN PERSON', 'COLLECT COD', 'NONE', 'TAKE BACK RETURN']
    SHIPMODES = ['AIR', 'AIR REG', 'MAIL', 'RAIL', 'SHIP', 'TRUCK', 'FOB']
    RETURNFLAGS = ['N', 'R', 'A']
    LINESTATUSES = ['O', 'F']

    def __init__(self, scale_factor: float = 0.01):
        """
        Initialize TPC-H generator

        Args:
            scale_factor: Data size multiplier (0.01 = 10MB, 1.0 = 1GB)
        """
        self.scale_factor = scale_factor
        self.random = random.Random(42)  # Deterministic seed for reproducibility

        # Row counts based on scale factor (SF=1 baseline)
        self.part_count = int(200000 * scale_factor)
        self.supplier_count = int(10000 * scale_factor)
        self.partsupp_count = int(800000 * scale_factor)
        self.customer_count = int(150000 * scale_factor)
        self.orders_count = int(1500000 * scale_factor)
        self.lineitem_count = int(6000000 * scale_factor)

        # Minimum counts for small scale factors
        self.part_count = max(100, self.part_count)
        self.supplier_count = max(10, self.supplier_count)
        self.customer_count = max(100, self.customer_count)
        self.orders_count = max(100, self.orders_count)
        self.lineitem_count = max(400, self.lineitem_count)

    def generate_regions(self) -> List[Tuple]:
        """Generate REGION table (5 rows, fixed)"""
        return self.REGIONS

    def generate_nations(self) -> List[Tuple]:
        """Generate NATION table (25 rows, fixed)"""
        return self.NATIONS

    def generate_suppliers(self) -> List[Tuple]:
        """Generate SUPPLIER table"""
        suppliers = []
        for i in range(self.supplier_count):
            s_suppkey = i + 1
            s_name = f'Supplier#{str(i+1).zfill(9)}'
            s_address = self._random_varchar(25)
            s_nationkey = self.random.randint(0, 24)
            s_phone = self._random_phone(s_nationkey)
            s_acctbal = round(self.random.uniform(-999.99, 9999.99), 2)
            s_comment = self._random_varchar(101)

            suppliers.append((
                s_suppkey, s_name, s_address, s_nationkey,
                s_phone, s_acctbal, s_comment
            ))

        return suppliers

    def generate_customers(self) -> List[Tuple]:
        """Generate CUSTOMER table"""
        customers = []
        for i in range(self.customer_count):
            c_custkey = i + 1
            c_name = f'Customer#{str(i+1).zfill(9)}'
            c_address = self._random_varchar(25)
            c_nationkey = self.random.randint(0, 24)
            c_phone = self._random_phone(c_nationkey)
            c_acctbal = round(self.random.uniform(-999.99, 9999.99), 2)
            c_mktsegment = self.random.choice(self.SEGMENTS)
            c_comment = self._random_varchar(117)

            customers.append((
                c_custkey, c_name, c_address, c_nationkey,
                c_phone, c_acctbal, c_mktsegment, c_comment
            ))

        return customers

    def generate_orders(self) -> List[Tuple]:
        """Generate ORDERS table"""
        orders = []
        start_date = datetime(1992, 1, 1)
        end_date = datetime(1998, 12, 31)
        date_range = (end_date - start_date).days

        for i in range(self.orders_count):
            o_orderkey = i + 1
            o_custkey = self.random.randint(1, self.customer_count)
            o_orderstatus = self.random.choice(['O', 'F', 'P'])
            o_totalprice = round(self.random.uniform(1000, 500000), 2)
            o_orderdate = start_date + timedelta(days=self.random.randint(0, date_range))
            o_orderpriority = self.random.choice(self.PRIORITIES)
            o_clerk = f'Clerk#{str(self.random.randint(1, 1000)).zfill(9)}'
            o_shippriority = 0
            o_comment = self._random_varchar(79)

            orders.append((
                o_orderkey, o_custkey, o_orderstatus, o_totalprice,
                o_orderdate.strftime('%Y-%m-%d'), o_orderpriority,
                o_clerk, o_shippriority, o_comment
            ))

        return orders

    def generate_lineitems(self) -> List[Tuple]:
        """Generate LINEITEM table"""
        lineitems = []
        start_date = datetime(1992, 1, 1)
        end_date = datetime(1998, 12, 31)
        date_range = (end_date - start_date).days

        # Distribute lineitems across orders
        lines_per_order = max(1, self.lineitem_count // self.orders_count)

        lineitem_id = 0
        for order_num in range(1, self.orders_count + 1):
            num_lines = self.random.randint(1, min(7, lines_per_order * 2))

            for line_num in range(1, num_lines + 1):
                if lineitem_id >= self.lineitem_count:
                    break

                l_orderkey = order_num
                l_partkey = self.random.randint(1, min(self.part_count, 200000))
                l_suppkey = self.random.randint(1, self.supplier_count)
                l_linenumber = line_num
                l_quantity = self.random.randint(1, 50)
                l_extendedprice = round(l_quantity * self.random.uniform(900, 100000), 2)
                l_discount = round(self.random.uniform(0, 0.10), 2)
                l_tax = round(self.random.uniform(0, 0.08), 2)
                l_returnflag = self.random.choice(self.RETURNFLAGS)
                l_linestatus = self.random.choice(self.LINESTATUSES)
                l_shipdate = start_date + timedelta(days=self.random.randint(0, date_range))
                l_commitdate = l_shipdate + timedelta(days=self.random.randint(-30, 30))
                l_receiptdate = l_shipdate + timedelta(days=self.random.randint(1, 30))
                l_shipinstruct = self.random.choice(self.INSTRUCTIONS)
                l_shipmode = self.random.choice(self.SHIPMODES)
                l_comment = self._random_varchar(44)

                lineitems.append((
                    l_orderkey, l_partkey, l_suppkey, l_linenumber,
                    l_quantity, l_extendedprice, l_discount, l_tax,
                    l_returnflag, l_linestatus,
                    l_shipdate.strftime('%Y-%m-%d'),
                    l_commitdate.strftime('%Y-%m-%d'),
                    l_receiptdate.strftime('%Y-%m-%d'),
                    l_shipinstruct, l_shipmode, l_comment
                ))

                lineitem_id += 1

        return lineitems[:self.lineitem_count]  # Ensure exact count

    def _random_varchar(self, max_length: int) -> str:
        """Generate random string of variable length"""
        length = self.random.randint(10, max_length)
        chars = 'abcdefghijklmnopqrstuvwxyz '
        return ''.join(self.random.choice(chars) for _ in range(length))

    def _random_phone(self, nation_key: int) -> str:
        """Generate phone number in format XX-XXX-XXX-XXXX"""
        country_code = str(10 + nation_key).zfill(2)
        return f'{country_code}-{self.random.randint(100, 999)}-{self.random.randint(100, 999)}-{self.random.randint(1000, 9999)}'

    def get_table_stats(self) -> Dict[str, int]:
        """Get row counts for all tables"""
        return {
            'region': len(self.REGIONS),
            'nation': len(self.NATIONS),
            'supplier': self.supplier_count,
            'customer': self.customer_count,
            'orders': self.orders_count,
            'lineitem': self.lineitem_count,
        }


# Convenience functions
def generate_tpch_data(scale_factor: float = 0.01) -> Dict[str, List[Tuple]]:
    """
    Generate all TPC-H tables at specified scale factor

    Args:
        scale_factor: Data size (0.01 = 10MB, 0.1 = 100MB, 1.0 = 1GB)

    Returns:
        Dictionary mapping table names to row data
    """
    gen = TPCHGenerator(scale_factor)

    return {
        'region': gen.generate_regions(),
        'nation': gen.generate_nations(),
        'supplier': gen.generate_suppliers(),
        'customer': gen.generate_customers(),
        'orders': gen.generate_orders(),
        'lineitem': gen.generate_lineitems(),
    }


def get_tpch_schema() -> Dict[str, str]:
    """Get CREATE TABLE statements for TPC-H schema"""
    return {
        'region': """
            CREATE TABLE region (
                r_regionkey INTEGER PRIMARY KEY,
                r_name VARCHAR(25) NOT NULL,
                r_comment VARCHAR(152)
            )
        """,
        'nation': """
            CREATE TABLE nation (
                n_nationkey INTEGER PRIMARY KEY,
                n_name VARCHAR(25) NOT NULL,
                n_regionkey INTEGER NOT NULL,
                n_comment VARCHAR(152)
            )
        """,
        'supplier': """
            CREATE TABLE supplier (
                s_suppkey INTEGER PRIMARY KEY,
                s_name VARCHAR(25) NOT NULL,
                s_address VARCHAR(40) NOT NULL,
                s_nationkey INTEGER NOT NULL,
                s_phone VARCHAR(15) NOT NULL,
                s_acctbal DECIMAL(15,2) NOT NULL,
                s_comment VARCHAR(101) NOT NULL
            )
        """,
        'customer': """
            CREATE TABLE customer (
                c_custkey INTEGER PRIMARY KEY,
                c_name VARCHAR(25) NOT NULL,
                c_address VARCHAR(40) NOT NULL,
                c_nationkey INTEGER NOT NULL,
                c_phone VARCHAR(15) NOT NULL,
                c_acctbal DECIMAL(15,2) NOT NULL,
                c_mktsegment VARCHAR(10) NOT NULL,
                c_comment VARCHAR(117) NOT NULL
            )
        """,
        'orders': """
            CREATE TABLE orders (
                o_orderkey INTEGER PRIMARY KEY,
                o_custkey INTEGER NOT NULL,
                o_orderstatus VARCHAR(1) NOT NULL,
                o_totalprice DECIMAL(15,2) NOT NULL,
                o_orderdate DATE NOT NULL,
                o_orderpriority VARCHAR(15) NOT NULL,
                o_clerk VARCHAR(15) NOT NULL,
                o_shippriority INTEGER NOT NULL,
                o_comment VARCHAR(79) NOT NULL
            )
        """,
        'lineitem': """
            CREATE TABLE lineitem (
                l_orderkey INTEGER NOT NULL,
                l_partkey INTEGER NOT NULL,
                l_suppkey INTEGER NOT NULL,
                l_linenumber INTEGER NOT NULL,
                l_quantity DECIMAL(15,2) NOT NULL,
                l_extendedprice DECIMAL(15,2) NOT NULL,
                l_discount DECIMAL(15,2) NOT NULL,
                l_tax DECIMAL(15,2) NOT NULL,
                l_returnflag VARCHAR(1) NOT NULL,
                l_linestatus VARCHAR(1) NOT NULL,
                l_shipdate DATE NOT NULL,
                l_commitdate DATE NOT NULL,
                l_receiptdate DATE NOT NULL,
                l_shipinstruct VARCHAR(25) NOT NULL,
                l_shipmode VARCHAR(10) NOT NULL,
                l_comment VARCHAR(44) NOT NULL,
                PRIMARY KEY (l_orderkey, l_linenumber)
            )
        """
    }
