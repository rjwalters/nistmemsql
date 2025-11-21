//! TPC-H Query Definitions
//!
//! This module contains all 22 TPC-H benchmark queries as string constants.
//! Each query is documented with its name and purpose.

// TPC-H Q1: Pricing Summary Report
pub const TPCH_Q1: &str = r#"
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) as sum_qty,
    SUM(l_extendedprice) as sum_base_price,
    SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    AVG(l_quantity) as avg_qty,
    AVG(l_extendedprice) as avg_price,
    AVG(l_discount) as avg_disc,
    COUNT(*) as count_order
FROM lineitem
WHERE l_shipdate <= '1998-09-01'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
"#;

// TPC-H Q2: Minimum Cost Supplier
pub const TPCH_Q2: &str = r#"
SELECT
    s_acctbal,
    s_name,
    n_name,
    s_address,
    s_phone,
    s_comment
FROM supplier, nation, region
WHERE s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
ORDER BY s_acctbal DESC
LIMIT 100
"#;

// TPC-H Q3: Shipping Priority
pub const TPCH_Q3: &str = r#"
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
ORDER BY revenue DESC, o_orderdate
LIMIT 10
"#;

// TPC-H Q4: Order Priority Checking
pub const TPCH_Q4: &str = r#"
SELECT
    o_orderpriority,
    COUNT(*) as order_count
FROM orders
WHERE o_orderdate >= '1993-07-01'
    AND o_orderdate < '1993-10-01'
    AND EXISTS (
        SELECT *
        FROM lineitem
        WHERE l_orderkey = o_orderkey
            AND l_commitdate < l_receiptdate
    )
GROUP BY o_orderpriority
ORDER BY o_orderpriority
"#;

// TPC-H Q5: Local Supplier Volume
pub const TPCH_Q5: &str = r#"
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
ORDER BY revenue DESC
"#;

// TPC-H Q6: Forecasting Revenue Change
pub const TPCH_Q6: &str = r#"
SELECT
    SUM(l_extendedprice * l_discount) as revenue
FROM lineitem
WHERE
    l_shipdate >= '1994-01-01'
    AND l_shipdate < '1995-01-01'
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_quantity < 24
"#;

// TPC-H Q7: Volume Shipping
pub const TPCH_Q7: &str = r#"
SELECT
    n1.n_name as supp_nation,
    n2.n_name as cust_nation,
    SUBSTR(l_shipdate, 1, 4) as l_year,
    SUM(l_extendedprice * (1 - l_discount)) as revenue
FROM supplier, lineitem, orders, customer, nation n1, nation n2
WHERE s_suppkey = l_suppkey
    AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey
    AND c_nationkey = n2.n_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
         OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
    AND l_shipdate >= '1995-01-01'
    AND l_shipdate <= '1996-12-31'
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year
"#;

// TPC-H Q8: National Market Share (8-way join)
pub const TPCH_Q8: &str = r#"
SELECT
    SUBSTR(o_orderdate, 1, 4) as o_year,
    SUM(CASE WHEN n2.n_name = 'BRAZIL'
        THEN l_extendedprice * (1 - l_discount)
        ELSE 0 END) / SUM(l_extendedprice * (1 - l_discount)) as mkt_share
FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
WHERE p_partkey = l_partkey
    AND s_suppkey = l_suppkey
    AND l_orderkey = o_orderkey
    AND o_custkey = c_custkey
    AND c_nationkey = n1.n_nationkey
    AND n1.n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND s_nationkey = n2.n_nationkey
    AND o_orderdate >= '1995-01-01'
    AND o_orderdate <= '1996-12-31'
    AND p_type = 'ECONOMY ANODIZED STEEL'
GROUP BY o_year
ORDER BY o_year
"#;

// TPC-H Q9: Product Type Profit Measure
pub const TPCH_Q9: &str = r#"
SELECT
    n_name as nation,
    SUBSTR(o_orderdate, 1, 4) as o_year,
    SUM(l_extendedprice * (1 - l_discount)) as sum_profit
FROM lineitem, orders, nation, supplier
WHERE l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND s_nationkey = n_nationkey
GROUP BY nation, o_year
ORDER BY nation, o_year DESC
"#;

// TPC-H Q10: Returned Item Reporting
pub const TPCH_Q10: &str = r#"
SELECT
    c_custkey,
    c_name,
    SUM(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM customer, orders, lineitem, nation
WHERE c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate >= '1993-10-01'
    AND o_orderdate < '1994-01-01'
    AND l_returnflag = 'R'
    AND c_nationkey = n_nationkey
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue DESC
LIMIT 20
"#;

// TPC-H Q11: Important Stock Identification
pub const TPCH_Q11: &str = r#"
SELECT
    s_suppkey,
    SUM(s_acctbal) as total_value
FROM supplier, nation
WHERE s_nationkey = n_nationkey
    AND n_name = 'GERMANY'
GROUP BY s_suppkey
HAVING SUM(s_acctbal) > (
    SELECT SUM(s_acctbal) * 0.0001
    FROM supplier, nation
    WHERE s_nationkey = n_nationkey
        AND n_name = 'GERMANY'
)
ORDER BY total_value DESC
"#;

// TPC-H Q12: Shipping Modes and Order Priority
pub const TPCH_Q12: &str = r#"
SELECT
    l_shipmode,
    SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH'
        THEN 1 ELSE 0 END) as high_line_count,
    SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH'
        THEN 1 ELSE 0 END) as low_line_count
FROM orders, lineitem
WHERE o_orderkey = l_orderkey
    AND l_shipmode IN ('MAIL', 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= '1994-01-01'
    AND l_receiptdate < '1995-01-01'
GROUP BY l_shipmode
ORDER BY l_shipmode
"#;

// TPC-H Q13: Customer Distribution
pub const TPCH_Q13: &str = r#"
SELECT
    c_count,
    COUNT(*) as custdist
FROM (
    SELECT
        c_custkey,
        COUNT(o_orderkey) as c_count
    FROM customer
    LEFT OUTER JOIN orders ON c_custkey = o_custkey
        AND o_comment NOT LIKE '%special%requests%'
    GROUP BY c_custkey
) c_orders
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC
"#;

// TPC-H Q14: Promotion Effect
pub const TPCH_Q14: &str = r#"
SELECT
    100.00 * SUM(CASE WHEN l_shipdate >= '1995-09-01' AND l_shipdate < '1995-10-01'
        THEN l_extendedprice * (1 - l_discount)
        ELSE 0 END) / SUM(l_extendedprice * (1 - l_discount)) as promo_revenue
FROM lineitem
WHERE l_shipdate >= '1995-09-01'
    AND l_shipdate < '1995-10-01'
"#;

// TPC-H Q15: Top Supplier
pub const TPCH_Q15: &str = r#"
SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM supplier, (
    SELECT
        l_suppkey as supplier_no,
        SUM(l_extendedprice * (1 - l_discount)) as total_revenue
    FROM lineitem
    WHERE l_shipdate >= '1996-01-01'
        AND l_shipdate < '1996-04-01'
    GROUP BY l_suppkey
) revenue
WHERE s_suppkey = supplier_no
    AND total_revenue = (
        SELECT MAX(total_revenue)
        FROM (
            SELECT SUM(l_extendedprice * (1 - l_discount)) as total_revenue
            FROM lineitem
            WHERE l_shipdate >= '1996-01-01'
                AND l_shipdate < '1996-04-01'
            GROUP BY l_suppkey
        ) max_revenue
    )
ORDER BY s_suppkey
"#;

// TPC-H Q16: Parts/Supplier Relationship
pub const TPCH_Q16: &str = r#"
SELECT
    COUNT(DISTINCT s_suppkey) as supplier_cnt
FROM supplier
WHERE s_suppkey NOT IN (
    SELECT s_suppkey
    FROM supplier
    WHERE s_comment LIKE '%Customer%Complaints%'
)
GROUP BY s_nationkey
ORDER BY supplier_cnt DESC, s_nationkey
LIMIT 1
"#;

// TPC-H Q17: Small-Quantity-Order Revenue
pub const TPCH_Q17: &str = r#"
SELECT
    SUM(l_extendedprice) / 7.0 as avg_yearly
FROM lineitem
WHERE l_quantity < (
    SELECT 0.2 * AVG(l_quantity)
    FROM lineitem
)
"#;

// TPC-H Q18: Large Volume Customer
pub const TPCH_Q18: &str = r#"
SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    SUM(l_quantity) as total_qty
FROM customer, orders, lineitem
WHERE c_custkey = o_custkey
    AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
HAVING SUM(l_quantity) > 300
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100
"#;

// TPC-H Q19: Discounted Revenue
pub const TPCH_Q19: &str = r#"
SELECT
    SUM(l_extendedprice * (1 - l_discount)) as revenue
FROM lineitem
WHERE l_quantity >= 1
    AND l_quantity <= 30
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
"#;

// TPC-H Q20: Potential Part Promotion (Full triple-nested IN subquery)
// Identifies suppliers in a nation with excess inventory of parts matching a name pattern
pub const TPCH_Q20: &str = r#"
SELECT
    s_name,
    s_address
FROM supplier, nation
WHERE s_suppkey IN (
    SELECT ps_suppkey
    FROM partsupp
    WHERE ps_partkey IN (
        SELECT p_partkey
        FROM part
        WHERE p_name LIKE 'forest%'
    )
    AND ps_availqty > (
        SELECT 0.5 * SUM(l_quantity)
        FROM lineitem
        WHERE l_partkey = ps_partkey
            AND l_suppkey = ps_suppkey
            AND l_shipdate >= '1994-01-01'
            AND l_shipdate < '1995-01-01'
    )
)
    AND s_nationkey = n_nationkey
    AND n_name = 'CANADA'
ORDER BY s_name
"#;

// TPC-H Q21: Suppliers Who Kept Orders Waiting
pub const TPCH_Q21: &str = r#"
SELECT
    s_name,
    COUNT(*) as numwait
FROM supplier, lineitem l1, orders, nation
WHERE s_suppkey = l1.l_suppkey
    AND o_orderkey = l1.l_orderkey
    AND o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS (
        SELECT *
        FROM lineitem l2
        WHERE l2.l_orderkey = l1.l_orderkey
            AND l2.l_suppkey <> l1.l_suppkey
    )
    AND s_nationkey = n_nationkey
    AND n_name = 'SAUDI ARABIA'
GROUP BY s_name
ORDER BY numwait DESC, s_name
LIMIT 100
"#;

// TPC-H Q22: Global Sales Opportunity
pub const TPCH_Q22: &str = r#"
SELECT
    SUBSTR(c_phone, 1, 2) as cntrycode,
    COUNT(*) as numcust,
    SUM(c_acctbal) as totacctbal
FROM customer
WHERE SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
    AND c_acctbal > (
        SELECT AVG(c_acctbal)
        FROM customer
        WHERE c_acctbal > 0.00
            AND SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
    )
    AND NOT EXISTS (
        SELECT *
        FROM orders
        WHERE o_custkey = c_custkey
    )
GROUP BY cntrycode
ORDER BY cntrycode
"#;
