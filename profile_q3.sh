#!/bin/bash
# Profile TPC-H Q3 execution

# Run with JOIN_REORDER_VERBOSE=1 to see join ordering
JOIN_REORDER_VERBOSE=1 cargo run --release --package vibesql-executor --example run_query <<'EOF'
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
EOF
