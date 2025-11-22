//! Tests for TPC-H Q18: Large Volume Customer (Issue #2312)
//!
//! Tests IN subquery with GROUP BY HAVING, which requires
//! subquery flattening to semi-join for optimization.

use vibesql_executor::SelectExecutor;

/// Setup a simplified TPC-H schema for Q18 testing
fn setup_q18_database() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();

    // Customer table (simplified - just custkey)
    let customer_schema = vibesql_catalog::TableSchema::new(
        "CUSTOMER".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "C_CUSTKEY".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(customer_schema).unwrap();

    // Orders table (simplified)
    let orders_schema = vibesql_catalog::TableSchema::new(
        "ORDERS".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "O_ORDERKEY".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "O_CUSTKEY".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "O_TOTALPRICE".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(orders_schema).unwrap();

    // Lineitem table
    let lineitem_schema = vibesql_catalog::TableSchema::new(
        "LINEITEM".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "L_ORDERKEY".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "L_QUANTITY".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(lineitem_schema).unwrap();

    // Insert test data
    // Customers
    for custkey in [1, 2, 3] {
        db.insert_row(
            "CUSTOMER",
            vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(custkey)]),
        )
        .unwrap();
    }

    // Orders: Order 100 -> Customer 1, Order 200 -> Customer 2, Order 300 -> Customer 3
    db.insert_row(
        "ORDERS",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(100),
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(1000),
        ]),
    )
    .unwrap();
    db.insert_row(
        "ORDERS",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(200),
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(2000),
        ]),
    )
    .unwrap();
    db.insert_row(
        "ORDERS",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(300),
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(3000),
        ]),
    )
    .unwrap();

    // Lineitem: Order 100 total=150, Order 200 total=350, Order 300 total=400
    // Order 100: below threshold (150 < 300)
    for _ in 0..3 {
        db.insert_row(
            "LINEITEM",
            vibesql_storage::Row::new(vec![
                vibesql_types::SqlValue::Integer(100),
                vibesql_types::SqlValue::Integer(50),
            ]),
        )
        .unwrap();
    }

    // Order 200: above threshold (350 > 300)
    db.insert_row(
        "LINEITEM",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(200),
            vibesql_types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "LINEITEM",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(200),
            vibesql_types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "LINEITEM",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(200),
            vibesql_types::SqlValue::Integer(150),
        ]),
    )
    .unwrap();

    // Order 300: above threshold (400 > 300)
    db.insert_row(
        "LINEITEM",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(300),
            vibesql_types::SqlValue::Integer(200),
        ]),
    )
    .unwrap();
    db.insert_row(
        "LINEITEM",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(300),
            vibesql_types::SqlValue::Integer(200),
        ]),
    )
    .unwrap();

    db
}

#[test]
fn test_in_subquery_with_group_by_having() {
    let db = setup_q18_database();
    let executor = SelectExecutor::new(&db);

    // Simplified Q18: IN subquery with GROUP BY HAVING
    let sql = "SELECT o_orderkey, SUM(l_quantity) as total_qty \
        FROM orders, lineitem \
        WHERE o_orderkey = l_orderkey \
            AND o_orderkey IN ( \
                SELECT l_orderkey \
                FROM lineitem \
                GROUP BY l_orderkey \
                HAVING SUM(l_quantity) > 300 \
            ) \
        GROUP BY o_orderkey \
        ORDER BY o_orderkey";

    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // Should return orders 200 and 300 (total qty > 300)
    assert_eq!(rows.len(), 2, "Expected 2 orders with total qty > 300");

    // Verify order 200 (total=350) is included
    assert_eq!(rows[0].get(0), Some(&vibesql_types::SqlValue::Integer(200)));
    assert_eq!(rows[0].get(1), Some(&vibesql_types::SqlValue::Integer(350)));

    // Verify order 300 (total=400) is included
    assert_eq!(rows[1].get(0), Some(&vibesql_types::SqlValue::Integer(300)));
    assert_eq!(rows[1].get(1), Some(&vibesql_types::SqlValue::Integer(400)));
}

#[test]
fn test_subquery_with_group_by_having_alone() {
    let db = setup_q18_database();
    let executor = SelectExecutor::new(&db);

    // Test just the subquery part
    let sql = "SELECT l_orderkey \
        FROM lineitem \
        GROUP BY l_orderkey \
        HAVING SUM(l_quantity) > 300 \
        ORDER BY l_orderkey";

    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // Should return orders 200 and 300
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get(0), Some(&vibesql_types::SqlValue::Integer(200)));
    assert_eq!(rows[1].get(0), Some(&vibesql_types::SqlValue::Integer(300)));
}

#[test]
fn test_full_tpch_q18_pattern() {
    let db = setup_q18_database();
    let executor = SelectExecutor::new(&db);

    // Simplified TPC-H Q18 pattern with 3-way join
    let sql = "SELECT \
            c_custkey, \
            o_orderkey, \
            o_totalprice, \
            SUM(l_quantity) as total_qty \
        FROM customer, orders, lineitem \
        WHERE c_custkey = o_custkey \
            AND o_orderkey = l_orderkey \
            AND o_orderkey IN ( \
                SELECT l_orderkey \
                FROM lineitem \
                GROUP BY l_orderkey \
                HAVING SUM(l_quantity) > 300 \
            ) \
        GROUP BY c_custkey, o_orderkey, o_totalprice \
        ORDER BY o_totalprice DESC";

    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // Should return 2 customers with large volume orders
    assert_eq!(rows.len(), 2, "Expected 2 large volume customer orders");

    // First row should be Customer 3 (order 300, totalprice 3000)
    assert_eq!(rows[0].get(0), Some(&vibesql_types::SqlValue::Integer(3)));
    assert_eq!(rows[0].get(1), Some(&vibesql_types::SqlValue::Integer(300)));

    // Second row should be Customer 2 (order 200, totalprice 2000)
    assert_eq!(rows[1].get(0), Some(&vibesql_types::SqlValue::Integer(2)));
    assert_eq!(rows[1].get(1), Some(&vibesql_types::SqlValue::Integer(200)));
}
