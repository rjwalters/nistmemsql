//! Predicate pushdown optimization tests (Phase 2)
//!
//! Tests for table-local predicate pushdown during table scans.
//! This ensures WHERE clause predicates are applied as early as possible
//! to reduce intermediate result sizes before JOINs.
//!
//! Note: Multi-table tests with table-qualified column references currently
//! have a limitation where predicates are applied twice (once during scan,
//! once after join), which requires proper handling in execute_without_aggregation.

use super::super::*;

#[test]
fn test_table_local_predicate_applied_at_scan() {
    // Verify that table-local predicates are applied during table scan,
    // not after all rows are retrieved
    let mut db = storage::Database::new();

    // Create table with 10 rows
    let schema = catalog::TableSchema::new(
        "t1".to_string(),
        vec![
            catalog::ColumnSchema::new("a".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("b".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert 10 rows
    for i in 0..10 {
        db.insert_row(
            "t1",
            storage::Row::new(vec![
                types::SqlValue::Integer(i),
                types::SqlValue::Integer(i * 10),
            ]),
        )
        .unwrap();
    }

    let executor = SelectExecutor::new(&db);

    // Query: SELECT * FROM t1 WHERE a = 5
    // Should return only 1 row
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        from: Some(ast::FromClause::Table {
            name: "t1".to_string(),
            alias: None,
        }),
        where_clause: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::ColumnRef {
                table: None,
                column: "a".to_string(),
            }),
            op: ast::BinaryOperator::Equal,
            right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(5))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should return exactly 1 row (a=5, b=50)
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(5));
    assert_eq!(result[0].values[1], types::SqlValue::Integer(50));
}

#[test]
#[ignore] // TODO: Requires execute_without_aggregation to only apply complex predicates
fn test_multi_table_with_local_predicates() {
    // Verify that table-local predicates reduce intermediate results
    // before Cartesian product in multi-table FROM
    let mut db = storage::Database::new();

    // Create three tables with 10 rows each
    for table_name in &["t1", "t2", "t3"] {
        let schema = catalog::TableSchema::new(
            table_name.to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("val".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();

        // Insert 10 rows per table
        for i in 0..10 {
            db.insert_row(
                table_name,
                storage::Row::new(vec![
                    types::SqlValue::Integer(i),
                    types::SqlValue::Integer(i * 100),
                ]),
            )
            .unwrap();
        }
    }

    let executor = SelectExecutor::new(&db);

    // Query: SELECT * FROM t1, t2, t3 WHERE t1.id = 5 AND t2.id = 7
    // Without pushdown: 10 × 10 × 10 = 1000 rows → filter → 1 row
    // With pushdown: 1 × 1 × 10 = 10 rows (much better!)
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        from: Some(ast::FromClause::Join {
            left: Box::new(ast::FromClause::Join {
                left: Box::new(ast::FromClause::Table {
                    name: "t1".to_string(),
                    alias: None,
                }),
                right: Box::new(ast::FromClause::Table {
                    name: "t2".to_string(),
                    alias: None,
                }),
                join_type: ast::JoinType::Inner,
                condition: None,
            }),
            right: Box::new(ast::FromClause::Table {
                name: "t3".to_string(),
                alias: None,
            }),
            join_type: ast::JoinType::Inner,
            condition: None,
        }),
        where_clause: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: Some("t1".to_string()),
                    column: "id".to_string(),
                }),
                op: ast::BinaryOperator::Equal,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(5))),
            }),
            op: ast::BinaryOperator::And,
            right: Box::new(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: Some("t2".to_string()),
                    column: "id".to_string(),
                }),
                op: ast::BinaryOperator::Equal,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(7))),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should return 10 rows (t1.id=5, t2.id=7, t3.id=0..9)
    assert_eq!(result.len(), 10);

    // Verify first row has correct values from t1 and t2
    assert_eq!(result[0].values[0], types::SqlValue::Integer(5)); // t1.id
    assert_eq!(result[0].values[2], types::SqlValue::Integer(7)); // t2.id
}

#[test]
#[ignore] // TODO: Requires execute_without_aggregation to only apply complex predicates
fn test_table_local_predicate_with_explicit_join() {
    // Test that table-local predicates work with explicit JOIN syntax
    let mut db = storage::Database::new();

    // Create two tables
    let schema1 = catalog::TableSchema::new(
        "orders".to_string(),
        vec![
            catalog::ColumnSchema::new("order_id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("customer_id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("amount".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema1).unwrap();

    let schema2 = catalog::TableSchema::new(
        "customers".to_string(),
        vec![
            catalog::ColumnSchema::new("customer_id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("name".to_string(), types::DataType::Varchar { max_length: Some(50) }, false),
        ],
    );
    db.create_table(schema2).unwrap();

    // Insert test data
    for i in 0..20 {
        db.insert_row(
            "orders",
            storage::Row::new(vec![
                types::SqlValue::Integer(i),
                types::SqlValue::Integer(i % 5), // customer_id 0-4
                types::SqlValue::Integer(100 + i),
            ]),
        )
        .unwrap();
    }

    for i in 0..5 {
        db.insert_row(
            "customers",
            storage::Row::new(vec![
                types::SqlValue::Integer(i),
                types::SqlValue::Varchar(format!("Customer{}", i)),
            ]),
        )
        .unwrap();
    }

    let executor = SelectExecutor::new(&db);

    // Query with table-local predicate and join condition:
    // SELECT * FROM orders JOIN customers ON orders.customer_id = customers.customer_id
    // WHERE orders.amount > 110
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        from: Some(ast::FromClause::Join {
            left: Box::new(ast::FromClause::Table {
                name: "orders".to_string(),
                alias: None,
            }),
            right: Box::new(ast::FromClause::Table {
                name: "customers".to_string(),
                alias: None,
            }),
            join_type: ast::JoinType::Inner,
            condition: Some(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: Some("orders".to_string()),
                    column: "customer_id".to_string(),
                }),
                op: ast::BinaryOperator::Equal,
                right: Box::new(ast::Expression::ColumnRef {
                    table: Some("customers".to_string()),
                    column: "customer_id".to_string(),
                }),
            }),
        }),
        where_clause: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::ColumnRef {
                table: Some("orders".to_string()),
                column: "amount".to_string(),
            }),
            op: ast::BinaryOperator::GreaterThan,
            right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(110))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Orders with amount > 110 are orders 11-19 (9 orders)
    assert_eq!(result.len(), 9);

    // Verify all results have amount > 110
    for row in &result {
        let amount = match &row.values[2] {
            types::SqlValue::Integer(a) => *a,
            _ => panic!("Expected integer amount"),
        };
        assert!(amount > 110);
    }
}

#[test]
fn test_table_local_predicate_with_multiple_conditions() {
    // Test multiple AND-ed table-local predicates on same table
    let mut db = storage::Database::new();

    let schema = catalog::TableSchema::new(
        "products".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("price".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("stock".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert 50 products
    for i in 0..50 {
        db.insert_row(
            "products",
            storage::Row::new(vec![
                types::SqlValue::Integer(i),
                types::SqlValue::Integer(10 + i * 2), // price: 10, 12, 14, ...
                types::SqlValue::Integer(i % 10),     // stock: 0-9 cycling
            ]),
        )
        .unwrap();
    }

    let executor = SelectExecutor::new(&db);

    // Query: SELECT * FROM products WHERE price > 50 AND stock > 5
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        from: Some(ast::FromClause::Table {
            name: "products".to_string(),
            alias: None,
        }),
        where_clause: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: None,
                    column: "price".to_string(),
                }),
                op: ast::BinaryOperator::GreaterThan,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(50))),
            }),
            op: ast::BinaryOperator::And,
            right: Box::new(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: None,
                    column: "stock".to_string(),
                }),
                op: ast::BinaryOperator::GreaterThan,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(5))),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Verify all results satisfy both conditions
    assert!(result.len() > 0);
    for row in &result {
        let price = match &row.values[1] {
            types::SqlValue::Integer(p) => *p,
            _ => panic!("Expected integer price"),
        };
        let stock = match &row.values[2] {
            types::SqlValue::Integer(s) => *s,
            _ => panic!("Expected integer stock"),
        };
        assert!(price > 50);
        assert!(stock > 5);
    }
}
