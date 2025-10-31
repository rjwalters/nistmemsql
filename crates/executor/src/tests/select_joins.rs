//! JOIN tests
//!
//! Tests for INNER JOIN operations.

use super::super::*;

#[test]
fn test_inner_join_two_tables() {
    let mut db = storage::Database::new();

    let users_schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(users_schema).unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("Alice".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Varchar("Bob".to_string()),
        ]),
    )
    .unwrap();

    let orders_schema = catalog::TableSchema::new(
        "orders".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("user_id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("amount".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(orders_schema).unwrap();
    db.insert_row(
        "orders",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Integer(1),
            types::SqlValue::Integer(50),
        ]),
    )
    .unwrap();
    db.insert_row(
        "orders",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Integer(2),
            types::SqlValue::Integer(75),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Join {
            left: Box::new(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            right: Box::new(ast::FromClause::Table { name: "orders".to_string(), alias: None }),
            join_type: ast::JoinType::Inner,
            condition: Some(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: Some("users".to_string()),
                    column: "id".to_string(),
                }),
                op: ast::BinaryOperator::Equal,
                right: Box::new(ast::Expression::ColumnRef {
                    table: Some("orders".to_string()),
                    column: "user_id".to_string(),
                }),
            }),
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values.len(), 5); // users (2 cols) + orders (3 cols)
}

#[test]
fn test_right_outer_join() {
    let mut db = storage::Database::new();

    // Create users table with 2 users
    let users_schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(users_schema).unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("Alice".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Varchar("Bob".to_string()),
        ]),
    )
    .unwrap();

    // Create orders table with order for user 2 and user 999 (no matching user)
    let orders_schema = catalog::TableSchema::new(
        "orders".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("user_id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("amount".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(orders_schema).unwrap();
    db.insert_row(
        "orders",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Integer(2),
            types::SqlValue::Integer(75),
        ]),
    )
    .unwrap();
    db.insert_row(
        "orders",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Integer(999), // No matching user
            types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();

    // RIGHT OUTER JOIN should include all orders, with NULLs for missing users
    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Join {
            left: Box::new(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            right: Box::new(ast::FromClause::Table { name: "orders".to_string(), alias: None }),
            join_type: ast::JoinType::RightOuter,
            condition: Some(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: Some("users".to_string()),
                    column: "id".to_string(),
                }),
                op: ast::BinaryOperator::Equal,
                right: Box::new(ast::Expression::ColumnRef {
                    table: Some("orders".to_string()),
                    column: "user_id".to_string(),
                }),
            }),
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2); // Both orders should appear

    // One row should have NULLs for user columns (order for user 999)
    let null_count = result.iter().filter(|row| row.values[0] == types::SqlValue::Null).count();
    assert_eq!(null_count, 1, "Should have one row with NULL user");
}

#[test]
fn test_full_outer_join() {
    let mut db = storage::Database::new();

    // Create users table with users 1, 2
    let users_schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(users_schema).unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("Alice".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Varchar("Bob".to_string()),
        ]),
    )
    .unwrap();

    // Create orders: one for user 2, one for user 999 (no match)
    let orders_schema = catalog::TableSchema::new(
        "orders".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("user_id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("amount".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(orders_schema).unwrap();
    db.insert_row(
        "orders",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Integer(2),
            types::SqlValue::Integer(75),
        ]),
    )
    .unwrap();
    db.insert_row(
        "orders",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Integer(999),
            types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();

    // FULL OUTER JOIN should include:
    // - User 1 with NULL order
    // - User 2 with order 1
    // - Order 2 with NULL user
    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Join {
            left: Box::new(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            right: Box::new(ast::FromClause::Table { name: "orders".to_string(), alias: None }),
            join_type: ast::JoinType::FullOuter,
            condition: Some(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: Some("users".to_string()),
                    column: "id".to_string(),
                }),
                op: ast::BinaryOperator::Equal,
                right: Box::new(ast::Expression::ColumnRef {
                    table: Some("orders".to_string()),
                    column: "user_id".to_string(),
                }),
            }),
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 3); // Alice (no order), Bob+order, order (no user)

    // Count rows with NULLs in user columns (order with no user)
    let null_user_count =
        result.iter().filter(|row| row.values[0] == types::SqlValue::Null).count();
    assert_eq!(null_user_count, 1, "Should have one unmatched order");

    // Count rows with NULLs in order columns (user with no order)
    let null_order_count =
        result.iter().filter(|row| row.values[2] == types::SqlValue::Null).count();
    assert_eq!(null_order_count, 1, "Should have one unmatched user");
}

#[test]
fn test_cross_join() {
    let mut db = storage::Database::new();

    // Create users table with 2 users
    let users_schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(users_schema).unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("Alice".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Varchar("Bob".to_string()),
        ]),
    )
    .unwrap();

    // Create products table with 3 products
    let products_schema = catalog::TableSchema::new(
        "products".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(products_schema).unwrap();
    db.insert_row(
        "products",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("Widget".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Varchar("Gadget".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        storage::Row::new(vec![
            types::SqlValue::Integer(3),
            types::SqlValue::Varchar("Doohickey".to_string()),
        ]),
    )
    .unwrap();

    // CROSS JOIN should produce cartesian product: 2 * 3 = 6 rows
    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Join {
            left: Box::new(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            right: Box::new(ast::FromClause::Table { name: "products".to_string(), alias: None }),
            join_type: ast::JoinType::Cross,
            condition: None, // CROSS JOIN has no condition
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 6); // 2 users * 3 products = 6 combinations
    assert_eq!(result[0].values.len(), 4); // users (2 cols) + products (2 cols)
}

#[test]
#[should_panic(expected = "CROSS JOIN does not support ON clause")]
fn test_cross_join_with_condition_fails() {
    let mut db = storage::Database::new();

    let users_schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(users_schema).unwrap();

    let products_schema = catalog::TableSchema::new(
        "products".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(products_schema).unwrap();

    // CROSS JOIN with condition should fail
    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Join {
            left: Box::new(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            right: Box::new(ast::FromClause::Table { name: "products".to_string(), alias: None }),
            join_type: ast::JoinType::Cross,
            condition: Some(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: Some("users".to_string()),
                    column: "id".to_string(),
                }),
                op: ast::BinaryOperator::Equal,
                right: Box::new(ast::Expression::ColumnRef {
                    table: Some("products".to_string()),
                    column: "id".to_string(),
                }),
            }),
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let _ = executor.execute(&stmt).unwrap(); // Should panic
}
