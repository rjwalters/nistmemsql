//! DISTINCT tests
//!
//! Tests for SELECT DISTINCT functionality to remove duplicate rows.

use super::super::*;

#[test]
fn test_distinct_removes_duplicate_rows() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "products".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "category".to_string(),
                types::DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert multiple rows with duplicate categories
    db.insert_row(
        "products",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("Electronics".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Varchar("Books".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        storage::Row::new(vec![
            types::SqlValue::Integer(3),
            types::SqlValue::Varchar("Electronics".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        storage::Row::new(vec![
            types::SqlValue::Integer(4),
            types::SqlValue::Varchar("Books".to_string()),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT DISTINCT category FROM products
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: true,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::ColumnRef { table: None, column: "category".to_string() },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "products".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should only return 2 unique categories
    assert_eq!(result.len(), 2);

    // Extract category values
    let categories: Vec<String> = result
        .iter()
        .map(|row| match &row.values[0] {
            types::SqlValue::Varchar(s) => s.clone(),
            _ => panic!("Expected varchar"),
        })
        .collect();

    // Both categories should be present
    assert!(categories.contains(&"Electronics".to_string()));
    assert!(categories.contains(&"Books".to_string()));
}

#[test]
fn test_distinct_with_multiple_columns() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "orders".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("customer_id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "status".to_string(),
                types::DataType::Varchar { max_length: Some(20) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert rows with various combinations
    db.insert_row(
        "orders",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Integer(100),
            types::SqlValue::Varchar("pending".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "orders",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Integer(100),
            types::SqlValue::Varchar("shipped".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "orders",
        storage::Row::new(vec![
            types::SqlValue::Integer(3),
            types::SqlValue::Integer(100),
            types::SqlValue::Varchar("pending".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "orders",
        storage::Row::new(vec![
            types::SqlValue::Integer(4),
            types::SqlValue::Integer(200),
            types::SqlValue::Varchar("pending".to_string()),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT DISTINCT customer_id, status FROM orders
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: true,
        select_list: vec![
            ast::SelectItem::Expression {
                expr: ast::Expression::ColumnRef { table: None, column: "customer_id".to_string() },
                alias: None,
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::ColumnRef { table: None, column: "status".to_string() },
                alias: None,
            },
        ],
        from: Some(ast::FromClause::Table { name: "orders".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should return 3 unique combinations: (100, pending), (100, shipped), (200, pending)
    assert_eq!(result.len(), 3);
}

#[test]
fn test_distinct_with_null_values() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "items".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "description".to_string(),
                types::DataType::Varchar { max_length: Some(100) },
                true, // nullable
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert rows with NULL values
    db.insert_row(
        "items",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("Item A".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "items",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Null]),
    )
    .unwrap();
    db.insert_row(
        "items",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Null]),
    )
    .unwrap();
    db.insert_row(
        "items",
        storage::Row::new(vec![
            types::SqlValue::Integer(4),
            types::SqlValue::Varchar("Item A".to_string()),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT DISTINCT description FROM items
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: true,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::ColumnRef { table: None, column: "description".to_string() },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "items".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should return 2 unique values: "Item A" and NULL (NULLs are considered equal for DISTINCT)
    assert_eq!(result.len(), 2);
}

#[test]
fn test_distinct_false_preserves_duplicates() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "products".to_string(),
        vec![catalog::ColumnSchema::new(
            "category".to_string(),
            types::DataType::Varchar { max_length: Some(50) },
            false,
        )],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "products",
        storage::Row::new(vec![types::SqlValue::Varchar("Electronics".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "products",
        storage::Row::new(vec![types::SqlValue::Varchar("Electronics".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "products",
        storage::Row::new(vec![types::SqlValue::Varchar("Books".to_string())]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT category FROM products (without DISTINCT)
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        from: Some(ast::FromClause::Table { name: "products".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should return all 3 rows including duplicates
    assert_eq!(result.len(), 3);
}

#[test]
fn test_distinct_with_where_clause() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "role".to_string(),
                types::DataType::Varchar { max_length: Some(20) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "users",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("admin".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Varchar("user".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![
            types::SqlValue::Integer(3),
            types::SqlValue::Varchar("admin".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![
            types::SqlValue::Integer(4),
            types::SqlValue::Varchar("admin".to_string()),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT DISTINCT role FROM users WHERE id > 1
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: true,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::ColumnRef { table: None, column: "role".to_string() },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
        where_clause: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: ast::BinaryOperator::GreaterThan,
            right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(1))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should filter first (id > 1), then apply DISTINCT
    // Remaining rows: id=2 (user), id=3 (admin), id=4 (admin)
    // After DISTINCT: user, admin
    assert_eq!(result.len(), 2);
}

#[test]
fn test_distinct_with_order_by() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "products".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "category".to_string(),
                types::DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "products",
        storage::Row::new(vec![
            types::SqlValue::Integer(3),
            types::SqlValue::Varchar("Books".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("Electronics".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Varchar("Books".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        storage::Row::new(vec![
            types::SqlValue::Integer(4),
            types::SqlValue::Varchar("Electronics".to_string()),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT DISTINCT category FROM products ORDER BY category
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: true,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::ColumnRef { table: None, column: "category".to_string() },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "products".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: Some(vec![ast::OrderByItem {
            expr: ast::Expression::ColumnRef { table: None, column: "category".to_string() },
            direction: ast::OrderDirection::Asc,
        }]),
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should return 2 unique categories, sorted
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("Books".to_string()));
    assert_eq!(result[1].values[0], types::SqlValue::Varchar("Electronics".to_string()));
}
