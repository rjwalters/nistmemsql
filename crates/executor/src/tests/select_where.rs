//! WHERE clause tests
//!
//! Tests for SELECT with WHERE clause filtering.

use super::super::*;

#[test]
fn test_select_with_where() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("age".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(25)]),
    )
    .unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(17)]),
    )
    .unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(30)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
        where_clause: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::ColumnRef { table: None, column: "age".to_string() }),
            op: ast::BinaryOperator::GreaterThanOrEqual,
            right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(18))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(1));
    assert_eq!(result[1].values[0], types::SqlValue::Integer(3));
}

#[test]
fn test_select_with_and_condition() {
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
    db.insert_row(
        "products",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Integer(100),
            types::SqlValue::Integer(5),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Integer(200),
            types::SqlValue::Integer(0),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        storage::Row::new(vec![
            types::SqlValue::Integer(3),
            types::SqlValue::Integer(150),
            types::SqlValue::Integer(10),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    // WHERE price > 50 AND stock > 0
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        from: Some(ast::FromClause::Table { name: "products".to_string(), alias: None }),
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
                right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(0))),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2); // Products 1 and 3
    assert_eq!(result[0].values[0], types::SqlValue::Integer(1));
    assert_eq!(result[1].values[0], types::SqlValue::Integer(3));
}

#[test]
fn test_select_with_or_condition() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "items".to_string(),
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
        "items",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("electronics".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "items",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Varchar("food".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "items",
        storage::Row::new(vec![
            types::SqlValue::Integer(3),
            types::SqlValue::Varchar("books".to_string()),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    // WHERE category = 'electronics' OR category = 'books'
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        from: Some(ast::FromClause::Table { name: "items".to_string(), alias: None }),
        where_clause: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: None,
                    column: "category".to_string(),
                }),
                op: ast::BinaryOperator::Equal,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "electronics".to_string(),
                ))),
            }),
            op: ast::BinaryOperator::Or,
            right: Box::new(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: None,
                    column: "category".to_string(),
                }),
                op: ast::BinaryOperator::Equal,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "books".to_string(),
                ))),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2); // Items 1 and 3
}

#[test]
fn test_select_with_null_in_where() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "data".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("value".to_string(), types::DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "data",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(100)]),
    )
    .unwrap();
    db.insert_row(
        "data",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Null]),
    )
    .unwrap();
    db.insert_row(
        "data",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(200)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    // WHERE value > 50 - should filter out NULL (NULL comparisons are unknown)
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        from: Some(ast::FromClause::Table { name: "data".to_string(), alias: None }),
        where_clause: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::ColumnRef { table: None, column: "value".to_string() }),
            op: ast::BinaryOperator::GreaterThan,
            right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(50))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    // NULL > 50 is unknown (not true), so row 2 is filtered out
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(1));
    assert_eq!(result[1].values[0], types::SqlValue::Integer(3));
}
