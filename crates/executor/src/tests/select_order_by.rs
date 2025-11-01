//! Basic SELECT ORDER BY tests
//!
//! Tests for ORDER BY functionality including single and multiple column ordering.

use super::super::*;

/// Test ORDER BY single column ascending
#[test]
fn test_order_by_single_column_asc() {
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
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(30)]),
    )
    .unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(20)]),
    )
    .unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(25)]),
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
        where_clause: None,
        group_by: None,
        having: None,
        order_by: Some(vec![ast::OrderByItem {
            expr: ast::Expression::ColumnRef { table: None, column: "age".to_string() },
            direction: ast::OrderDirection::Asc,
        }]),
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].values[1], types::SqlValue::Integer(20));
    assert_eq!(result[1].values[1], types::SqlValue::Integer(25));
    assert_eq!(result[2].values[1], types::SqlValue::Integer(30));
}

/// Test ORDER BY multiple columns with different directions
#[test]
fn test_order_by_multiple_columns() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("dept".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("age".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Integer(2),
            types::SqlValue::Integer(35),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Integer(1),
            types::SqlValue::Integer(30),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![
            types::SqlValue::Integer(3),
            types::SqlValue::Integer(2),
            types::SqlValue::Integer(20),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![
            types::SqlValue::Integer(4),
            types::SqlValue::Integer(1),
            types::SqlValue::Integer(25),
        ]),
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
        where_clause: None,
        group_by: None,
        having: None,
        order_by: Some(vec![
            ast::OrderByItem {
                expr: ast::Expression::ColumnRef { table: None, column: "dept".to_string() },
                direction: ast::OrderDirection::Asc,
            },
            ast::OrderByItem {
                expr: ast::Expression::ColumnRef { table: None, column: "age".to_string() },
                direction: ast::OrderDirection::Desc,
            },
        ]),
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 4);
    assert_eq!(result[0].values[1], types::SqlValue::Integer(1));
    assert_eq!(result[0].values[2], types::SqlValue::Integer(30));
    assert_eq!(result[1].values[1], types::SqlValue::Integer(1));
    assert_eq!(result[1].values[2], types::SqlValue::Integer(25));
    assert_eq!(result[2].values[1], types::SqlValue::Integer(2));
    assert_eq!(result[2].values[2], types::SqlValue::Integer(35));
    assert_eq!(result[3].values[1], types::SqlValue::Integer(2));
    assert_eq!(result[3].values[2], types::SqlValue::Integer(20));
}
