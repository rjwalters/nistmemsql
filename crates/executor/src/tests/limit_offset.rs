//! LIMIT/OFFSET pagination tests
//!
//! Tests for LIMIT and OFFSET clause handling in SELECT statements.

use super::super::*;
fn make_pagination_stmt(limit: Option<usize>, offset: Option<usize>) -> ast::SelectStmt {
    ast::SelectStmt {
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit,
        offset,
    }
}

fn make_users_table() -> storage::Database {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: 100 },
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();
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
    db.insert_row(
        "users",
        storage::Row::new(vec![
            types::SqlValue::Integer(3),
            types::SqlValue::Varchar("Carol".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![
            types::SqlValue::Integer(4),
            types::SqlValue::Varchar("Dave".to_string()),
        ]),
    )
    .unwrap();
    db
}

#[test]
fn test_limit_basic() {
    let db = make_users_table();
    let executor = SelectExecutor::new(&db);
    let stmt = make_pagination_stmt(Some(2), None);

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(1));
    assert_eq!(result[1].values[0], types::SqlValue::Integer(2));
}

#[test]
fn test_offset_basic() {
    let db = make_users_table();
    let executor = SelectExecutor::new(&db);
    let stmt = make_pagination_stmt(None, Some(2));

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(3));
    assert_eq!(result[1].values[0], types::SqlValue::Integer(4));
}

#[test]
fn test_limit_and_offset() {
    let db = make_users_table();
    let executor = SelectExecutor::new(&db);
    let stmt = make_pagination_stmt(Some(2), Some(1));

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(2));
    assert_eq!(result[1].values[0], types::SqlValue::Integer(3));
}

#[test]
fn test_offset_beyond_result_set() {
    let db = make_users_table();
    let executor = SelectExecutor::new(&db);
    let stmt = make_pagination_stmt(None, Some(10));

    let result = executor.execute(&stmt).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_limit_greater_than_result_set() {
    let db = make_users_table();
    let executor = SelectExecutor::new(&db);
    let stmt = make_pagination_stmt(Some(10), None);

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 4);
}
