//! POSITION string function tests
//!
//! Tests for SQL POSITION(substring IN string) function:
//! - Substring found (1-indexed position)
//! - Substring not found (returns 0)
//! - NULL substring handling
//! - NULL string handling

use super::super::super::*;

#[test]
fn test_position_found() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT POSITION('world' IN 'hello world')
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Position {
                substring: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "world".to_string(),
                ))),
                string: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "hello world".to_string(),
                ))),
                character_unit: None,
            },
            alias: Some("pos".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(7)); // Position is 1-indexed
}

#[test]
fn test_position_not_found() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT POSITION('xyz' IN 'hello world')
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Position {
                substring: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "xyz".to_string(),
                ))),
                string: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "hello world".to_string(),
                ))),
                character_unit: None,
            },
            alias: Some("pos".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(0)); // Not found returns 0
}

#[test]
fn test_position_null_substring() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT POSITION(NULL IN 'hello world')
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Position {
                substring: Box::new(ast::Expression::Literal(types::SqlValue::Null)),
                string: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "hello world".to_string(),
                ))),
                character_unit: None,
            },
            alias: Some("pos".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Null);
}

#[test]
fn test_position_null_string() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT POSITION('world' IN NULL)
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Position {
                substring: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "world".to_string(),
                ))),
                string: Box::new(ast::Expression::Literal(types::SqlValue::Null)),
                character_unit: None,
            },
            alias: Some("pos".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Null);
}
