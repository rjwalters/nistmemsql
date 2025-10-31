//! TRIM string function tests
//!
//! Tests for SQL TRIM function variations:
//! - Default BOTH trimming (spaces)
//! - LEADING trim (left side only)
//! - TRAILING trim (right side only)
//! - Custom character trimming
//! - NULL string and removal character handling

use super::super::super::*;

#[test]
fn test_trim_both_default() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT TRIM('  hello  ')
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Trim {
                position: None,     // Defaults to Both
                removal_char: None, // Defaults to space
                string: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "  hello  ".to_string(),
                ))),
            },
            alias: Some("result".to_string()),
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
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("hello".to_string()));
}

#[test]
fn test_trim_leading() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT TRIM(LEADING FROM '  hello  ')
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Trim {
                position: Some(ast::TrimPosition::Leading),
                removal_char: None,
                string: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "  hello  ".to_string(),
                ))),
            },
            alias: Some("result".to_string()),
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
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("hello  ".to_string()));
}

#[test]
fn test_trim_trailing() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT TRIM(TRAILING FROM '  hello  ')
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Trim {
                position: Some(ast::TrimPosition::Trailing),
                removal_char: None,
                string: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "  hello  ".to_string(),
                ))),
            },
            alias: Some("result".to_string()),
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
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("  hello".to_string()));
}

#[test]
fn test_trim_custom_char() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT TRIM('x' FROM 'xxxhelloxxx')
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Trim {
                position: None,
                removal_char: Some(Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "x".to_string(),
                )))),
                string: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "xxxhelloxxx".to_string(),
                ))),
            },
            alias: Some("result".to_string()),
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
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("hello".to_string()));
}

#[test]
fn test_trim_null_string() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT TRIM(NULL)
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Trim {
                position: None,
                removal_char: None,
                string: Box::new(ast::Expression::Literal(types::SqlValue::Null)),
            },
            alias: Some("result".to_string()),
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
fn test_trim_null_removal_char() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT TRIM(NULL FROM 'hello')
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Trim {
                position: None,
                removal_char: Some(Box::new(ast::Expression::Literal(types::SqlValue::Null))),
                string: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "hello".to_string(),
                ))),
            },
            alias: Some("result".to_string()),
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
