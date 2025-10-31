//! CAST expression tests
//!
//! Tests for SQL CAST(expr AS type) expressions:
//! - Integer to VARCHAR conversion
//! - VARCHAR to Integer conversion
//! - NULL casting (preserves NULL)
//! - Type coercion behavior

use super::super::super::*;

#[test]
fn test_cast_integer_to_varchar() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT CAST(123 AS VARCHAR(10))
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Cast {
                expr: Box::new(ast::Expression::Literal(types::SqlValue::Integer(123))),
                data_type: types::DataType::Varchar { max_length: Some(10) },
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
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("123".to_string()));
}

#[test]
fn test_cast_varchar_to_integer() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT CAST('456' AS INTEGER)
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Cast {
                expr: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "456".to_string(),
                ))),
                data_type: types::DataType::Integer,
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
    assert_eq!(result[0].values[0], types::SqlValue::Integer(456));
}

#[test]
fn test_cast_null() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT CAST(NULL AS INTEGER)
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Cast {
                expr: Box::new(ast::Expression::Literal(types::SqlValue::Null)),
                data_type: types::DataType::Integer,
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
