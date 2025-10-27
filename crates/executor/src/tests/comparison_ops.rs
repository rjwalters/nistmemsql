//! Comparison operator tests
//!
//! Tests for comparison operators in expressions.

use super::super::*;

#[test]
fn test_greater_than_comparison() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "nums".to_string(),
        vec![catalog::ColumnSchema::new("val".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("nums", storage::Row::new(vec![types::SqlValue::Integer(10)])).unwrap();
    db.insert_row("nums", storage::Row::new(vec![types::SqlValue::Integer(20)])).unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Table { name: "nums".to_string(), alias: None }),
        where_clause: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::ColumnRef { table: None, column: "val".to_string() }),
            op: ast::BinaryOperator::GreaterThan,
            right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(15))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(20));
}

#[test]
fn test_less_than_comparison() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "nums".to_string(),
        vec![catalog::ColumnSchema::new("val".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("nums", storage::Row::new(vec![types::SqlValue::Integer(10)])).unwrap();
    db.insert_row("nums", storage::Row::new(vec![types::SqlValue::Integer(20)])).unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Table { name: "nums".to_string(), alias: None }),
        where_clause: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::ColumnRef { table: None, column: "val".to_string() }),
            op: ast::BinaryOperator::LessThan,
            right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(15))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(10));
}

#[test]
fn test_not_equal_comparison() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "nums".to_string(),
        vec![catalog::ColumnSchema::new("val".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("nums", storage::Row::new(vec![types::SqlValue::Integer(10)])).unwrap();
    db.insert_row("nums", storage::Row::new(vec![types::SqlValue::Integer(20)])).unwrap();
    db.insert_row("nums", storage::Row::new(vec![types::SqlValue::Integer(30)])).unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Table { name: "nums".to_string(), alias: None }),
        where_clause: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::ColumnRef { table: None, column: "val".to_string() }),
            op: ast::BinaryOperator::NotEqual,
            right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(20))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(10));
    assert_eq!(result[1].values[0], types::SqlValue::Integer(30));
}

#[test]
fn test_less_than_or_equal_comparison() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "nums".to_string(),
        vec![catalog::ColumnSchema::new("val".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("nums", storage::Row::new(vec![types::SqlValue::Integer(10)])).unwrap();
    db.insert_row("nums", storage::Row::new(vec![types::SqlValue::Integer(20)])).unwrap();
    db.insert_row("nums", storage::Row::new(vec![types::SqlValue::Integer(30)])).unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Table { name: "nums".to_string(), alias: None }),
        where_clause: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::ColumnRef { table: None, column: "val".to_string() }),
            op: ast::BinaryOperator::LessThanOrEqual,
            right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(20))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(10));
    assert_eq!(result[1].values[0], types::SqlValue::Integer(20));
}
