//! BETWEEN and NOT BETWEEN range tests
//!
//! Tests for SQL BETWEEN predicates covering:
//! - Basic range inclusion (inclusive boundaries)
//! - NOT BETWEEN range exclusion
//! - NULL expression handling
//! - Boundary value edge cases

use super::super::super::*;

#[test]
fn test_between_with_null_expr() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new("val".to_string(), types::DataType::Integer, true)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Null])).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(5)])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val BETWEEN 1 AND 10
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(ast::Expression::Between {
            expr: Box::new(ast::Expression::ColumnRef { table: None, column: "val".to_string() }),
            low: Box::new(ast::Expression::Literal(types::SqlValue::Integer(1))),
            high: Box::new(ast::Expression::Literal(types::SqlValue::Integer(10))),
            negated: false,
            symmetric: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1); // NULL doesn't match, only 5
    assert_eq!(result[0].values[0], types::SqlValue::Integer(5));
}

#[test]
fn test_not_between() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new("val".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(5)])).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(15)])).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(25)])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val NOT BETWEEN 10 AND 20
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(ast::Expression::Between {
            expr: Box::new(ast::Expression::ColumnRef { table: None, column: "val".to_string() }),
            low: Box::new(ast::Expression::Literal(types::SqlValue::Integer(10))),
            high: Box::new(ast::Expression::Literal(types::SqlValue::Integer(20))),
            negated: true,
            symmetric: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2); // 5 and 25
}

#[test]
fn test_between_boundary_values() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new("val".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(10)])).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(15)])).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(20)])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val BETWEEN 10 AND 20
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(ast::Expression::Between {
            expr: Box::new(ast::Expression::ColumnRef { table: None, column: "val".to_string() }),
            low: Box::new(ast::Expression::Literal(types::SqlValue::Integer(10))),
            high: Box::new(ast::Expression::Literal(types::SqlValue::Integer(20))),
            negated: false,
            symmetric: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 3); // All values including boundaries
}
