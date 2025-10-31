//! IN and NOT IN predicate tests
//!
//! Tests for SQL IN/NOT IN predicates covering:
//! - Basic IN list matching
//! - NOT IN list exclusion
//! - NULL value handling (both in expression and list)
//! - Three-valued logic compliance (SQL:1999)

use super::super::super::*;

#[test]
fn test_in_list_basic() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new("val".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(1)])).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(2)])).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(5)])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val IN (1, 3, 5)
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        from: Some(ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(ast::Expression::InList {
            expr: Box::new(ast::Expression::ColumnRef { table: None, column: "val".to_string() }),
            values: vec![
                ast::Expression::Literal(types::SqlValue::Integer(1)),
                ast::Expression::Literal(types::SqlValue::Integer(3)),
                ast::Expression::Literal(types::SqlValue::Integer(5)),
            ],
            negated: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2); // 1 and 5 match
}

#[test]
fn test_not_in_list() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new("val".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(1)])).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(2)])).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(5)])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val NOT IN (1, 3, 5)
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        from: Some(ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(ast::Expression::InList {
            expr: Box::new(ast::Expression::ColumnRef { table: None, column: "val".to_string() }),
            values: vec![
                ast::Expression::Literal(types::SqlValue::Integer(1)),
                ast::Expression::Literal(types::SqlValue::Integer(3)),
                ast::Expression::Literal(types::SqlValue::Integer(5)),
            ],
            negated: true,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1); // Only 2 doesn't match
    assert_eq!(result[0].values[0], types::SqlValue::Integer(2));
}

#[test]
fn test_in_list_with_null_value() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new("val".to_string(), types::DataType::Integer, true)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(1)])).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Null])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val IN (1, 3, 5)
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        from: Some(ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(ast::Expression::InList {
            expr: Box::new(ast::Expression::ColumnRef { table: None, column: "val".to_string() }),
            values: vec![
                ast::Expression::Literal(types::SqlValue::Integer(1)),
                ast::Expression::Literal(types::SqlValue::Integer(3)),
                ast::Expression::Literal(types::SqlValue::Integer(5)),
            ],
            negated: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1); // NULL doesn't match, only 1 matches
    assert_eq!(result[0].values[0], types::SqlValue::Integer(1));
}

#[test]
fn test_in_list_with_null_in_list() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new("val".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(2)])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val IN (1, NULL, 5)
    // Should return no rows because 2 doesn't match 1 or 5, and NULL comparison is unknown
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        from: Some(ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(ast::Expression::InList {
            expr: Box::new(ast::Expression::ColumnRef { table: None, column: "val".to_string() }),
            values: vec![
                ast::Expression::Literal(types::SqlValue::Integer(1)),
                ast::Expression::Literal(types::SqlValue::Null),
                ast::Expression::Literal(types::SqlValue::Integer(5)),
            ],
            negated: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 0); // NULL in list causes unknown result for non-matching value
}
