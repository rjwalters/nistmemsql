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
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new("val".to_string(), vibesql_types::DataType::Integer, true)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Null])).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(5)])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val BETWEEN 1 AND 10
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(vibesql_ast::Expression::Between {
            expr: Box::new(vibesql_ast::Expression::ColumnRef { table: None, column: "val".to_string() }),
            low: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1))),
            high: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(10))),
            negated: false,
            symmetric: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1); // NULL doesn't match, only 5
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(5));
}

#[test]
fn test_not_between() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new("val".to_string(), vibesql_types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(5)])).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(15)])).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(25)])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val NOT BETWEEN 10 AND 20
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(vibesql_ast::Expression::Between {
            expr: Box::new(vibesql_ast::Expression::ColumnRef { table: None, column: "val".to_string() }),
            low: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(10))),
            high: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(20))),
            negated: true,
            symmetric: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2); // 5 and 25
}

#[test]
fn test_between_boundary_values() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new("val".to_string(), vibesql_types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(10)])).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(15)])).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(20)])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val BETWEEN 10 AND 20
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(vibesql_ast::Expression::Between {
            expr: Box::new(vibesql_ast::Expression::ColumnRef { table: None, column: "val".to_string() }),
            low: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(10))),
            high: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(20))),
            negated: false,
            symmetric: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 3); // All values including boundaries
}

#[test]
fn test_not_between_with_null_bound() {
    // Tests issue #1797: NOT BETWEEN with NULL upper bound (SQLite behavior)
    // SQLite: val NOT BETWEEN low AND NULL → val < low (ignores NULL upper bound)
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new("val".to_string(), vibesql_types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(5)])).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(15)])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val NOT BETWEEN 10 AND NULL
    // SQLite behavior: returns rows where val < 10 (i.e., val=5)
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(vibesql_ast::Expression::Between {
            expr: Box::new(vibesql_ast::Expression::ColumnRef { table: None, column: "val".to_string() }),
            low: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(10))),
            high: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
            negated: true,
            symmetric: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1); // Returns val=5 (val < 10)
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(5));
}

#[test]
fn test_between_with_null_bound() {
    // Tests that BETWEEN with NULL bound returns FALSE (SQLite behavior)
    // This causes WHERE clause to exclude all rows
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new("val".to_string(), vibesql_types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(5)])).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(15)])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val BETWEEN NULL AND 20
    // SQLite behavior: returns FALSE (0 rows)
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(vibesql_ast::Expression::Between {
            expr: Box::new(vibesql_ast::Expression::ColumnRef { table: None, column: "val".to_string() }),
            low: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
            high: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(20))),
            negated: false,
            symmetric: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 0); // SQLite: FALSE (excludes all rows)
}

#[test]
fn test_not_between_with_null_lower_bound() {
    // Tests issue #1797: NOT BETWEEN with NULL lower bound (SQLite behavior)
    // SQLite: val NOT BETWEEN NULL AND high → val > high (ignores NULL lower bound)
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new("val".to_string(), vibesql_types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(5)])).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(15)])).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(25)])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val NOT BETWEEN NULL AND 20
    // SQLite behavior: returns rows where val > 20 (i.e., val=25)
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(vibesql_ast::Expression::Between {
            expr: Box::new(vibesql_ast::Expression::ColumnRef { table: None, column: "val".to_string() }),
            low: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
            high: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(20))),
            negated: true,
            symmetric: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1); // Returns val=25 (val > 20)
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(25));
}
