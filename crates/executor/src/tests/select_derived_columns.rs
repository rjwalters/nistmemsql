//! SELECT with derived column lists tests
//!
//! Tests for SQL:1999 Feature E051-07/08 - derived column lists (AS (col1, col2, ...))
//! These tests cover various wildcard scenarios with column renaming.

use super::super::*;

// ============================================================================
// Derived Column List Tests (SQL:1999 Feature E051-07/08)
// ============================================================================

/// Test SELECT * with derived columns (column renaming)
#[test]
fn test_select_star_with_derived_columns() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "t1".to_string(),
        vec![
            catalog::ColumnSchema::new("A".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("B".to_string(), types::DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "t1",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(2)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard {
            alias: Some(vec!["C".to_string(), "D".to_string()]),
        }],
        from: Some(ast::FromClause::Table { name: "t1".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute_with_columns(&stmt).unwrap();

    // Check column names are renamed
    assert_eq!(result.columns, vec!["C", "D"]);

    // Check values are correct
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], types::SqlValue::Integer(1));
    assert_eq!(result.rows[0].values[1], types::SqlValue::Integer(2));
}

/// Test SELECT qualified * with derived columns
#[test]
fn test_select_qualified_star_with_derived_columns() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "t1".to_string(),
        vec![
            catalog::ColumnSchema::new("A".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("B".to_string(), types::DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "t1",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(2)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::QualifiedWildcard {
            qualifier: "t1".to_string(),
            alias: Some(vec!["C".to_string(), "D".to_string()]),
        }],
        from: Some(ast::FromClause::Table { name: "t1".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute_with_columns(&stmt).unwrap();

    // Check column names are renamed
    assert_eq!(result.columns, vec!["C", "D"]);

    // Check values are correct
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], types::SqlValue::Integer(1));
    assert_eq!(result.rows[0].values[1], types::SqlValue::Integer(2));
}

/// Test error when derived columns count doesn't match table columns
#[test]
fn test_derived_columns_count_mismatch() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "t1".to_string(),
        vec![
            catalog::ColumnSchema::new("A".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("B".to_string(), types::DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "t1",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(2)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // 2 columns but 3 aliases - should error
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard {
            alias: Some(vec!["C".to_string(), "D".to_string(), "E".to_string()]),
        }],
        from: Some(ast::FromClause::Table { name: "t1".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute_with_columns(&stmt);
    assert!(result.is_err());
    match result {
        Err(ExecutorError::ColumnCountMismatch { expected: 2, provided: 3 }) => {
            // Success - expected error
        }
        _ => panic!("Expected ColumnCountMismatch error"),
    }
}

/// Test SELECT DISTINCT * with derived columns
#[test]
fn test_select_distinct_star_with_derived_columns() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "t1".to_string(),
        vec![
            catalog::ColumnSchema::new("A".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("B".to_string(), types::DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "t1",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(2)]),
    )
    .unwrap();
    db.insert_row(
        "t1",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(2)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: true, // DISTINCT
        select_list: vec![ast::SelectItem::Wildcard {
            alias: Some(vec!["C".to_string(), "D".to_string()]),
        }],
        from: Some(ast::FromClause::Table { name: "t1".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute_with_columns(&stmt).unwrap();

    // Check column names are renamed
    assert_eq!(result.columns, vec!["C", "D"]);

    // Check DISTINCT worked - only 1 row
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], types::SqlValue::Integer(1));
    assert_eq!(result.rows[0].values[1], types::SqlValue::Integer(2));
}

/// Test SELECT * with derived columns and table alias
#[test]
fn test_select_star_alias_with_table_alias() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "t1".to_string(),
        vec![
            catalog::ColumnSchema::new("A".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("B".to_string(), types::DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "t1",
        storage::Row::new(vec![types::SqlValue::Integer(10), types::SqlValue::Integer(20)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::QualifiedWildcard {
            qualifier: "alias_name".to_string(),
            alias: Some(vec!["X".to_string(), "Y".to_string()]),
        }],
        from: Some(ast::FromClause::Table {
            name: "t1".to_string(),
            alias: Some("alias_name".to_string()),
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute_with_columns(&stmt).unwrap();

    // Check column names are renamed
    assert_eq!(result.columns, vec!["X", "Y"]);

    // Check values are correct
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], types::SqlValue::Integer(10));
    assert_eq!(result.rows[0].values[1], types::SqlValue::Integer(20));
}

// ============================================================================
// Tests for derived column lists (SQL:1999 E051-07, E051-08)
// ============================================================================

/// Test SELECT * AS (c, d) using parser
#[test]
fn test_select_wildcard_with_derived_column_list() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "T".to_string(),
        vec![
            catalog::ColumnSchema::new("a".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("b".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "T",
        storage::Row::new(vec![
            types::SqlValue::Integer(10),
            types::SqlValue::Integer(20),
        ]),
    )
    .unwrap();

    let stmt = parser::Parser::parse_sql("SELECT * AS (c, d) FROM t;").unwrap();
    let select_stmt = match stmt {
        ast::Statement::Select(s) => s,
        _ => panic!("Expected SELECT statement"),
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute_with_columns(&select_stmt).unwrap();

    // Check column names are renamed
    assert_eq!(result.columns, vec!["C", "D"]);

    // Check values are correct
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], types::SqlValue::Integer(10));
    assert_eq!(result.rows[0].values[1], types::SqlValue::Integer(20));
}

/// Test SELECT ALL * AS (c, d) using parser
#[test]
fn test_select_all_wildcard_with_derived_column_list() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "T".to_string(),
        vec![
            catalog::ColumnSchema::new("a".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("b".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "T",
        storage::Row::new(vec![
            types::SqlValue::Integer(10),
            types::SqlValue::Integer(20),
        ]),
    )
    .unwrap();

    let stmt = parser::Parser::parse_sql("SELECT ALL * AS (c, d) FROM t;").unwrap();
    let select_stmt = match stmt {
        ast::Statement::Select(s) => s,
        _ => panic!("Expected SELECT statement"),
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute_with_columns(&select_stmt).unwrap();

    // Check column names are renamed
    assert_eq!(result.columns, vec!["C", "D"]);

    // Check values are correct
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], types::SqlValue::Integer(10));
    assert_eq!(result.rows[0].values[1], types::SqlValue::Integer(20));
}

/// Test SELECT DISTINCT * AS (c, d) using parser
#[test]
fn test_select_distinct_wildcard_with_derived_column_list() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "T".to_string(),
        vec![
            catalog::ColumnSchema::new("a".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("b".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "T",
        storage::Row::new(vec![
            types::SqlValue::Integer(10),
            types::SqlValue::Integer(20),
        ]),
    )
    .unwrap();
    db.insert_row(
        "T",
        storage::Row::new(vec![
            types::SqlValue::Integer(10),
            types::SqlValue::Integer(20),
        ]),
    )
    .unwrap();

    let stmt = parser::Parser::parse_sql("SELECT DISTINCT * AS (c, d) FROM t;").unwrap();
    let select_stmt = match stmt {
        ast::Statement::Select(s) => s,
        _ => panic!("Expected SELECT statement"),
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute_with_columns(&select_stmt).unwrap();

    // Check column names are renamed
    assert_eq!(result.columns, vec!["C", "D"]);

    // Check DISTINCT works - should have only 1 row
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], types::SqlValue::Integer(10));
    assert_eq!(result.rows[0].values[1], types::SqlValue::Integer(20));
}

/// Test SELECT t.* AS (c, d) using parser
#[test]
fn test_select_qualified_wildcard_with_derived_column_list() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "T".to_string(),
        vec![
            catalog::ColumnSchema::new("a".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("b".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "T",
        storage::Row::new(vec![
            types::SqlValue::Integer(10),
            types::SqlValue::Integer(20),
        ]),
    )
    .unwrap();

    let stmt = parser::Parser::parse_sql("SELECT t.* AS (c, d) FROM t;").unwrap();
    let select_stmt = match stmt {
        ast::Statement::Select(s) => s,
        _ => panic!("Expected SELECT statement"),
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute_with_columns(&select_stmt).unwrap();

    // Check column names are renamed
    assert_eq!(result.columns, vec!["C", "D"]);

    // Check values are correct
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], types::SqlValue::Integer(10));
    assert_eq!(result.rows[0].values[1], types::SqlValue::Integer(20));
}

/// Test SELECT myalias.* AS (c, d) FROM mytable AS myalias using parser
#[test]
fn test_select_alias_wildcard_with_derived_column_list() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "MYTABLE".to_string(),
        vec![
            catalog::ColumnSchema::new("a".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("b".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "MYTABLE",
        storage::Row::new(vec![
            types::SqlValue::Integer(10),
            types::SqlValue::Integer(20),
        ]),
    )
    .unwrap();

    let stmt = parser::Parser::parse_sql("SELECT myalias.* AS (c, d) FROM mytable AS myalias;").unwrap();
    let select_stmt = match stmt {
        ast::Statement::Select(s) => s,
        _ => panic!("Expected SELECT statement"),
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute_with_columns(&select_stmt).unwrap();

    // Check column names are renamed
    assert_eq!(result.columns, vec!["C", "D"]);

    // Check values are correct
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], types::SqlValue::Integer(10));
    assert_eq!(result.rows[0].values[1], types::SqlValue::Integer(20));
}

/// Test error when derived column list count doesn't match table columns
#[test]
fn test_select_derived_column_list_count_mismatch_error() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "T".to_string(),
        vec![
            catalog::ColumnSchema::new("a".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("b".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    let stmt = parser::Parser::parse_sql("SELECT * AS (c, d, e) FROM t;").unwrap();
    let select_stmt = match stmt {
        ast::Statement::Select(s) => s,
        _ => panic!("Expected SELECT statement"),
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute_with_columns(&select_stmt);

    // Should fail with column count mismatch
    assert!(result.is_err());
    match result {
        Err(ExecutorError::ColumnCountMismatch { expected, provided }) => {
            assert_eq!(expected, 2); // Table has 2 columns
            assert_eq!(provided, 3); // We provided 3 names in derived list
        }
        _ => panic!("Expected ColumnCountMismatch error"),
    }
}
