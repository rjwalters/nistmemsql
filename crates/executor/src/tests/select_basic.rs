//! Basic SELECT tests (no WHERE, no aggregates, no JOINs)
//!
//! Tests for fundamental SELECT functionality including wildcards, column projection, and ORDER BY.

use super::super::*;

#[test]
fn test_select_star() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: Some(100) },
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
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(1));
    assert_eq!(result[0].values[1], types::SqlValue::Varchar("Alice".to_string()));
    assert_eq!(result[1].values[0], types::SqlValue::Integer(2));
    assert_eq!(result[1].values[1], types::SqlValue::Varchar("Bob".to_string()));
}

#[test]
fn test_select_specific_columns() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
            catalog::ColumnSchema::new("age".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("Alice".to_string()),
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
        select_list: vec![
            ast::SelectItem::Expression {
                expr: ast::Expression::ColumnRef { table: None, column: "name".to_string() },
                alias: None,
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::ColumnRef { table: None, column: "age".to_string() },
                alias: None,
            },
        ],
        from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values.len(), 2);
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("Alice".to_string()));
    assert_eq!(result[0].values[1], types::SqlValue::Integer(25));
}

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

// ============================================================================
// Derived Column List Tests (SQL:1999 Feature E051-07/08)
// ============================================================================

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
