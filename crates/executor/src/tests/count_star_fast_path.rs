//! Tests for COUNT(*) fast path optimization
//!
//! These tests verify that the fast path optimization correctly handles
//! simple COUNT(*) queries and properly falls back to the normal path
//! when conditions are not met.

use super::super::*;

#[test]
fn test_count_star_fast_path_simple() {
    // Basic test: Simple COUNT(*) FROM table should use fast path
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test_table".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("value".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert 1000 rows
    for i in 0..1000 {
        db.insert_row(
            "test_table",
            storage::Row::new(vec![types::SqlValue::Integer(i), types::SqlValue::Integer(i * 2)]),
        )
        .unwrap();
    }

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::AggregateFunction {
                name: "COUNT".to_string(),
                distinct: false,
                args: vec![ast::Expression::Wildcard],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "test_table".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(1000));
}

#[test]
fn test_count_star_fast_path_empty_table() {
    // Test COUNT(*) on empty table
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "empty_table".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::AggregateFunction {
                name: "COUNT".to_string(),
                distinct: false,
                args: vec![ast::Expression::Wildcard],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "empty_table".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(0));
}

#[test]
fn test_count_star_with_where_no_fast_path() {
    // COUNT(*) with WHERE should NOT use fast path
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test_table".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("value".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    for i in 0..10 {
        db.insert_row(
            "test_table",
            storage::Row::new(vec![types::SqlValue::Integer(i), types::SqlValue::Integer(i * 2)]),
        )
        .unwrap();
    }

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::AggregateFunction {
                name: "COUNT".to_string(),
                distinct: false,
                args: vec![ast::Expression::Wildcard],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "test_table".to_string(), alias: None }),
        where_clause: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::ColumnRef { table: None, column: "value".to_string() }),
            op: ast::BinaryOperator::GreaterThan,
            right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(5))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // Should count only rows where value > 5 (which is id > 2, so 7 rows)
    assert_eq!(result[0].values[0], types::SqlValue::Integer(7));
}

#[test]
fn test_count_star_with_group_by_no_fast_path() {
    // COUNT(*) with GROUP BY should NOT use fast path
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test_table".to_string(),
        vec![
            catalog::ColumnSchema::new("category".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("value".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert rows with categories 1, 1, 2, 2, 2
    db.insert_row(
        "test_table",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(10)]),
    )
    .unwrap();
    db.insert_row(
        "test_table",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(20)]),
    )
    .unwrap();
    db.insert_row(
        "test_table",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(30)]),
    )
    .unwrap();
    db.insert_row(
        "test_table",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(40)]),
    )
    .unwrap();
    db.insert_row(
        "test_table",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(50)]),
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
                expr: ast::Expression::ColumnRef { table: None, column: "category".to_string() },
                alias: None,
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: false,
                    args: vec![ast::Expression::Wildcard],
                },
                alias: None,
            },
        ],
        from: Some(ast::FromClause::Table { name: "test_table".to_string(), alias: None }),
        where_clause: None,
        group_by: Some(vec![ast::Expression::ColumnRef {
            table: None,
            column: "category".to_string(),
        }]),
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2);
    // Should have two groups with counts 2 and 3
}

#[test]
fn test_count_star_distinct_no_fast_path() {
    // SELECT DISTINCT COUNT(*) should NOT use fast path
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test_table".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    for i in 0..5 {
        db.insert_row("test_table", storage::Row::new(vec![types::SqlValue::Integer(i)])).unwrap();
    }

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: true, // DISTINCT specified
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::AggregateFunction {
                name: "COUNT".to_string(),
                distinct: false,
                args: vec![ast::Expression::Wildcard],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "test_table".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(5));
}

#[test]
fn test_count_column_no_fast_path() {
    // COUNT(column) should NOT use fast path (only COUNT(*) should)
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test_table".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    for i in 0..5 {
        db.insert_row("test_table", storage::Row::new(vec![types::SqlValue::Integer(i)])).unwrap();
    }

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::AggregateFunction {
                name: "COUNT".to_string(),
                distinct: false,
                args: vec![ast::Expression::ColumnRef { table: None, column: "id".to_string() }],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "test_table".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(5));
}

#[test]
fn test_count_star_with_alias() {
    // COUNT(*) with alias should still use fast path
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test_table".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    for i in 0..100 {
        db.insert_row("test_table", storage::Row::new(vec![types::SqlValue::Integer(i)])).unwrap();
    }

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::AggregateFunction {
                name: "COUNT".to_string(),
                distinct: false,
                args: vec![ast::Expression::Wildcard],
            },
            alias: Some("total".to_string()), // Has alias
        }],
        from: Some(ast::FromClause::Table { name: "test_table".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(100));
}

#[test]
fn test_count_star_multiple_select_items_no_fast_path() {
    // Multiple select items should NOT use fast path
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test_table".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("value".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    for i in 0..5 {
        db.insert_row(
            "test_table",
            storage::Row::new(vec![types::SqlValue::Integer(i), types::SqlValue::Integer(i * 10)]),
        )
        .unwrap();
    }

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            ast::SelectItem::Expression {
                expr: ast::Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: false,
                    args: vec![ast::Expression::Wildcard],
                },
                alias: None,
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::AggregateFunction {
                    name: "SUM".to_string(),
                    distinct: false,
                    args: vec![ast::Expression::ColumnRef {
                        table: None,
                        column: "value".to_string(),
                    }],
                },
                alias: None,
            },
        ],
        from: Some(ast::FromClause::Table { name: "test_table".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(5));
    assert_eq!(result[0].values[1], types::SqlValue::Integer(100)); // 0+10+20+30+40
}
