//! COUNT, SUM, and AVG aggregate function tests
//!
//! Tests for basic aggregate functions and their behavior with NULL values.

use super::super::*;

#[test]
fn test_count_star_no_group_by() {
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
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(25)]),
    )
    .unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(30)]),
    )
    .unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(35)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "COUNT".to_string(),
                args: vec![ast::Expression::Wildcard],
            },
            alias: None,
        }],
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
    assert_eq!(result[0].values[0], types::SqlValue::Integer(3));
}

#[test]
fn test_sum_no_group_by() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "sales".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("amount".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "sales",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(100)]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(200)]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(150)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "SUM".to_string(),
                args: vec![ast::Expression::ColumnRef {
                    table: None,
                    column: "amount".to_string(),
                }],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "sales".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(450));
}

#[test]
fn test_count_with_nulls() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "data".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("value".to_string(), types::DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "data",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(10)]),
    )
    .unwrap();
    db.insert_row(
        "data",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Null]),
    )
    .unwrap();
    db.insert_row(
        "data",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(20)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    // COUNT(*) counts all rows including NULL
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "COUNT".to_string(),
                args: vec![ast::Expression::Wildcard],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "data".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(3)); // Counts all 3 rows
}

#[test]
fn test_sum_with_nulls() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "amounts".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("value".to_string(), types::DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "amounts",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(100)]),
    )
    .unwrap();
    db.insert_row(
        "amounts",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Null]),
    )
    .unwrap();
    db.insert_row(
        "amounts",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(200)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "SUM".to_string(),
                args: vec![ast::Expression::ColumnRef { table: None, column: "value".to_string() }],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "amounts".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // SUM ignores NULL values, so 100 + 200 = 300
    assert_eq!(result[0].values[0], types::SqlValue::Integer(300));
}

#[test]
fn test_avg_function() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "scores".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("score".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "scores",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(80)]),
    )
    .unwrap();
    db.insert_row(
        "scores",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(90)]),
    )
    .unwrap();
    db.insert_row(
        "scores",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(70)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "AVG".to_string(),
                args: vec![ast::Expression::ColumnRef { table: None, column: "score".to_string() }],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "scores".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // AVG(80, 90, 70) = 240 / 3 = 80
    assert_eq!(result[0].values[0], types::SqlValue::Integer(80));
}

#[test]
fn test_avg_with_nulls() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "ratings".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("rating".to_string(), types::DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "ratings",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(5)]),
    )
    .unwrap();
    db.insert_row(
        "ratings",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Null]),
    )
    .unwrap();
    db.insert_row(
        "ratings",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(3)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "AVG".to_string(),
                args: vec![ast::Expression::ColumnRef {
                    table: None,
                    column: "rating".to_string(),
                }],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "ratings".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // AVG ignores NULL, so (5 + 3) / 2 = 4
    assert_eq!(result[0].values[0], types::SqlValue::Integer(4));
}

#[test]
fn test_count_column_all_nulls() {
    // Edge case: COUNT(column) with ALL NULL values should return 0, not row count
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "null_data".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("value".to_string(), types::DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "null_data",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Null]),
    )
    .unwrap();
    db.insert_row(
        "null_data",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Null]),
    )
    .unwrap();
    db.insert_row(
        "null_data",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Null]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // COUNT(column) should return 0 when all values are NULL
    let stmt_count_col = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "COUNT".to_string(),
                args: vec![ast::Expression::ColumnRef { table: None, column: "value".to_string() }],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "null_data".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt_count_col).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(0)); // COUNT(col) with all NULLs = 0

    // COUNT(*) should still return row count
    let stmt_count_star = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "COUNT".to_string(),
                args: vec![ast::Expression::Wildcard],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "null_data".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt_count_star).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(3)); // COUNT(*) counts rows
}
