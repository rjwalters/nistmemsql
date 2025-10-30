//! Tests for COUNT DISTINCT

use super::*;

#[test]
fn test_count_distinct_basic() {
    let db = create_test_db_with_duplicates();
    let executor = SelectExecutor::new(&db);

    // SELECT COUNT(DISTINCT amount) FROM sales
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::AggregateFunction {
                name: "COUNT".to_string(),
                distinct: true,
                args: vec![ast::Expression::ColumnRef {
                    table: None,
                    column: "amount".to_string(),
                }],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table {
            name: "sales".to_string(),
            alias: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // Should count 3 distinct values: 100, 200, 300
    assert_eq!(result[0].values[0], types::SqlValue::Integer(3));
}

#[test]
fn test_count_distinct_vs_count_all() {
    let db = create_test_db_with_duplicates();
    let executor = SelectExecutor::new(&db);

    // SELECT COUNT(amount), COUNT(DISTINCT amount) FROM sales
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            ast::SelectItem::Expression {
                expr: ast::Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: false,
                    args: vec![ast::Expression::ColumnRef {
                        table: None,
                        column: "amount".to_string(),
                    }],
                },
                alias: None,
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: true,
                    args: vec![ast::Expression::ColumnRef {
                        table: None,
                        column: "amount".to_string(),
                    }],
                },
                alias: None,
            },
        ],
        from: Some(ast::FromClause::Table {
            name: "sales".to_string(),
            alias: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(6)); // COUNT(amount) - all rows
    assert_eq!(result[0].values[1], types::SqlValue::Integer(3)); // COUNT(DISTINCT amount) - unique values
}

#[test]
fn test_count_distinct_with_nulls() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new(
            "val".to_string(),
            types::DataType::Integer,
            true, // nullable
        )],
    );
    db.create_table(schema).unwrap();

    // Insert values including NULLs: 1, 1, 2, NULL, NULL
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(1)]))
        .unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(1)]))
        .unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(2)]))
        .unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Null]))
        .unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Null]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT COUNT(DISTINCT val) FROM test
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::AggregateFunction {
                name: "COUNT".to_string(),
                distinct: true,
                args: vec![ast::Expression::ColumnRef {
                    table: None,
                    column: "val".to_string(),
                }],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table {
            name: "test".to_string(),
            alias: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // Should count only unique non-NULL values: 1, 2 = 2 distinct values
    assert_eq!(result[0].values[0], types::SqlValue::Integer(2));
}
