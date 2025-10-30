//! Edge case tests for DISTINCT aggregates

use super::*;

#[test]
fn test_distinct_all_same_value() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new(
            "val".to_string(),
            types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    // Insert same value 3 times
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(42)]))
        .unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(42)]))
        .unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(42)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT COUNT(DISTINCT val), SUM(DISTINCT val) FROM test
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            ast::SelectItem::Expression {
                expr: ast::Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: true,
                    args: vec![ast::Expression::ColumnRef {
                        table: None,
                        column: "val".to_string(),
                    }],
                },
                alias: None,
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::AggregateFunction {
                    name: "SUM".to_string(),
                    distinct: true,
                    args: vec![ast::Expression::ColumnRef {
                        table: None,
                        column: "val".to_string(),
                    }],
                },
                alias: None,
            },
        ],
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
    // COUNT(DISTINCT): 1 unique value
    assert_eq!(result[0].values[0], types::SqlValue::Integer(1));
    // SUM(DISTINCT): 42 (only counted once)
    assert_eq!(result[0].values[1], types::SqlValue::Integer(42));
}

#[test]
fn test_distinct_empty_table() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "empty_test".to_string(),
        vec![catalog::ColumnSchema::new(
            "val".to_string(),
            types::DataType::Integer,
            true,
        )],
    );
    db.create_table(schema).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT COUNT(DISTINCT val), SUM(DISTINCT val) FROM empty_test
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            ast::SelectItem::Expression {
                expr: ast::Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: true,
                    args: vec![ast::Expression::ColumnRef {
                        table: None,
                        column: "val".to_string(),
                    }],
                },
                alias: None,
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::AggregateFunction {
                    name: "SUM".to_string(),
                    distinct: true,
                    args: vec![ast::Expression::ColumnRef {
                        table: None,
                        column: "val".to_string(),
                    }],
                },
                alias: None,
            },
        ],
        from: Some(ast::FromClause::Table {
            name: "empty_test".to_string(),
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
    // COUNT on empty table should be 0
    assert_eq!(result[0].values[0], types::SqlValue::Integer(0));
    // SUM on empty table should be 0
    assert_eq!(result[0].values[1], types::SqlValue::Integer(0));
}
