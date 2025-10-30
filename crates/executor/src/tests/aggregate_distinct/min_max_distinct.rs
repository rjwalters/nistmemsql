//! Tests for MIN and MAX with DISTINCT

use super::*;

#[test]
fn test_min_distinct() {
    let db = create_test_db_with_duplicates();
    let executor = SelectExecutor::new(&db);

    // SELECT MIN(DISTINCT amount) FROM sales
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::AggregateFunction {
                name: "MIN".to_string(),
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
    // MIN should be 100 (same with or without DISTINCT)
    assert_eq!(result[0].values[0], types::SqlValue::Integer(100));
}

#[test]
fn test_max_distinct() {
    let db = create_test_db_with_duplicates();
    let executor = SelectExecutor::new(&db);

    // SELECT MAX(DISTINCT amount) FROM sales
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::AggregateFunction {
                name: "MAX".to_string(),
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
    // MAX should be 300 (same with or without DISTINCT)
    assert_eq!(result[0].values[0], types::SqlValue::Integer(300));
}
