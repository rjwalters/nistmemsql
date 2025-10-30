//! Tests for SUM DISTINCT

use super::*;

#[test]
fn test_sum_distinct() {
    let db = create_test_db_with_duplicates();
    let executor = SelectExecutor::new(&db);

    // SELECT SUM(DISTINCT amount) FROM sales
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::AggregateFunction {
                name: "SUM".to_string(),
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
    // Should sum unique values: 100 + 200 + 300 = 600
    assert_eq!(result[0].values[0], types::SqlValue::Integer(600));
}

#[test]
fn test_sum_distinct_vs_sum_all() {
    let db = create_test_db_with_duplicates();
    let executor = SelectExecutor::new(&db);

    // SELECT SUM(amount), SUM(DISTINCT amount) FROM sales
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            ast::SelectItem::Expression {
                expr: ast::Expression::AggregateFunction {
                    name: "SUM".to_string(),
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
                    name: "SUM".to_string(),
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
    // SUM: 100+100+200+100+300+200 = 1000
    assert_eq!(result[0].values[0], types::SqlValue::Integer(1000));
    // SUM(DISTINCT): 100+200+300 = 600
    assert_eq!(result[0].values[1], types::SqlValue::Integer(600));
}
