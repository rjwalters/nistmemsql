//! Tests for AVG DISTINCT

use super::*;

#[test]
fn test_avg_distinct() {
    let db = create_test_db_with_duplicates();
    let executor = SelectExecutor::new(&db);

    // SELECT AVG(DISTINCT amount) FROM sales
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::AggregateFunction {
                name: "AVG".to_string(),
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
    // Average of unique values: (100 + 200 + 300) / 3 = 200
    assert_eq!(result[0].values[0], types::SqlValue::Integer(200));
}
