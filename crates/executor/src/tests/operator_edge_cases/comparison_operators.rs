//! Comparison operator edge case tests
//!
//! Tests for complex nested comparison expressions,
//! logical AND/OR combinations, and comparison edge cases.

use crate::*;

#[test]
fn test_nested_comparisons() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new("val".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(15)])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE (val > 10) AND (val < 20)
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        from: Some(ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: None,
                    column: "val".to_string(),
                }),
                op: ast::BinaryOperator::GreaterThan,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(10))),
            }),
            op: ast::BinaryOperator::And,
            right: Box::new(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: None,
                    column: "val".to_string(),
                }),
                op: ast::BinaryOperator::LessThan,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(20))),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(15));
}
