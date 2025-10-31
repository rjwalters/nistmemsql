//! Binary arithmetic operator edge case tests
//!
//! Tests for complex nested arithmetic expressions,
//! operator precedence, and associativity.

use crate::*;

#[test]
fn test_nested_arithmetic() {
    let db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT ((5 + 3) * 2) - (10 / 2)
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::BinaryOp {
                        left: Box::new(ast::Expression::Literal(types::SqlValue::Integer(5))),
                        op: ast::BinaryOperator::Plus,
                        right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(3))),
                    }),
                    op: ast::BinaryOperator::Multiply,
                    right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(2))),
                }),
                op: ast::BinaryOperator::Minus,
                right: Box::new(ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::Literal(types::SqlValue::Integer(10))),
                    op: ast::BinaryOperator::Divide,
                    right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(2))),
                }),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(11)); // (8 * 2) - 5 = 11
}
