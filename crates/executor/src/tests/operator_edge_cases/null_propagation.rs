//! NULL propagation tests
//!
//! Tests for NULL handling across all operators including
//! arithmetic operations, comparisons, and various operand positions.

use super::operator_test_utils::*;

#[test]
fn test_null_plus_integer() {
    let db = storage::Database::new();
    let expr = ast::Expression::BinaryOp {
        left: Box::new(ast::Expression::Literal(types::SqlValue::Null)),
        op: ast::BinaryOperator::Plus,
        right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(5))),
    };
    assert_expression_result(&db, expr, types::SqlValue::Null);
}

#[test]
fn test_integer_plus_null() {
    let db = storage::Database::new();
    let expr = ast::Expression::BinaryOp {
        left: Box::new(ast::Expression::Literal(types::SqlValue::Integer(5))),
        op: ast::BinaryOperator::Plus,
        right: Box::new(ast::Expression::Literal(types::SqlValue::Null)),
    };
    assert_expression_result(&db, expr, types::SqlValue::Null);
}

#[test]
fn test_null_multiply_integer() {
    let db = storage::Database::new();
    let expr = ast::Expression::BinaryOp {
        left: Box::new(ast::Expression::Literal(types::SqlValue::Null)),
        op: ast::BinaryOperator::Multiply,
        right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(5))),
    };
    assert_expression_result(&db, expr, types::SqlValue::Null);
}

#[test]
fn test_null_comparison() {
    let db = storage::Database::new();
    let expr = ast::Expression::BinaryOp {
        left: Box::new(ast::Expression::Literal(types::SqlValue::Null)),
        op: ast::BinaryOperator::Equal,
        right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(5))),
    };
    assert_expression_result(&db, expr, types::SqlValue::Null);
}
