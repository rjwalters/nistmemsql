//! NULL propagation tests
//!
//! Tests for NULL handling across all operators including
//! arithmetic operations, comparisons, and various operand positions.

use super::operator_test_utils::*;

#[test]
fn test_null_plus_integer() {
    let db = vibesql_storage::Database::new();
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
        op: vibesql_ast::BinaryOperator::Plus,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(5))),
    };
    assert_expression_result(&db, expr, vibesql_types::SqlValue::Null);
}

#[test]
fn test_integer_plus_null() {
    let db = vibesql_storage::Database::new();
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(5))),
        op: vibesql_ast::BinaryOperator::Plus,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
    };
    assert_expression_result(&db, expr, vibesql_types::SqlValue::Null);
}

#[test]
fn test_null_multiply_integer() {
    let db = vibesql_storage::Database::new();
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
        op: vibesql_ast::BinaryOperator::Multiply,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(5))),
    };
    assert_expression_result(&db, expr, vibesql_types::SqlValue::Null);
}

#[test]
fn test_null_comparison() {
    let db = vibesql_storage::Database::new();
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
        op: vibesql_ast::BinaryOperator::Equal,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(5))),
    };
    assert_expression_result(&db, expr, vibesql_types::SqlValue::Null);
}
