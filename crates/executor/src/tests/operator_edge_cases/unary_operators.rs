//! Unary operator edge case tests
//!
//! Tests for unary plus and minus operators on various types,
//! including NULL propagation and type validation.

use super::operator_test_utils::*;

#[test]
fn test_unary_plus_integer() {
    let db = storage::Database::new();
    let expr = ast::Expression::UnaryOp {
        op: ast::UnaryOperator::Plus,
        expr: Box::new(ast::Expression::Literal(types::SqlValue::Integer(42))),
    };
    assert_expression_result(&db, expr, types::SqlValue::Integer(42));
}

#[test]
fn test_unary_plus_float() {
    let db = storage::Database::new();
    let expr = ast::Expression::UnaryOp {
        op: ast::UnaryOperator::Plus,
        expr: Box::new(ast::Expression::Literal(types::SqlValue::Float(3.14))),
    };
    assert_expression_result(&db, expr, types::SqlValue::Float(3.14));
}

#[test]
fn test_unary_minus_integer() {
    let db = storage::Database::new();
    let expr = ast::Expression::UnaryOp {
        op: ast::UnaryOperator::Minus,
        expr: Box::new(ast::Expression::Literal(types::SqlValue::Integer(42))),
    };
    assert_expression_result(&db, expr, types::SqlValue::Integer(-42));
}

#[test]
fn test_unary_minus_negative() {
    let db = storage::Database::new();
    let expr = ast::Expression::UnaryOp {
        op: ast::UnaryOperator::Minus,
        expr: Box::new(ast::Expression::Literal(types::SqlValue::Integer(-42))),
    };
    assert_expression_result(&db, expr, types::SqlValue::Integer(42));
}

#[test]
fn test_unary_minus_numeric_string() {
    let db = storage::Database::new();
    let expr = ast::Expression::UnaryOp {
        op: ast::UnaryOperator::Minus,
        expr: Box::new(ast::Expression::Literal(types::SqlValue::Numeric("123.45".to_string()))),
    };
    assert_expression_result(&db, expr, types::SqlValue::Numeric("-123.45".to_string()));
}

#[test]
fn test_unary_minus_negative_numeric() {
    let db = storage::Database::new();
    let expr = ast::Expression::UnaryOp {
        op: ast::UnaryOperator::Minus,
        expr: Box::new(ast::Expression::Literal(types::SqlValue::Numeric("-123.45".to_string()))),
    };
    assert_expression_result(&db, expr, types::SqlValue::Numeric("123.45".to_string()));
}

#[test]
fn test_unary_plus_null() {
    let db = storage::Database::new();
    let expr = ast::Expression::UnaryOp {
        op: ast::UnaryOperator::Plus,
        expr: Box::new(ast::Expression::Literal(types::SqlValue::Null)),
    };
    assert_expression_result(&db, expr, types::SqlValue::Null);
}

#[test]
fn test_unary_minus_null() {
    let db = storage::Database::new();
    let expr = ast::Expression::UnaryOp {
        op: ast::UnaryOperator::Minus,
        expr: Box::new(ast::Expression::Literal(types::SqlValue::Null)),
    };
    assert_expression_result(&db, expr, types::SqlValue::Null);
}

#[test]
fn test_unary_plus_invalid_type() {
    let db = storage::Database::new();
    let expr = ast::Expression::UnaryOp {
        op: ast::UnaryOperator::Plus,
        expr: Box::new(ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string()))),
    };
    assert_type_mismatch(&db, expr);
}

#[test]
fn test_unary_minus_invalid_type() {
    let db = storage::Database::new();
    let expr = ast::Expression::UnaryOp {
        op: ast::UnaryOperator::Minus,
        expr: Box::new(ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string()))),
    };
    assert_type_mismatch(&db, expr);
}
