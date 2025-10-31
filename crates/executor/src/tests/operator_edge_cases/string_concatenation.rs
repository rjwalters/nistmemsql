//! String concatenation edge case tests
//!
//! Tests for string concatenation including empty strings,
//! NULL handling, and complex multi-part concatenations.

use super::operator_test_utils::*;

#[test]
fn test_string_concat_basic() {
    let db = storage::Database::new();
    let expr = ast::Expression::BinaryOp {
        left: Box::new(ast::Expression::Literal(types::SqlValue::Varchar("Hello".to_string()))),
        op: ast::BinaryOperator::Concat,
        right: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(" World".to_string()))),
    };
    assert_expression_result(&db, expr, types::SqlValue::Varchar("Hello World".to_string()));
}

#[test]
fn test_string_concat_empty() {
    let db = storage::Database::new();
    let expr = ast::Expression::BinaryOp {
        left: Box::new(ast::Expression::Literal(types::SqlValue::Varchar("Hello".to_string()))),
        op: ast::BinaryOperator::Concat,
        right: Box::new(ast::Expression::Literal(types::SqlValue::Varchar("".to_string()))),
    };
    assert_expression_result(&db, expr, types::SqlValue::Varchar("Hello".to_string()));
}

#[test]
fn test_string_concat_null() {
    let db = storage::Database::new();
    let expr = ast::Expression::BinaryOp {
        left: Box::new(ast::Expression::Literal(types::SqlValue::Varchar("Hello".to_string()))),
        op: ast::BinaryOperator::Concat,
        right: Box::new(ast::Expression::Literal(types::SqlValue::Null)),
    };
    assert_expression_result(&db, expr, types::SqlValue::Null);
}

#[test]
fn test_string_concat_multiple() {
    let db = storage::Database::new();

    // "Hello" || " " || "Beautiful" || " " || "World"
    let expr = ast::Expression::BinaryOp {
        left: Box::new(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                        "Hello".to_string(),
                    ))),
                    op: ast::BinaryOperator::Concat,
                    right: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                        " ".to_string(),
                    ))),
                }),
                op: ast::BinaryOperator::Concat,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "Beautiful".to_string(),
                ))),
            }),
            op: ast::BinaryOperator::Concat,
            right: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                " ".to_string(),
            ))),
        }),
        op: ast::BinaryOperator::Concat,
        right: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
            "World".to_string(),
        ))),
    };

    assert_expression_result(&db, expr, types::SqlValue::Varchar("Hello Beautiful World".to_string()));
}
