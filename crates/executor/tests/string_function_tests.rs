//! Tests for string functions, particularly SUBSTRING syntax variants

mod common;

use ast;
use common::create_test_evaluator;
use types;

// ============================================================================
// SUBSTRING Function Tests
// ============================================================================

#[test]
fn test_substring_from_for() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(2)),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("ell".to_string()));
}

#[test]
fn test_substring_from_only() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(2)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("ello".to_string()));
}

#[test]
fn test_substring_both_syntaxes_equivalent() {
    let (evaluator, row) = create_test_evaluator();

    // Test comma syntax
    let comma_expr = ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(2)),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let comma_result = evaluator.eval(&comma_expr, &row).unwrap();

    // Test FROM/FOR syntax (same AST)
    let from_for_expr = ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(2)),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let from_for_result = evaluator.eval(&from_for_expr, &row).unwrap();

    assert_eq!(comma_result, from_for_result);
    assert_eq!(comma_result, types::SqlValue::Varchar("ell".to_string()));
}
