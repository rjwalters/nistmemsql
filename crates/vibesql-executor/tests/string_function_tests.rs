//! Tests for string functions, particularly SUBSTRING syntax variants

mod common;

use common::create_test_evaluator;

// ============================================================================
// SUBSTRING Function Tests
// ============================================================================

#[test]
fn test_substring_from_for() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("hello".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("ell".to_string()));
}

#[test]
fn test_substring_from_only() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("hello".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("ello".to_string()));
}

#[test]
fn test_substring_both_syntaxes_equivalent() {
    let (evaluator, row) = create_test_evaluator();

    // Test comma syntax
    let comma_expr = vibesql_ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("hello".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let comma_result = evaluator.eval(&comma_expr, &row).unwrap();

    // Test FROM/FOR syntax (same AST)
    let from_for_expr = vibesql_ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("hello".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let from_for_result = evaluator.eval(&from_for_expr, &row).unwrap();

    assert_eq!(comma_result, from_for_result);
    assert_eq!(comma_result, vibesql_types::SqlValue::Varchar("ell".to_string()));
}

// ============================================================================
// TRIM Function Tests
// ============================================================================

#[test]
fn test_trim_from_no_char() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Trim {
        position: None,     // Defaults to BOTH
        removal_char: None, // Defaults to space
        string: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "  hello  ".to_string(),
        ))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("hello".to_string()));
}

#[test]
fn test_trim_both_from_no_char() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Trim {
        position: Some(vibesql_ast::TrimPosition::Both),
        removal_char: None, // Defaults to space
        string: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "  hello  ".to_string(),
        ))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("hello".to_string()));
}

#[test]
fn test_trim_leading_from_no_char() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Trim {
        position: Some(vibesql_ast::TrimPosition::Leading),
        removal_char: None, // Defaults to space
        string: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "  hello  ".to_string(),
        ))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("hello  ".to_string()));
}

#[test]
fn test_trim_trailing_from_no_char() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Trim {
        position: Some(vibesql_ast::TrimPosition::Trailing),
        removal_char: None, // Defaults to space
        string: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "  hello  ".to_string(),
        ))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("  hello".to_string()));
}

#[test]
fn test_trim_from_only_spaces() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Trim {
        position: None,
        removal_char: None, // Defaults to space
        string: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("    ".to_string()))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("".to_string()));
}

#[test]
fn test_trim_from_empty_string() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Trim {
        position: None,
        removal_char: None, // Defaults to space
        string: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("".to_string()))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("".to_string()));
}

#[test]
fn test_trim_from_no_spaces() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Trim {
        position: None,
        removal_char: None, // Defaults to space
        string: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("hello".to_string()))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("hello".to_string()));
}

#[test]
fn test_trim_with_char_still_works() {
    // Verify existing functionality is preserved
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Trim {
        position: Some(vibesql_ast::TrimPosition::Both),
        removal_char: Some(Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "x".to_string(),
        )))),
        string: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("xfoox".to_string()))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("foo".to_string()));
}
