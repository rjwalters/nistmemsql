//! Tests for string transformation functions
//!
//! This module tests:
//! - CONCAT: Concatenate multiple strings
//! - REPLACE: Replace all occurrences of substring
//! - TRIM: Remove leading/trailing characters
//!
//! Coverage includes NULL handling, empty strings, multibyte characters,
//! character type inputs, and error conditions.

use crate::common::create_test_evaluator;

// ============================================================================
// CONCAT Tests
// ============================================================================

#[test]
fn test_concat_null_propagation() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "CONCAT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Varchar("world".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_concat_empty_strings() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "CONCAT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("".to_string()));
}

#[test]
fn test_concat_single_arg() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "CONCAT".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("hello".to_string()));
}

#[test]
fn test_concat_many_args() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "CONCAT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("a".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("b".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("c".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("d".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("e".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("abcde".to_string()));
}

#[test]
fn test_concat_integer_conversion() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "CONCAT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("ID:".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(123)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("ID:123".to_string()));
}

#[test]
fn test_concat_no_args() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "CONCAT".to_string(),
        args: vec![],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_concat_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "CONCAT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Character("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Character(" ".to_string())),
            ast::Expression::Literal(types::SqlValue::Character("world".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("hello world".to_string()));
}

// ============================================================================
// REPLACE Tests
// ============================================================================

#[test]
fn test_replace_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "REPLACE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Varchar("a".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("b".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_replace_multiple_occurrences() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "REPLACE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("ll".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("rr".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("herro herro".to_string()));
}

#[test]
fn test_replace_empty_search() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "REPLACE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("x".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    // Empty string search inserts replacement between every character
    assert_eq!(result, types::SqlValue::Varchar("xhxexlxlxox".to_string()));
}

#[test]
fn test_replace_empty_replacement() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "REPLACE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("l".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("heo".to_string()));
}

#[test]
fn test_replace_not_found() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "REPLACE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("xyz".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("abc".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("hello".to_string()));
}

#[test]
fn test_replace_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "REPLACE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("l".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_replace_wrong_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "REPLACE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(123)),
            ast::Expression::Literal(types::SqlValue::Varchar("x".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_replace_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "REPLACE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Character("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Character("l".to_string())),
            ast::Expression::Literal(types::SqlValue::Character("r".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("herro".to_string()));
}

// ============================================================================
// TRIM Tests
// ============================================================================

#[test]
fn test_trim_null_removal_char() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Trim {
        position: Some(ast::TrimPosition::Both),
        removal_char: Some(Box::new(ast::Expression::Literal(types::SqlValue::Null))),
        string: Box::new(ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string()))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_trim_null_string() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Trim {
        position: None,
        removal_char: None,
        string: Box::new(ast::Expression::Literal(types::SqlValue::Null)),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_trim_custom_char_multibyte() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Trim {
        position: Some(ast::TrimPosition::Both),
        removal_char: Some(Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
            "x".to_string(),
        )))),
        string: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
            "xxxhelloxxx".to_string(),
        ))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("hello".to_string()));
}
