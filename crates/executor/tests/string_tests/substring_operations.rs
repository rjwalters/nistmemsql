//! Test suite for substring operations (SUBSTRING, LEFT, RIGHT)
//!
//! Tests cover:
//! - NULL handling (NULL string, NULL position, NULL length)
//! - Empty string handling
//! - Boundary conditions (negative/zero positions, lengths exceeding string)
//! - Multi-byte UTF-8 character handling
//! - Both VARCHAR and CHARACTER data types
//! - Error conditions (wrong argument count, wrong type)

use crate::common::create_test_evaluator;

// ============================================================================
// SUBSTRING Tests
// ============================================================================

#[test]
fn test_substring_null_string() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Integer(1)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_substring_null_start() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Null),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_substring_null_length() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Null),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_substring_negative_start() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(-5)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    // SQL:1999 treats negative/zero start as position 1
    assert_eq!(result, types::SqlValue::Varchar("hello".to_string()));
}

#[test]
fn test_substring_zero_start() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(0)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("hello".to_string()));
}

#[test]
fn test_substring_start_beyond_length() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(100)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("".to_string()));
}

#[test]
fn test_substring_zero_length() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(2)),
            ast::Expression::Literal(types::SqlValue::Integer(0)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("".to_string()));
}

#[test]
fn test_substring_negative_length() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(2)),
            ast::Expression::Literal(types::SqlValue::Integer(-3)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("".to_string()));
}

#[test]
fn test_substring_multibyte_characters() {
    let (evaluator, row) = create_test_evaluator();
    // "café" is 4 characters but 5 bytes in UTF-8
    let expr = ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("café".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("caf".to_string()));
}

#[test]
fn test_substring_empty_string() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(1)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("".to_string()));
}

#[test]
fn test_substring_wrong_arg_count_too_few() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_substring_wrong_arg_count_too_many() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Integer(2)),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_substring_wrong_type_string() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(123)),
            ast::Expression::Literal(types::SqlValue::Integer(1)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_substring_wrong_type_start() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("one".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_substring_wrong_type_length() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Varchar("two".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

// ============================================================================
// LEFT Tests
// ============================================================================

#[test]
fn test_left_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LEFT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_left_negative_length() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LEFT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(-5)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("".to_string()));
}

#[test]
fn test_left_zero_length() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LEFT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(0)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("".to_string()));
}

#[test]
fn test_left_length_exceeds_string() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LEFT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(100)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("hello".to_string()));
}

#[test]
fn test_left_multibyte() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LEFT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("café".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("caf".to_string()));
}

#[test]
fn test_left_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LEFT".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_left_wrong_type_string() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LEFT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(123)),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_left_wrong_type_length() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LEFT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("three".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_left_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LEFT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Character("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("hel".to_string()));
}

// ============================================================================
// RIGHT Tests
// ============================================================================

#[test]
fn test_right_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "RIGHT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_right_negative_length() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "RIGHT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(-5)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("".to_string()));
}

#[test]
fn test_right_zero_length() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "RIGHT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(0)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("".to_string()));
}

#[test]
fn test_right_length_exceeds_string() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "RIGHT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(100)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("hello".to_string()));
}

#[test]
fn test_right_multibyte() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "RIGHT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("café".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("afé".to_string()));
}

#[test]
fn test_right_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "RIGHT".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_right_wrong_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "RIGHT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(123)),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_right_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "RIGHT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Character("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("llo".to_string()));
}
