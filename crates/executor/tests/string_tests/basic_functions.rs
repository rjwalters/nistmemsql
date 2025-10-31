//! Tests for basic string transformation functions
//!
//! This module covers:
//! - UPPER/LOWER - Case conversion
//! - LENGTH/CHAR_LENGTH/OCTET_LENGTH - String length functions
//! - REVERSE - String reversal
//!
//! Each function is tested for:
//! - NULL handling
//! - Empty strings
//! - Unicode/multi-byte UTF-8 characters
//! - Type conversions (VARCHAR vs CHARACTER)
//! - Error conditions (wrong argument count, wrong types)

use crate::common::create_test_evaluator;

// ============================================================================
// UPPER/LOWER Function Tests
// ============================================================================

#[test]
fn test_upper_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "UPPER".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_upper_empty() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "UPPER".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("".to_string()));
}

#[test]
fn test_upper_unicode() {
    let (evaluator, row) = create_test_evaluator();
    // Test Greek, accented characters, emojis
    let expr = ast::Expression::Function {
        name: "UPPER".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("café".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("CAFÉ".to_string()));
}

#[test]
fn test_upper_mixed_case() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "UPPER".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("HeLLo WoRLd".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("HELLO WORLD".to_string()));
}

#[test]
fn test_upper_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "UPPER".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Character("test".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("TEST".to_string()));
}

#[test]
fn test_upper_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "UPPER".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("test".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("extra".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_upper_wrong_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "UPPER".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Integer(123))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_lower_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LOWER".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_lower_empty() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LOWER".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("".to_string()));
}

#[test]
fn test_lower_unicode() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LOWER".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("CAFÉ".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("café".to_string()));
}

#[test]
fn test_lower_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LOWER".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("TEST".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("extra".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_lower_wrong_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LOWER".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Integer(123))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_lower_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LOWER".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Character("TEST".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("test".to_string()));
}

// ============================================================================
// CHAR_LENGTH and OCTET_LENGTH Tests
// ============================================================================

#[test]
fn test_char_length_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "CHAR_LENGTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_char_length_empty() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "CHAR_LENGTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(0));
}

#[test]
fn test_char_length_multibyte() {
    let (evaluator, row) = create_test_evaluator();
    // "café" is 4 characters but 5 bytes
    let expr = ast::Expression::Function {
        name: "CHAR_LENGTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("café".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(4));
}

#[test]
fn test_char_length_using_characters() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "CHAR_LENGTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("café".to_string()))],
        character_unit: Some(ast::CharacterUnit::Characters),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(4));
}

#[test]
fn test_char_length_using_octets() {
    let (evaluator, row) = create_test_evaluator();
    // "café" is 5 bytes in UTF-8 (c=1, a=1, f=1, é=2)
    let expr = ast::Expression::Function {
        name: "CHAR_LENGTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("café".to_string()))],
        character_unit: Some(ast::CharacterUnit::Octets),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(5));
}

#[test]
fn test_char_length_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "CHAR_LENGTH".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("extra".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_char_length_wrong_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "CHAR_LENGTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Integer(123))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_char_length_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "CHAR_LENGTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Character("test".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(4));
}

#[test]
fn test_octet_length_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "OCTET_LENGTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_octet_length_empty() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "OCTET_LENGTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(0));
}

#[test]
fn test_octet_length_multibyte() {
    let (evaluator, row) = create_test_evaluator();
    // "café" is 5 bytes in UTF-8
    let expr = ast::Expression::Function {
        name: "OCTET_LENGTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("café".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(5));
}

#[test]
fn test_octet_length_vs_char_length() {
    let (evaluator, row) = create_test_evaluator();

    // ASCII: same count
    let ascii_expr = ast::Expression::Function {
        name: "CHAR_LENGTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string()))],
        character_unit: None,
    };
    let char_result = evaluator.eval(&ascii_expr, &row).unwrap();

    let octet_expr = ast::Expression::Function {
        name: "OCTET_LENGTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string()))],
        character_unit: None,
    };
    let octet_result = evaluator.eval(&octet_expr, &row).unwrap();

    assert_eq!(char_result, types::SqlValue::Integer(5));
    assert_eq!(octet_result, types::SqlValue::Integer(5));
}

#[test]
fn test_octet_length_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "OCTET_LENGTH".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("extra".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_octet_length_wrong_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "OCTET_LENGTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Integer(123))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_octet_length_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "OCTET_LENGTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Character("café".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(5));
}

// ============================================================================
// REVERSE Tests
// ============================================================================

#[test]
fn test_reverse_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "REVERSE".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_reverse_empty() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "REVERSE".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("".to_string()));
}

#[test]
fn test_reverse_single_char() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "REVERSE".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("a".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("a".to_string()));
}

#[test]
fn test_reverse_basic() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "REVERSE".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("olleh".to_string()));
}

#[test]
fn test_reverse_multibyte() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "REVERSE".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("café".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("éfac".to_string()));
}

#[test]
fn test_reverse_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "REVERSE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("extra".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_reverse_wrong_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "REVERSE".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Integer(123))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_reverse_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "REVERSE".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Character("test".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("tset".to_string()));
}

// ============================================================================
// LENGTH Tests
// ============================================================================

#[test]
fn test_length_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LENGTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_length_empty() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LENGTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(0));
}

#[test]
fn test_length_basic() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LENGTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(5));
}

#[test]
fn test_length_multibyte() {
    let (evaluator, row) = create_test_evaluator();
    // LENGTH returns byte count (unlike CHAR_LENGTH)
    let expr = ast::Expression::Function {
        name: "LENGTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("café".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    // "café" is 5 bytes in UTF-8
    assert_eq!(result, types::SqlValue::Integer(5));
}

#[test]
fn test_length_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LENGTH".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("extra".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_length_wrong_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LENGTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Integer(123))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_length_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LENGTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Character("test".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(4));
}
