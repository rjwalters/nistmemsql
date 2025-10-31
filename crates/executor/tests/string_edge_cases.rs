//! Comprehensive edge case tests for string functions
//!
//! This test file covers:
//! - NULL handling for all string functions
//! - Unicode and multi-byte UTF-8 character handling
//! - Boundary conditions and edge cases
//! - Error conditions and invalid inputs
//!
//! Goal: Increase string.rs coverage from 45% to 80%+

mod common;

use common::create_test_evaluator;

// ============================================================================
// SUBSTRING Edge Cases
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

// ============================================================================
// POSITION Tests
// ============================================================================

#[test]
fn test_position_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "POSITION".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_position_not_found() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "POSITION".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("xyz".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(0));
}

#[test]
fn test_position_found() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "POSITION".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("lo".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(4)); // 1-indexed
}

#[test]
fn test_position_empty_needle() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "POSITION".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    // Empty string is found at position 1
    assert_eq!(result, types::SqlValue::Integer(1));
}

#[test]
fn test_position_multiple_occurrences() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "POSITION".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("l".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    // Returns first occurrence (position 3, 1-indexed)
    assert_eq!(result, types::SqlValue::Integer(3));
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

// ============================================================================
// LEFT/RIGHT Tests
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

// ============================================================================
// INSTR/LOCATE Tests
// ============================================================================

#[test]
fn test_instr_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "INSTR".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Varchar("lo".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_instr_not_found() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "INSTR".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("xyz".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(0));
}

#[test]
fn test_instr_found() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "INSTR".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("ll".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(3)); // 1-indexed
}

#[test]
fn test_locate_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LOCATE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_locate_not_found() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LOCATE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("xyz".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(0));
}

#[test]
fn test_locate_with_start_position() {
    let (evaluator, row) = create_test_evaluator();
    // Find second occurrence of 'l' in 'hello'
    let expr = ast::Expression::Function {
        name: "LOCATE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("l".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(4)), // Start after first 'l'
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(4)); // Found at position 4
}

#[test]
fn test_locate_start_beyond_length() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LOCATE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("l".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(100)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(0)); // Not found
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

// ============================================================================
// TRIM Advanced Tests (NULL removal character)
// ============================================================================

// ============================================================================
// Additional Error Condition Tests
// ============================================================================

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
fn test_position_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "POSITION".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_position_wrong_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "POSITION".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(123)),
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
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
fn test_instr_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "INSTR".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_instr_wrong_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "INSTR".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(123)),
            ast::Expression::Literal(types::SqlValue::Varchar("l".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_locate_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LOCATE".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("l".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_locate_wrong_type_needle() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LOCATE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(123)),
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_locate_wrong_type_start() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LOCATE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("l".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("one".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_locate_null_start() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LOCATE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("l".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Null),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
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

#[test]
fn test_position_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "POSITION".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Character("lo".to_string())),
            ast::Expression::Literal(types::SqlValue::Character("hello".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(4));
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

#[test]
fn test_instr_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "INSTR".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Character("hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Character("ll".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(3));
}

#[test]
fn test_locate_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LOCATE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Character("ll".to_string())),
            ast::Expression::Literal(types::SqlValue::Character("hello".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(3));
}
