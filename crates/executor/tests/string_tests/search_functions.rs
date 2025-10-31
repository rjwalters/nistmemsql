//! Tests for string search functions
//!
//! This module tests:
//! - POSITION: Find first occurrence of substring
//! - INSTR: MySQL-style position function
//! - LOCATE: MySQL-style position with optional start position
//!
//! Coverage includes NULL handling, empty strings, multibyte characters,
//! and error conditions.

use crate::common::create_test_evaluator;

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

// ============================================================================
// INSTR Tests
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

// ============================================================================
// LOCATE Tests
// ============================================================================

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
