//! Utility and System Function Tests - SQL CORE Phase 3D
//!
//! Tests for:
//! - String utilities: SUBSTR, INSTR, LOCATE, FORMAT
//! - System functions: VERSION, DATABASE, USER

mod common;

use ast;
use common::create_test_evaluator;
use types;

// ============================================================================
// SUBSTR / SUBSTRING Tests
// ============================================================================

#[test]
fn test_substr_basic() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "SUBSTR".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("Hello World".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Integer(5)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("Hello".to_string()));
}

#[test]
fn test_substr_to_end() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "SUBSTR".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("Hello World".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(7)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("World".to_string()));
}

#[test]
fn test_substr_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "SUBSTR".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Integer(1)),
        ],
        character_unit: None,
    };
    assert_eq!(evaluator.eval(&expr, &row).unwrap(), types::SqlValue::Null);
}

// ============================================================================
// INSTR Tests
// ============================================================================

#[test]
fn test_instr_found() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "INSTR".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("Hello World".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("World".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(7));
}

#[test]
fn test_instr_not_found() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "INSTR".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("Hello World".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("xyz".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(0));
}

#[test]
fn test_instr_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "INSTR".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Varchar("test".to_string())),
        ],
        character_unit: None,
    };
    assert_eq!(evaluator.eval(&expr, &row).unwrap(), types::SqlValue::Null);
}

// ============================================================================
// LOCATE Tests
// ============================================================================

#[test]
fn test_locate_basic() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LOCATE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("World".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("Hello World".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(7));
}

#[test]
fn test_locate_with_start() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "LOCATE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("o".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("Hello World".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(6)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(8)); // Second 'o' at position 8
}

// ============================================================================
// FORMAT Tests
// ============================================================================

#[test]
fn test_format_basic() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "FORMAT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Double(1234567.89)),
            ast::Expression::Literal(types::SqlValue::Integer(2)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("1,234,567.89".to_string()));
}

#[test]
fn test_format_zero_decimals() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "FORMAT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Double(1234567.89)),
            ast::Expression::Literal(types::SqlValue::Integer(0)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("1,234,568".to_string())); // Rounds
}

#[test]
fn test_format_adds_zeros() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "FORMAT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(42)),
            ast::Expression::Literal(types::SqlValue::Integer(2)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("42.00".to_string()));
}

#[test]
fn test_format_negative() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "FORMAT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Double(-1234567.89)),
            ast::Expression::Literal(types::SqlValue::Integer(2)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("-1,234,567.89".to_string()));
}

// ============================================================================
// VERSION Tests
// ============================================================================

#[test]
fn test_version() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function { name: "VERSION".to_string(), args: vec![], character_unit: None };
    let result = evaluator.eval(&expr, &row).unwrap();
    match result {
        types::SqlValue::Varchar(s) => {
            assert!(
                s.starts_with("NistMemSQL"),
                "VERSION should start with 'NistMemSQL', got: {}",
                s
            );
        }
        _ => panic!("VERSION should return Varchar"),
    }
}

// ============================================================================
// DATABASE / SCHEMA Tests
// ============================================================================

#[test]
fn test_database() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function { name: "DATABASE".to_string(), args: vec![], character_unit: None };
    let result = evaluator.eval(&expr, &row).unwrap();
    match result {
        types::SqlValue::Varchar(s) => {
            assert!(!s.is_empty(), "DATABASE should return non-empty string");
        }
        types::SqlValue::Null => {
            // NULL is also acceptable if no database selected
        }
        _ => panic!("DATABASE should return Varchar or Null"),
    }
}

#[test]
fn test_schema_alias() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function { name: "SCHEMA".to_string(), args: vec![], character_unit: None };
    let result = evaluator.eval(&expr, &row).unwrap();
    // SCHEMA is an alias for DATABASE
    assert!(matches!(result, types::SqlValue::Varchar(_) | types::SqlValue::Null));
}

// ============================================================================
// USER / CURRENT_USER Tests
// ============================================================================

#[test]
fn test_user() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function { name: "USER".to_string(), args: vec![], character_unit: None };
    let result = evaluator.eval(&expr, &row).unwrap();
    match result {
        types::SqlValue::Varchar(s) => {
            assert!(!s.is_empty(), "USER should return non-empty string");
        }
        _ => panic!("USER should return Varchar"),
    }
}

#[test]
fn test_current_user_alias() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function { name: "CURRENT_USER".to_string(), args: vec![], character_unit: None };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert!(matches!(result, types::SqlValue::Varchar(_)));
}
