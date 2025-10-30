//! Tests for new scalar functions (numeric and string functions)

// Allow approximate constants in tests - these are test data values, not mathematical constants
#![allow(clippy::approx_constant)]

mod common;

use common::create_test_evaluator;

// ==================== NUMERIC FUNCTION TESTS ====================

#[test]
fn test_abs_positive() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "ABS".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Double(-5.2))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Double(5.2));
}

#[test]
fn test_abs_integer() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "ABS".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Integer(-42))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(42));
}

#[test]
fn test_round_basic() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "ROUND".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Double(3.7))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Double(4.0));
}

#[test]
fn test_round_with_precision() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "ROUND".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Double(3.14159)),
            ast::Expression::Literal(types::SqlValue::Integer(2)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Double(3.14));
}

#[test]
fn test_floor_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "FLOOR".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Double(3.9))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Double(3.0));
}

#[test]
fn test_ceil_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "CEIL".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Double(3.1))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Double(4.0));
}

#[test]
fn test_ceiling_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "CEILING".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Double(3.1))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Double(4.0));
}

#[test]
fn test_mod_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "MOD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(17)),
            ast::Expression::Literal(types::SqlValue::Integer(5)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(2));
}

#[test]
fn test_power_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "POWER".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(2)),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Double(8.0));
}

#[test]
fn test_pow_alias() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "POW".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(5)),
            ast::Expression::Literal(types::SqlValue::Integer(2)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Double(25.0));
}

#[test]
fn test_sqrt_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "SQRT".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Integer(16))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Double(4.0));
}

// ==================== STRING FUNCTION TESTS ====================

#[test]
fn test_concat_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "CONCAT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("Hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar(" ".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("World".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("Hello World".to_string()));
}

#[test]
fn test_concat_with_integer() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "CONCAT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("ID:".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(42)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("ID:42".to_string()));
}

#[test]
fn test_length_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "LENGTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("Hello".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(5));
}

#[test]
fn test_position_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "POSITION".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("lo".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("Hello".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(4)); // 'lo' starts at position 4 (1-indexed)
}

#[test]
fn test_position_not_found() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "POSITION".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("xyz".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("Hello".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(0)); // Not found returns 0
}

#[test]
fn test_replace_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "REPLACE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("Hello World".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("World".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("Rust".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("Hello Rust".to_string()));
}

#[test]
fn test_reverse_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "REVERSE".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("Hello".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("olleH".to_string()));
}

#[test]
fn test_left_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "LEFT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("Hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("Hel".to_string()));
}

#[test]
fn test_right_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "RIGHT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("Hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("llo".to_string()));
}

// ==================== NULL HANDLING TESTS ====================

#[test]
fn test_abs_with_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "ABS".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_concat_with_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "CONCAT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("Hello".to_string())),
            ast::Expression::Literal(types::SqlValue::Null),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null); // NULL in concat makes result NULL
}

#[test]
fn test_length_with_null() {
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
fn test_octet_length_ascii() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "OCTET_LENGTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("foo".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(3));
}

#[test]
fn test_octet_length_empty_string() {
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

    // Emoji is 4 bytes in UTF-8
    let expr = ast::Expression::Function {
        name: "OCTET_LENGTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar("🦀".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(4));
}

#[test]
fn test_octet_length_with_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "OCTET_LENGTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

// ==================== NESTED FUNCTION TESTS ====================

#[test]
fn test_nested_functions() {
    let (evaluator, row) = create_test_evaluator();

    // ABS(ROUND(-3.7)) should equal 4.0
    let expr = ast::Expression::Function {
        name: "ABS".to_string(),
        args: vec![ast::Expression::Function {
            name: "ROUND".to_string(),
            args: vec![ast::Expression::Literal(types::SqlValue::Double(-3.7))],
            character_unit: None,
        }],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Double(4.0));
}

#[test]
fn test_upper_left_nested() {
    let (evaluator, row) = create_test_evaluator();

    // UPPER(LEFT('hello', 3)) should equal 'HEL'
    let expr = ast::Expression::Function {
        name: "UPPER".to_string(),
        args: vec![ast::Expression::Function {
            name: "LEFT".to_string(),
            args: vec![
                ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
                ast::Expression::Literal(types::SqlValue::Integer(3)),
            ],
            character_unit: None,
        }],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("HEL".to_string()));
}
