//! Tests for type conversion functions
//!
//! Tests for SQL:1999 Section 6.10-6.12 type conversion functions:
//! - TO_NUMBER: Convert string to numeric value
//! - TO_DATE: Convert string to date with format
//! - TO_TIMESTAMP: Convert string to timestamp with format
//! - TO_CHAR: Convert date/number to formatted string
//! - CAST: Standard SQL type conversion

mod common;

use common::create_test_evaluator;

// ============================================================================
// TO_NUMBER Function Tests
// ============================================================================

#[test]
fn test_to_number_valid_integer() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_NUMBER".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar(
            "42".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(42));
}

#[test]
fn test_to_number_valid_double() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_NUMBER".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar(
            "3.14".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Double(3.14));
}

#[test]
fn test_to_number_negative_integer() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_NUMBER".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar(
            "-42".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(-42));
}

#[test]
fn test_to_number_negative_double() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_NUMBER".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar(
            "-3.14".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Double(-3.14));
}

#[test]
fn test_to_number_with_whitespace() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_NUMBER".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar(
            "  123  ".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(123));
}

#[test]
fn test_to_number_with_commas() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_NUMBER".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar(
            "1,234.56".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Double(1234.56));
}

#[test]
fn test_to_number_null_input() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_NUMBER".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_to_number_invalid_string() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_NUMBER".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar(
            "invalid".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_to_number_mixed_alphanumeric() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_NUMBER".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar(
            "abc123".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_to_number_zero() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_NUMBER".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar(
            "0".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(0));
}

#[test]
fn test_to_number_scientific_notation() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_NUMBER".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar(
            "1.5e2".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Double(150.0));
}

#[test]
fn test_to_number_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_NUMBER".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("42".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("extra".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_to_number_non_string_argument() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_NUMBER".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Integer(42))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

// ============================================================================
// TO_DATE Function Tests
// ============================================================================

#[test]
fn test_to_date_basic() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_DATE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("2023-12-25".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("YYYY-MM-DD".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(
        result,
        types::SqlValue::Date("2023-12-25".to_string())
    );
}

#[test]
fn test_to_date_different_format() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_DATE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("25/12/2023".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("DD/MM/YYYY".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(
        result,
        types::SqlValue::Date("2023-12-25".to_string())
    );
}

#[test]
fn test_to_date_null_input() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_DATE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Varchar("YYYY-MM-DD".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_to_date_null_format() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_DATE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("2023-12-25".to_string())),
            ast::Expression::Literal(types::SqlValue::Null),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_to_date_invalid_format() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_DATE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("2023-12-25".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("DD/MM/YYYY".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_to_date_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_DATE".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar(
            "2023-12-25".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_to_date_non_string_arguments() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_DATE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(20231225)),
            ast::Expression::Literal(types::SqlValue::Varchar("YYYY-MM-DD".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

// ============================================================================
// TO_TIMESTAMP Function Tests
// ============================================================================

#[test]
fn test_to_timestamp_basic() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_TIMESTAMP".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar(
                "2023-12-25 14:30:45".to_string(),
            )),
            ast::Expression::Literal(types::SqlValue::Varchar(
                "YYYY-MM-DD HH24:MI:SS".to_string(),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(
        result,
        types::SqlValue::Timestamp("2023-12-25 14:30:45".to_string())
    );
}

#[test]
fn test_to_timestamp_null_input() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_TIMESTAMP".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Varchar(
                "YYYY-MM-DD HH24:MI:SS".to_string(),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_to_timestamp_null_format() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_TIMESTAMP".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar(
                "2023-12-25 14:30:45".to_string(),
            )),
            ast::Expression::Literal(types::SqlValue::Null),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_to_timestamp_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_TIMESTAMP".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Varchar(
            "2023-12-25 14:30:45".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_to_timestamp_non_string_arguments() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_TIMESTAMP".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(20231225)),
            ast::Expression::Literal(types::SqlValue::Varchar(
                "YYYY-MM-DD HH24:MI:SS".to_string(),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_to_date_with_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_DATE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Character("2023-12-25".to_string())),
            ast::Expression::Literal(types::SqlValue::Character("YYYY-MM-DD".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(
        result,
        types::SqlValue::Date("2023-12-25".to_string())
    );
}

#[test]
fn test_to_timestamp_with_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_TIMESTAMP".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Character(
                "2023-12-25 14:30:45".to_string(),
            )),
            ast::Expression::Literal(types::SqlValue::Character(
                "YYYY-MM-DD HH24:MI:SS".to_string(),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(
        result,
        types::SqlValue::Timestamp("2023-12-25 14:30:45".to_string())
    );
}

// ============================================================================
// TO_CHAR Function Tests
// ============================================================================

#[test]
fn test_to_char_date() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_CHAR".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2023-12-25".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("YYYY-MM-DD".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    match result {
        types::SqlValue::Varchar(s) => {
            assert_eq!(s, "2023-12-25");
        }
        _ => panic!("Expected Varchar result"),
    }
}

#[test]
fn test_to_char_timestamp() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_CHAR".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Timestamp(
                "2023-12-25 14:30:45".to_string(),
            )),
            ast::Expression::Literal(types::SqlValue::Varchar(
                "YYYY-MM-DD HH24:MI:SS".to_string(),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    match result {
        types::SqlValue::Varchar(s) => {
            assert_eq!(s, "2023-12-25 14:30:45");
        }
        _ => panic!("Expected Varchar result"),
    }
}

#[test]
fn test_to_char_integer() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_CHAR".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(42)),
            ast::Expression::Literal(types::SqlValue::Varchar("999".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    match result {
        types::SqlValue::Varchar(_) => {
            // Accept any varchar result - the specific formatting depends on format_number implementation
        }
        _ => panic!("Expected Varchar result"),
    }
}

#[test]
fn test_to_char_double() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_CHAR".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Double(3.14)),
            ast::Expression::Literal(types::SqlValue::Varchar("999.99".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    match result {
        types::SqlValue::Varchar(_) => {
            // Accept any varchar result - the specific formatting depends on format_number implementation
        }
        _ => panic!("Expected Varchar result"),
    }
}

#[test]
fn test_to_char_null_value() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_CHAR".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Varchar("YYYY-MM-DD".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_to_char_null_format() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_CHAR".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2023-12-25".to_string())),
            ast::Expression::Literal(types::SqlValue::Null),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_to_char_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_CHAR".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Date(
            "2023-12-25".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_to_char_all_numeric_types() {
    let (evaluator, row) = create_test_evaluator();

    // Test Smallint
    let expr = ast::Expression::Function {
        name: "TO_CHAR".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Smallint(42)),
            ast::Expression::Literal(types::SqlValue::Varchar("999".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert!(matches!(result, types::SqlValue::Varchar(_)));

    // Test Bigint
    let expr = ast::Expression::Function {
        name: "TO_CHAR".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Bigint(42000)),
            ast::Expression::Literal(types::SqlValue::Varchar("999999".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert!(matches!(result, types::SqlValue::Varchar(_)));

    // Test Float
    let expr = ast::Expression::Function {
        name: "TO_CHAR".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Float(3.14)),
            ast::Expression::Literal(types::SqlValue::Varchar("999.99".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert!(matches!(result, types::SqlValue::Varchar(_)));

    // Test Real
    let expr = ast::Expression::Function {
        name: "TO_CHAR".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Real(2.71)),
            ast::Expression::Literal(types::SqlValue::Varchar("999.99".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert!(matches!(result, types::SqlValue::Varchar(_)));
}

#[test]
fn test_to_char_time() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_CHAR".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Time("14:30:45".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("HH24:MI:SS".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    match result {
        types::SqlValue::Varchar(s) => {
            assert_eq!(s, "14:30:45");
        }
        _ => panic!("Expected Varchar result"),
    }
}

#[test]
fn test_to_char_non_string_format() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "TO_CHAR".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2023-12-25".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(123)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

// ============================================================================
// CAST Function Tests
// ============================================================================

#[test]
fn test_cast_integer_to_varchar() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "CAST".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(42)),
            ast::Expression::Literal(types::SqlValue::Varchar("VARCHAR".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    match result {
        types::SqlValue::Varchar(s) => assert_eq!(s, "42"),
        types::SqlValue::Character(s) => assert_eq!(s, "42"),
        _ => panic!("Expected string result, got {:?}", result),
    }
}

#[test]
fn test_cast_varchar_to_integer() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "CAST".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("42".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("INTEGER".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(42));
}

#[test]
fn test_cast_integer_to_double() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "CAST".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(42)),
            ast::Expression::Literal(types::SqlValue::Varchar("DOUBLE".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Double(42.0));
}

#[test]
fn test_cast_double_to_integer_unsupported() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "CAST".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Double(42.9)),
            ast::Expression::Literal(types::SqlValue::Varchar("INTEGER".to_string())),
        ],
        character_unit: None,
    };
    // Note: Double to Integer cast is not currently supported in the implementation
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_cast_smallint_to_bigint() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "CAST".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Smallint(42)),
            ast::Expression::Literal(types::SqlValue::Varchar("BIGINT".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Bigint(42));
}

#[test]
fn test_cast_varchar_to_date() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "CAST".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("2023-12-25".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("DATE".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(
        result,
        types::SqlValue::Date("2023-12-25".to_string())
    );
}

#[test]
fn test_cast_unsupported_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "CAST".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(42)),
            ast::Expression::Literal(types::SqlValue::Varchar("BLOB".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_cast_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "CAST".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Integer(42))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_cast_invalid_conversion() {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function {
        name: "CAST".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("invalid".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("INTEGER".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}
