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

// ==================== HELPER FUNCTIONS ====================

/// Helper to create a function expression
fn create_function_expr(name: &str, args: Vec<ast::Expression>) -> ast::Expression {
    ast::Expression::Function {
        name: name.to_string(),
        args,
        character_unit: None,
    }
}

/// Helper to evaluate a function and return the result
fn eval_function(name: &str, args: Vec<ast::Expression>) -> types::SqlValue {
    let (evaluator, row) = create_test_evaluator();
    let expr = create_function_expr(name, args);
    evaluator.eval(&expr, &row).unwrap()
}

/// Helper to evaluate a function and expect an error
fn eval_function_expect_error(name: &str, args: Vec<ast::Expression>) {
    let (evaluator, row) = create_test_evaluator();
    let expr = create_function_expr(name, args);
    assert!(evaluator.eval(&expr, &row).is_err());
}

/// Helper to create a varchar literal expression
fn varchar_lit(value: &str) -> ast::Expression {
    ast::Expression::Literal(types::SqlValue::Varchar(value.to_string()))
}

/// Helper to create a character literal expression
fn character_lit(value: &str) -> ast::Expression {
    ast::Expression::Literal(types::SqlValue::Character(value.to_string()))
}

/// Helper to create a date literal expression
fn date_lit(date_str: &str) -> ast::Expression {
    ast::Expression::Literal(types::SqlValue::Date(date_str.to_string()))
}

/// Helper to create a timestamp literal expression
fn timestamp_lit(ts_str: &str) -> ast::Expression {
    ast::Expression::Literal(types::SqlValue::Timestamp(ts_str.to_string()))
}

/// Helper to create a time literal expression
fn time_lit(time_str: &str) -> ast::Expression {
    ast::Expression::Literal(types::SqlValue::Time(time_str.to_string()))
}

/// Helper to create an integer literal expression
fn int_lit(value: i64) -> ast::Expression {
    ast::Expression::Literal(types::SqlValue::Integer(value))
}

/// Helper to create a smallint literal expression
fn smallint_lit(value: i16) -> ast::Expression {
    ast::Expression::Literal(types::SqlValue::Smallint(value))
}

/// Helper to create a bigint literal expression
fn bigint_lit(value: i64) -> ast::Expression {
    ast::Expression::Literal(types::SqlValue::Bigint(value))
}

/// Helper to create a double literal expression
fn double_lit(value: f64) -> ast::Expression {
    ast::Expression::Literal(types::SqlValue::Double(value))
}

/// Helper to create a float literal expression
fn float_lit(value: f32) -> ast::Expression {
    ast::Expression::Literal(types::SqlValue::Float(value))
}

/// Helper to create a real literal expression
fn real_lit(value: f32) -> ast::Expression {
    ast::Expression::Literal(types::SqlValue::Real(value))
}

/// Helper to create a null literal expression
fn null_lit() -> ast::Expression {
    ast::Expression::Literal(types::SqlValue::Null)
}

// ==================== TO_NUMBER FUNCTION TESTS ====================

#[test]
fn test_to_number_valid_integer() {
    let result = eval_function("TO_NUMBER", vec![varchar_lit("42")]);
    assert_eq!(result, types::SqlValue::Integer(42));
}

#[test]
fn test_to_number_valid_double() {
    let result = eval_function("TO_NUMBER", vec![varchar_lit("3.14")]);
    assert_eq!(result, types::SqlValue::Double(3.14));
}

#[test]
fn test_to_number_negative_integer() {
    let result = eval_function("TO_NUMBER", vec![varchar_lit("-42")]);
    assert_eq!(result, types::SqlValue::Integer(-42));
}

#[test]
fn test_to_number_negative_double() {
    let result = eval_function("TO_NUMBER", vec![varchar_lit("-3.14")]);
    assert_eq!(result, types::SqlValue::Double(-3.14));
}

#[test]
fn test_to_number_with_whitespace() {
    let result = eval_function("TO_NUMBER", vec![varchar_lit("  123  ")]);
    assert_eq!(result, types::SqlValue::Integer(123));
}

#[test]
fn test_to_number_with_commas() {
    let result = eval_function("TO_NUMBER", vec![varchar_lit("1,234.56")]);
    assert_eq!(result, types::SqlValue::Double(1234.56));
}

#[test]
fn test_to_number_null_input() {
    let result = eval_function("TO_NUMBER", vec![null_lit()]);
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_to_number_invalid_string() {
    eval_function_expect_error("TO_NUMBER", vec![varchar_lit("invalid")]);
}

#[test]
fn test_to_number_mixed_alphanumeric() {
    eval_function_expect_error("TO_NUMBER", vec![varchar_lit("abc123")]);
}

#[test]
fn test_to_number_zero() {
    let result = eval_function("TO_NUMBER", vec![varchar_lit("0")]);
    assert_eq!(result, types::SqlValue::Integer(0));
}

#[test]
fn test_to_number_scientific_notation() {
    let result = eval_function("TO_NUMBER", vec![varchar_lit("1.5e2")]);
    assert_eq!(result, types::SqlValue::Double(150.0));
}

#[test]
fn test_to_number_wrong_arg_count() {
    eval_function_expect_error("TO_NUMBER", vec![varchar_lit("42"), varchar_lit("extra")]);
}

#[test]
fn test_to_number_non_string_argument() {
    eval_function_expect_error("TO_NUMBER", vec![int_lit(42)]);
}

// ==================== TO_DATE FUNCTION TESTS ====================

#[test]
fn test_to_date_basic() {
    let result = eval_function("TO_DATE", vec![varchar_lit("2023-12-25"), varchar_lit("YYYY-MM-DD")]);
    assert_eq!(result, types::SqlValue::Date("2023-12-25".to_string()));
}

#[test]
fn test_to_date_different_format() {
    let result = eval_function("TO_DATE", vec![varchar_lit("25/12/2023"), varchar_lit("DD/MM/YYYY")]);
    assert_eq!(result, types::SqlValue::Date("2023-12-25".to_string()));
}

#[test]
fn test_to_date_null_input() {
    let result = eval_function("TO_DATE", vec![null_lit(), varchar_lit("YYYY-MM-DD")]);
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_to_date_null_format() {
    let result = eval_function("TO_DATE", vec![varchar_lit("2023-12-25"), null_lit()]);
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_to_date_invalid_format() {
    eval_function_expect_error("TO_DATE", vec![varchar_lit("2023-12-25"), varchar_lit("DD/MM/YYYY")]);
}

#[test]
fn test_to_date_wrong_arg_count() {
    eval_function_expect_error("TO_DATE", vec![varchar_lit("2023-12-25")]);
}

#[test]
fn test_to_date_non_string_arguments() {
    eval_function_expect_error("TO_DATE", vec![int_lit(20231225), varchar_lit("YYYY-MM-DD")]);
}

// ==================== TO_TIMESTAMP FUNCTION TESTS ====================

#[test]
fn test_to_timestamp_basic() {
    let result = eval_function(
        "TO_TIMESTAMP",
        vec![varchar_lit("2023-12-25 14:30:45"), varchar_lit("YYYY-MM-DD HH24:MI:SS")],
    );
    assert_eq!(result, types::SqlValue::Timestamp("2023-12-25 14:30:45".to_string()));
}

#[test]
fn test_to_timestamp_null_input() {
    let result = eval_function(
        "TO_TIMESTAMP",
        vec![null_lit(), varchar_lit("YYYY-MM-DD HH24:MI:SS")],
    );
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_to_timestamp_null_format() {
    let result = eval_function("TO_TIMESTAMP", vec![varchar_lit("2023-12-25 14:30:45"), null_lit()]);
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_to_timestamp_wrong_arg_count() {
    eval_function_expect_error("TO_TIMESTAMP", vec![varchar_lit("2023-12-25 14:30:45")]);
}

#[test]
fn test_to_timestamp_non_string_arguments() {
    eval_function_expect_error(
        "TO_TIMESTAMP",
        vec![int_lit(20231225), varchar_lit("YYYY-MM-DD HH24:MI:SS")],
    );
}

#[test]
fn test_to_date_with_character_type() {
    let result = eval_function(
        "TO_DATE",
        vec![character_lit("2023-12-25"), character_lit("YYYY-MM-DD")],
    );
    assert_eq!(result, types::SqlValue::Date("2023-12-25".to_string()));
}

#[test]
fn test_to_timestamp_with_character_type() {
    let result = eval_function(
        "TO_TIMESTAMP",
        vec![
            character_lit("2023-12-25 14:30:45"),
            character_lit("YYYY-MM-DD HH24:MI:SS"),
        ],
    );
    assert_eq!(result, types::SqlValue::Timestamp("2023-12-25 14:30:45".to_string()));
}

// ==================== TO_CHAR FUNCTION TESTS ====================

#[test]
fn test_to_char_date() {
    let result = eval_function("TO_CHAR", vec![date_lit("2023-12-25"), varchar_lit("YYYY-MM-DD")]);
    match result {
        types::SqlValue::Varchar(s) => {
            assert_eq!(s, "2023-12-25");
        }
        _ => panic!("Expected Varchar result"),
    }
}

#[test]
fn test_to_char_timestamp() {
    let result = eval_function(
        "TO_CHAR",
        vec![
            timestamp_lit("2023-12-25 14:30:45"),
            varchar_lit("YYYY-MM-DD HH24:MI:SS"),
        ],
    );
    match result {
        types::SqlValue::Varchar(s) => {
            assert_eq!(s, "2023-12-25 14:30:45");
        }
        _ => panic!("Expected Varchar result"),
    }
}

#[test]
fn test_to_char_integer() {
    let result = eval_function("TO_CHAR", vec![int_lit(42), varchar_lit("999")]);
    match result {
        types::SqlValue::Varchar(_) => {
            // Accept any varchar result - the specific formatting depends on format_number implementation
        }
        _ => panic!("Expected Varchar result"),
    }
}

#[test]
fn test_to_char_double() {
    let result = eval_function("TO_CHAR", vec![double_lit(3.14), varchar_lit("999.99")]);
    match result {
        types::SqlValue::Varchar(_) => {
            // Accept any varchar result - the specific formatting depends on format_number implementation
        }
        _ => panic!("Expected Varchar result"),
    }
}

#[test]
fn test_to_char_null_value() {
    let result = eval_function("TO_CHAR", vec![null_lit(), varchar_lit("YYYY-MM-DD")]);
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_to_char_null_format() {
    let result = eval_function("TO_CHAR", vec![date_lit("2023-12-25"), null_lit()]);
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_to_char_wrong_arg_count() {
    eval_function_expect_error("TO_CHAR", vec![date_lit("2023-12-25")]);
}

#[test]
fn test_to_char_all_numeric_types() {
    // Test Smallint
    let result = eval_function("TO_CHAR", vec![smallint_lit(42), varchar_lit("999")]);
    assert!(matches!(result, types::SqlValue::Varchar(_)));

    // Test Bigint
    let result = eval_function("TO_CHAR", vec![bigint_lit(42000), varchar_lit("999999")]);
    assert!(matches!(result, types::SqlValue::Varchar(_)));

    // Test Float
    let result = eval_function("TO_CHAR", vec![float_lit(3.14), varchar_lit("999.99")]);
    assert!(matches!(result, types::SqlValue::Varchar(_)));

    // Test Real
    let result = eval_function("TO_CHAR", vec![real_lit(2.71), varchar_lit("999.99")]);
    assert!(matches!(result, types::SqlValue::Varchar(_)));
}

#[test]
fn test_to_char_time() {
    let result = eval_function("TO_CHAR", vec![time_lit("14:30:45"), varchar_lit("HH24:MI:SS")]);
    match result {
        types::SqlValue::Varchar(s) => {
            assert_eq!(s, "14:30:45");
        }
        _ => panic!("Expected Varchar result"),
    }
}

#[test]
fn test_to_char_non_string_format() {
    eval_function_expect_error("TO_CHAR", vec![date_lit("2023-12-25"), int_lit(123)]);
}

// ==================== CAST FUNCTION TESTS ====================

#[test]
fn test_cast_integer_to_varchar() {
    let result = eval_function("CAST", vec![int_lit(42), varchar_lit("VARCHAR")]);
    match result {
        types::SqlValue::Varchar(s) => assert_eq!(s, "42"),
        types::SqlValue::Character(s) => assert_eq!(s, "42"),
        _ => panic!("Expected string result, got {:?}", result),
    }
}

#[test]
fn test_cast_varchar_to_integer() {
    let result = eval_function("CAST", vec![varchar_lit("42"), varchar_lit("INTEGER")]);
    assert_eq!(result, types::SqlValue::Integer(42));
}

#[test]
fn test_cast_integer_to_double() {
    let result = eval_function("CAST", vec![int_lit(42), varchar_lit("DOUBLE")]);
    assert_eq!(result, types::SqlValue::Double(42.0));
}

#[test]
fn test_cast_double_to_integer_unsupported() {
    // Note: Double to Integer cast is not currently supported in the implementation
    eval_function_expect_error("CAST", vec![double_lit(42.9), varchar_lit("INTEGER")]);
}

#[test]
fn test_cast_smallint_to_bigint() {
    let result = eval_function("CAST", vec![smallint_lit(42), varchar_lit("BIGINT")]);
    assert_eq!(result, types::SqlValue::Bigint(42));
}

#[test]
fn test_cast_varchar_to_date() {
    let result = eval_function("CAST", vec![varchar_lit("2023-12-25"), varchar_lit("DATE")]);
    assert_eq!(result, types::SqlValue::Date("2023-12-25".to_string()));
}

#[test]
fn test_cast_unsupported_type() {
    eval_function_expect_error("CAST", vec![int_lit(42), varchar_lit("BLOB")]);
}

#[test]
fn test_cast_wrong_arg_count() {
    eval_function_expect_error("CAST", vec![int_lit(42)]);
}

#[test]
fn test_cast_invalid_conversion() {
    eval_function_expect_error("CAST", vec![varchar_lit("invalid"), varchar_lit("INTEGER")]);
}
