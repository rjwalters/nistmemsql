//! Basic string function coverage extracted from `string_edge_cases`.
//!
//! These tests validate fundamental SQL string functions such as `UPPER`, `LOWER`,
//! `LENGTH`, `CHAR_LENGTH`, `OCTET_LENGTH`, and `TRIM` in isolation so that the
//! remaining edge-case suite can focus on more advanced scenarios.

use crate::common::create_test_evaluator;

type EvalResult = Result<types::SqlValue, executor::ExecutorError>;

fn lit(value: types::SqlValue) -> ast::Expression {
    ast::Expression::Literal(value)
}

fn varchar(value: &str) -> types::SqlValue {
    types::SqlValue::Varchar(value.to_string())
}

fn character(value: &str) -> types::SqlValue {
    types::SqlValue::Character(value.to_string())
}

fn int(value: i64) -> types::SqlValue {
    types::SqlValue::Integer(value)
}

fn run_function(
    name: &str,
    args: Vec<ast::Expression>,
    character_unit: Option<ast::CharacterUnit>,
) -> EvalResult {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Function { name: name.to_string(), args, character_unit };
    evaluator.eval(&expr, &row)
}

fn assert_function_value(name: &str, args: Vec<ast::Expression>, expected: types::SqlValue) {
    assert_eq!(run_function(name, args, None).unwrap(), expected);
}

fn assert_function_value_with_unit(
    name: &str,
    args: Vec<ast::Expression>,
    unit: ast::CharacterUnit,
    expected: types::SqlValue,
) {
    assert_eq!(run_function(name, args, Some(unit)).unwrap(), expected);
}

fn assert_function_error(name: &str, args: Vec<ast::Expression>) {
    assert!(run_function(name, args, None).is_err());
}

fn run_trim(
    position: Option<ast::TrimPosition>,
    removal_char: Option<types::SqlValue>,
    string: types::SqlValue,
) -> EvalResult {
    let (evaluator, row) = create_test_evaluator();
    let expr = ast::Expression::Trim {
        position,
        removal_char: removal_char.map(|value| Box::new(lit(value))),
        string: Box::new(lit(string)),
    };
    evaluator.eval(&expr, &row)
}

fn assert_trim_value(
    position: Option<ast::TrimPosition>,
    removal_char: Option<types::SqlValue>,
    string: types::SqlValue,
    expected: types::SqlValue,
) {
    assert_eq!(run_trim(position, removal_char, string).unwrap(), expected);
}

// ============================================================================
// UPPER/LOWER Function Tests
// ============================================================================

#[test]
fn test_upper_null() {
    assert_function_value("UPPER", vec![lit(types::SqlValue::Null)], types::SqlValue::Null);
}

#[test]
fn test_upper_empty() {
    assert_function_value("UPPER", vec![lit(varchar(""))], varchar(""));
}

#[test]
fn test_upper_unicode() {
    assert_function_value("UPPER", vec![lit(varchar("café"))], varchar("CAFÉ"));
}

#[test]
fn test_upper_mixed_case() {
    assert_function_value("UPPER", vec![lit(varchar("HeLLo WoRLd"))], varchar("HELLO WORLD"));
}

#[test]
fn test_upper_character_type() {
    assert_function_value("UPPER", vec![lit(character("test"))], varchar("TEST"));
}

#[test]
fn test_upper_wrong_arg_count() {
    assert_function_error(
        "UPPER",
        vec![lit(varchar("test")), lit(varchar("extra"))],
    );
}

#[test]
fn test_upper_wrong_type() {
    assert_function_error("UPPER", vec![lit(int(123))]);
}

#[test]
fn test_lower_null() {
    assert_function_value("LOWER", vec![lit(types::SqlValue::Null)], types::SqlValue::Null);
}

#[test]
fn test_lower_empty() {
    assert_function_value("LOWER", vec![lit(varchar(""))], varchar(""));
}

#[test]
fn test_lower_unicode() {
    assert_function_value("LOWER", vec![lit(varchar("CAFÉ"))], varchar("café"));
}

#[test]
fn test_lower_wrong_arg_count() {
    assert_function_error(
        "LOWER",
        vec![lit(varchar("TEST")), lit(varchar("extra"))],
    );
}

#[test]
fn test_lower_wrong_type() {
    assert_function_error("LOWER", vec![lit(int(123))]);
}

#[test]
fn test_lower_character_type() {
    assert_function_value("LOWER", vec![lit(character("TEST"))], varchar("test"));
}

// ============================================================================
// LENGTH / CHAR_LENGTH / OCTET_LENGTH Tests
// ============================================================================

#[test]
fn test_length_null() {
    assert_function_value("LENGTH", vec![lit(types::SqlValue::Null)], types::SqlValue::Null);
}

#[test]
fn test_length_empty() {
    assert_function_value("LENGTH", vec![lit(varchar(""))], int(0));
}

#[test]
fn test_length_basic() {
    assert_function_value("LENGTH", vec![lit(varchar("hello"))], int(5));
}

#[test]
fn test_length_multibyte() {
    assert_function_value("LENGTH", vec![lit(varchar("café"))], int(5));
}

#[test]
fn test_length_wrong_arg_count() {
    assert_function_error(
        "LENGTH",
        vec![lit(varchar("hello")), lit(varchar("extra"))],
    );
}

#[test]
fn test_length_wrong_type() {
    assert_function_error("LENGTH", vec![lit(int(123))]);
}

#[test]
fn test_length_character_type() {
    assert_function_value("LENGTH", vec![lit(character("test"))], int(4));
}

#[test]
fn test_char_length_null() {
    assert_function_value("CHAR_LENGTH", vec![lit(types::SqlValue::Null)], types::SqlValue::Null);
}

#[test]
fn test_char_length_empty() {
    assert_function_value("CHAR_LENGTH", vec![lit(varchar(""))], int(0));
}

#[test]
fn test_char_length_multibyte() {
    assert_function_value("CHAR_LENGTH", vec![lit(varchar("café"))], int(4));
}

#[test]
fn test_char_length_using_characters() {
    assert_function_value_with_unit(
        "CHAR_LENGTH",
        vec![lit(varchar("café"))],
        ast::CharacterUnit::Characters,
        int(4),
    );
}

#[test]
fn test_char_length_using_octets() {
    assert_function_value_with_unit(
        "CHAR_LENGTH",
        vec![lit(varchar("café"))],
        ast::CharacterUnit::Octets,
        int(5),
    );
}

#[test]
fn test_char_length_wrong_arg_count() {
    assert_function_error(
        "CHAR_LENGTH",
        vec![lit(varchar("hello")), lit(varchar("extra"))],
    );
}

#[test]
fn test_char_length_wrong_type() {
    assert_function_error("CHAR_LENGTH", vec![lit(int(123))]);
}

#[test]
fn test_char_length_character_type() {
    assert_function_value("CHAR_LENGTH", vec![lit(character("test"))], int(4));
}

#[test]
fn test_octet_length_null() {
    assert_function_value("OCTET_LENGTH", vec![lit(types::SqlValue::Null)], types::SqlValue::Null);
}

#[test]
fn test_octet_length_empty() {
    assert_function_value("OCTET_LENGTH", vec![lit(varchar(""))], int(0));
}

#[test]
fn test_octet_length_multibyte() {
    assert_function_value("OCTET_LENGTH", vec![lit(varchar("café"))], int(5));
}

#[test]
fn test_octet_length_vs_char_length() {
    let char_result = run_function("CHAR_LENGTH", vec![lit(varchar("hello"))], None).unwrap();
    let octet_result = run_function("OCTET_LENGTH", vec![lit(varchar("hello"))], None).unwrap();
    assert_eq!(char_result, int(5));
    assert_eq!(octet_result, int(5));
}

#[test]
fn test_octet_length_wrong_arg_count() {
    assert_function_error(
        "OCTET_LENGTH",
        vec![lit(varchar("hello")), lit(varchar("extra"))],
    );
}

#[test]
fn test_octet_length_wrong_type() {
    assert_function_error("OCTET_LENGTH", vec![lit(int(123))]);
}

#[test]
fn test_octet_length_character_type() {
    assert_function_value("OCTET_LENGTH", vec![lit(character("café"))], int(5));
}

// ============================================================================
// TRIM Function Tests
// ============================================================================

#[test]
fn test_trim_null_removal_char() {
    assert_trim_value(
        Some(ast::TrimPosition::Both),
        Some(types::SqlValue::Null),
        varchar("hello"),
        types::SqlValue::Null,
    );
}

#[test]
fn test_trim_null_string() {
    assert_trim_value(None, None, types::SqlValue::Null, types::SqlValue::Null);
}

#[test]
fn test_trim_custom_char_multibyte() {
    assert_trim_value(
        Some(ast::TrimPosition::Both),
        Some(varchar("x")),
        varchar("xxxhelloxxx"),
        varchar("hello"),
    );
}
