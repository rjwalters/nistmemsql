//! Trigonometric function edge cases (SIN, ASIN, ACOS, ATAN2, RADIANS, DEGREES)

use vibesql_types::SqlValue;

use super::{
    basic::{
        assert_function_errors, assert_function_returns_null_on_null_input, create_function_expr,
    },
    common::create_test_evaluator,
};

#[test]
fn test_sin_null() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_null_on_null_input(
        &evaluator,
        &row,
        "SIN",
        vec![SqlValue::Double(0.0)],
    );
}

#[test]
fn test_asin_out_of_range() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_errors(&evaluator, &row, "ASIN", vec![SqlValue::Double(2.0)]);
}

#[test]
fn test_acos_out_of_range() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_errors(&evaluator, &row, "ACOS", vec![SqlValue::Double(-2.0)]);
}

#[test]
fn test_atan2_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = create_function_expr("ATAN2", vec![SqlValue::Double(1.0), SqlValue::Null]);
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, SqlValue::Null);
}

#[test]
fn test_radians_null() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_null_on_null_input(
        &evaluator,
        &row,
        "RADIANS",
        vec![SqlValue::Double(180.0)],
    );
}

#[test]
fn test_degrees_null() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_null_on_null_input(
        &evaluator,
        &row,
        "DEGREES",
        vec![SqlValue::Double(std::f64::consts::PI)],
    );
}
