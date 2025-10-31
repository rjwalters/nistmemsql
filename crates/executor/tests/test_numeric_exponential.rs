//! Exponential function edge cases (POWER, SQRT, EXP, LN, LOG10)

mod common;
mod test_numeric_helpers;

use common::create_test_evaluator;
use test_numeric_helpers::*;
use types::SqlValue;

#[test]
fn test_power_zero_to_zero() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_double(&evaluator, &row, "POWER", vec![SqlValue::Integer(0), SqlValue::Integer(0)], 1.0, 0.001);
}

#[test]
fn test_power_negative_base() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_double(&evaluator, &row, "POWER", vec![SqlValue::Integer(-2), SqlValue::Integer(3)], -8.0, 0.001);
}

#[test]
fn test_power_with_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = create_function_expr("POWER", vec![SqlValue::Integer(2), SqlValue::Null]);
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, SqlValue::Null);
}

#[test]
fn test_sqrt_negative() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_errors(&evaluator, &row, "SQRT", vec![SqlValue::Integer(-1)]);
}

#[test]
fn test_sqrt_null() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_null_on_null_input(&evaluator, &row, "SQRT", vec![SqlValue::Double(4.0)]);
}

#[test]
fn test_exp_overflow() {
    let (evaluator, row) = create_test_evaluator();
    let expr = create_function_expr("EXP", vec![SqlValue::Double(1000.0)]);
    let result = evaluator.eval(&expr, &row).unwrap();

    // Should return infinity
    match result {
        SqlValue::Double(val) => {
            assert!(val.is_infinite() && val.is_sign_positive());
        }
        _ => panic!("Expected Double value"),
    }
}

#[test]
fn test_exp_null() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_null_on_null_input(&evaluator, &row, "EXP", vec![SqlValue::Double(1.0)]);
}

#[test]
fn test_ln_zero() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_errors(&evaluator, &row, "LN", vec![SqlValue::Integer(0)]);
}

#[test]
fn test_ln_negative() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_errors(&evaluator, &row, "LN", vec![SqlValue::Integer(-1)]);
}

#[test]
fn test_ln_null() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_null_on_null_input(&evaluator, &row, "LN", vec![SqlValue::Double(1.0)]);
}

#[test]
fn test_log10_zero() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_errors(&evaluator, &row, "LOG10", vec![SqlValue::Integer(0)]);
}

#[test]
fn test_log10_null() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_null_on_null_input(&evaluator, &row, "LOG10", vec![SqlValue::Double(10.0)]);
}
