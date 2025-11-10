//! Rounding function edge cases (ROUND, FLOOR, CEIL)

use vibesql_types::SqlValue;

use super::{
    basic::{
        assert_function_returns_double, assert_function_returns_null_on_null_input,
        create_function_expr,
    },
    common::create_test_evaluator,
};

#[test]
fn test_round_null() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_null_on_null_input(
        &evaluator,
        &row,
        "ROUND",
        vec![SqlValue::Double(3.14159)],
    );
}

#[test]
fn test_round_with_null_precision() {
    let (evaluator, row) = create_test_evaluator();
    let expr = create_function_expr("ROUND", vec![SqlValue::Double(3.14159), SqlValue::Null]);
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, SqlValue::Null);
}

#[test]
fn test_round_negative_precision() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_double(
        &evaluator,
        &row,
        "ROUND",
        vec![SqlValue::Double(123.456), SqlValue::Integer(-1)],
        120.0,
        0.001,
    );
}

#[test]
fn test_floor_null() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_null_on_null_input(
        &evaluator,
        &row,
        "FLOOR",
        vec![SqlValue::Double(3.14159)],
    );
}

#[test]
fn test_floor_negative() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_double(
        &evaluator,
        &row,
        "FLOOR",
        vec![SqlValue::Double(-1.5)],
        -2.0,
        0.001,
    );
}

#[test]
fn test_ceil_null() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_null_on_null_input(
        &evaluator,
        &row,
        "CEIL",
        vec![SqlValue::Double(3.14159)],
    );
}

#[test]
fn test_ceil_negative() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_double(
        &evaluator,
        &row,
        "CEIL",
        vec![SqlValue::Double(-1.5)],
        -1.0,
        0.001,
    );
}
