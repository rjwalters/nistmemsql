//! Type coercion and mixed type tests

mod common;
mod test_numeric_helpers;

use common::create_test_evaluator;
use test_numeric_helpers::*;
use types::SqlValue;

#[test]
fn test_abs_type_coercion() {
    let (evaluator, row) = create_test_evaluator();

    // Test ABS with different numeric types
    test_function_across_types(&evaluator, &row, "ABS", |input| input);
}

#[test]
fn test_power_type_mixing() {
    let (evaluator, row) = create_test_evaluator();

    // Test POWER with mixed types
    assert_function_returns_double(&evaluator, &row, "POWER", vec![SqlValue::Integer(2), SqlValue::Double(3.0)], 8.0, 0.001);
}
