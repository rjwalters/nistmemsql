//! Precision and accuracy tests

use vibesql_types::SqlValue;

use super::{
    basic::{assert_function_returns_double, create_function_expr},
    common::create_test_evaluator,
};

#[test]
fn test_round_precision_edge_cases() {
    let (evaluator, row) = create_test_evaluator();

    // Test rounding at precision boundaries
    let test_cases = vec![
        (1.23456789, 2, 1.23),
        (1.23456789, 0, 1.0),
        (1.99999999, 2, 2.00),
        (-1.23456789, 2, -1.23),
    ];

    for (input, precision, expected) in test_cases {
        assert_function_returns_double(
            &evaluator,
            &row,
            "ROUND",
            vec![SqlValue::Double(input), SqlValue::Integer(precision)],
            expected,
            0.001,
        );
    }
}

#[test]
fn test_floating_point_precision() {
    let (evaluator, row) = create_test_evaluator();

    // Test operations that might lose precision
    let expr = create_function_expr("SQRT", vec![SqlValue::Double(2.0)]);
    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        SqlValue::Double(val) => {
            let squared = val * val;
            // sqrt(2)^2 should be very close to 2
            assert!((squared - 2.0).abs() < 0.0000001);
        }
        _ => panic!("Expected Double value"),
    }
}
