//! Domain and range error tests

use vibesql_types::SqlValue;

use super::{basic::assert_function_errors, common::create_test_evaluator};

#[test]
fn test_trig_domain_errors() {
    let (evaluator, row) = create_test_evaluator();

    // Test various domain errors
    let test_cases = vec![
        ("ASIN", vec![SqlValue::Double(1.1)]),
        ("ASIN", vec![SqlValue::Double(-1.1)]),
        ("ACOS", vec![SqlValue::Double(1.1)]),
        ("ACOS", vec![SqlValue::Double(-1.1)]),
    ];

    for (func_name, args) in test_cases {
        assert_function_errors(&evaluator, &row, func_name, args);
    }
}
