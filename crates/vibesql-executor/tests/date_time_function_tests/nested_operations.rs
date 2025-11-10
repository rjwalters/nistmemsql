//! Tests for nested datetime function operations
//!
//! This module tests combinations and nesting of datetime functions:
//! - Extraction from current date/time functions
//! - Multiple extractions from same value
//! - Complex nested operations

use super::fixtures::*;

// ==================== EXTRACTION FROM CURRENT FUNCTIONS ====================

#[test]
fn test_extract_from_current_date() {
    let (evaluator, row) = setup_test();

    // YEAR(CURRENT_DATE) should return current year as integer
    let expr =
        create_datetime_function("YEAR", vec![create_datetime_function("CURRENT_DATE", vec![])]);
    let result = evaluator.eval(&expr, &row).unwrap();

    // Should return an integer year (e.g., 2024, 2025)
    match result {
        vibesql_types::SqlValue::Integer(year) => {
            assert!((2024..=2100).contains(&year), "Year should be reasonable: {}", year);
        }
        _ => panic!("YEAR should return Integer"),
    }
}

#[test]
fn test_extract_from_current_timestamp() {
    let (evaluator, row) = setup_test();

    // HOUR(NOW()) should return current hour as integer
    let expr = create_datetime_function("HOUR", vec![create_datetime_function("NOW", vec![])]);
    let result = evaluator.eval(&expr, &row).unwrap();

    // Should return an integer hour (0-23)
    match result {
        vibesql_types::SqlValue::Integer(hour) => {
            assert!((0..=23).contains(&hour), "Hour should be 0-23: {}", hour);
        }
        _ => panic!("HOUR should return Integer"),
    }
}

// ==================== MULTIPLE EXTRACTIONS ====================

#[test]
fn test_multiple_extractions() {
    let (evaluator, row) = setup_test();

    let timestamp = vibesql_types::SqlValue::Timestamp(TEST_TIMESTAMP_CHRISTMAS.parse().unwrap());

    // Test YEAR
    let year_expr = create_datetime_function("YEAR", vec![create_literal(timestamp.clone())]);
    let year_result = evaluator.eval(&year_expr, &row).unwrap();
    assert_eq!(year_result, vibesql_types::SqlValue::Integer(2024));

    // Test MONTH
    let month_expr = create_datetime_function("MONTH", vec![create_literal(timestamp.clone())]);
    let month_result = evaluator.eval(&month_expr, &row).unwrap();
    assert_eq!(month_result, vibesql_types::SqlValue::Integer(12));

    // Test DAY
    let day_expr = create_datetime_function("DAY", vec![create_literal(timestamp.clone())]);
    let day_result = evaluator.eval(&day_expr, &row).unwrap();
    assert_eq!(day_result, vibesql_types::SqlValue::Integer(25));

    // Test HOUR
    let hour_expr = create_datetime_function("HOUR", vec![create_literal(timestamp.clone())]);
    let hour_result = evaluator.eval(&hour_expr, &row).unwrap();
    assert_eq!(hour_result, vibesql_types::SqlValue::Integer(23));

    // Test MINUTE
    let minute_expr = create_datetime_function("MINUTE", vec![create_literal(timestamp.clone())]);
    let minute_result = evaluator.eval(&minute_expr, &row).unwrap();
    assert_eq!(minute_result, vibesql_types::SqlValue::Integer(59));

    // Test SECOND
    let second_expr = create_datetime_function("SECOND", vec![create_literal(timestamp)]);
    let second_result = evaluator.eval(&second_expr, &row).unwrap();
    assert_eq!(second_result, vibesql_types::SqlValue::Integer(58));
}
