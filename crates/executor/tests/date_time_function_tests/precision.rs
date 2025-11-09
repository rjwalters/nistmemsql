//! Tests for datetime precision arguments
//!
//! This module tests precision specification for time and timestamp functions:
//! - Valid precision values (0-9)
//! - Invalid precision values (negative, > 9)
//! - Fractional second formatting

use super::fixtures::*;

// ==================== CURRENT_TIME PRECISION ====================

#[test]
fn test_current_time_precision_0() {
    let (evaluator, row) = setup_test();

    let expr =
        create_datetime_function("CURRENT_TIME", vec![create_literal(types::SqlValue::Integer(0))]);
    let result = evaluator.eval(&expr, &row).unwrap();

    // Precision 0: HH:MM:SS (no fractional)
    match result {
        types::SqlValue::Time(s) => {
            validate_fractional_precision(&s.to_string(), 0);
            validate_time_format(&s.to_string());
        }
        _ => panic!("CURRENT_TIME should return Time type"),
    }
}

#[test]
fn test_current_time_precision_3() {
    let (evaluator, row) = setup_test();

    let expr =
        create_datetime_function("CURRENT_TIME", vec![create_literal(types::SqlValue::Integer(3))]);
    let result = evaluator.eval(&expr, &row).unwrap();

    // Precision 3: HH:MM:SS.fff
    match result {
        types::SqlValue::Time(s) => {
            validate_fractional_precision(&s.to_string(), 3);
        }
        _ => panic!("CURRENT_TIME should return Time type"),
    }
}

#[test]
fn test_current_time_precision_6() {
    let (evaluator, row) = setup_test();

    let expr =
        create_datetime_function("CURRENT_TIME", vec![create_literal(types::SqlValue::Integer(6))]);
    let result = evaluator.eval(&expr, &row).unwrap();

    // Precision 6: HH:MM:SS.ffffff
    match result {
        types::SqlValue::Time(s) => {
            validate_fractional_precision(&s.to_string(), 6);
        }
        _ => panic!("CURRENT_TIME should return Time type"),
    }
}

#[test]
fn test_current_time_precision_9() {
    let (evaluator, row) = setup_test();

    let expr =
        create_datetime_function("CURRENT_TIME", vec![create_literal(types::SqlValue::Integer(9))]);
    let result = evaluator.eval(&expr, &row).unwrap();

    // Precision 9: HH:MM:SS.nnnnnnnnn (max precision)
    match result {
        types::SqlValue::Time(s) => {
            validate_fractional_precision(&s.to_string(), 9);
        }
        _ => panic!("CURRENT_TIME should return Time type"),
    }
}

#[test]
fn test_current_time_precision_invalid_high() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "CURRENT_TIME",
        vec![create_literal(types::SqlValue::Integer(10))],
    );
    let result = evaluator.eval(&expr, &row);

    // Precision > 9 should error
    assert!(result.is_err(), "Precision 10 should return an error");
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(err_msg.to_lowercase().contains("precision"), "Error should mention precision");
}

#[test]
fn test_current_time_precision_invalid_negative() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "CURRENT_TIME",
        vec![create_literal(types::SqlValue::Integer(-1))],
    );
    let result = evaluator.eval(&expr, &row);

    // Negative precision should error
    assert!(result.is_err(), "Negative precision should return an error");
}

// ==================== CURRENT_TIMESTAMP PRECISION ====================

#[test]
fn test_current_timestamp_precision_0() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "CURRENT_TIMESTAMP",
        vec![create_literal(types::SqlValue::Integer(0))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    // Precision 0: YYYY-MM-DD HH:MM:SS (no fractional)
    match result {
        types::SqlValue::Timestamp(s) => {
            validate_fractional_precision(&s.to_string(), 0);
            validate_timestamp_format(&s.to_string());
        }
        _ => panic!("CURRENT_TIMESTAMP should return Timestamp type"),
    }
}

#[test]
fn test_current_timestamp_precision_3() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "CURRENT_TIMESTAMP",
        vec![create_literal(types::SqlValue::Integer(3))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    // Precision 3: YYYY-MM-DD HH:MM:SS.fff
    match result {
        types::SqlValue::Timestamp(s) => {
            validate_fractional_precision(&s.to_string(), 3);
        }
        _ => panic!("CURRENT_TIMESTAMP should return Timestamp type"),
    }
}

#[test]
fn test_current_timestamp_precision_6() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "CURRENT_TIMESTAMP",
        vec![create_literal(types::SqlValue::Integer(6))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    // Precision 6: YYYY-MM-DD HH:MM:SS.ffffff
    match result {
        types::SqlValue::Timestamp(s) => {
            validate_fractional_precision(&s.to_string(), 6);
        }
        _ => panic!("CURRENT_TIMESTAMP should return Timestamp type"),
    }
}

#[test]
fn test_current_timestamp_precision_invalid() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "CURRENT_TIMESTAMP",
        vec![create_literal(types::SqlValue::Integer(10))],
    );
    let result = evaluator.eval(&expr, &row);

    // Precision > 9 should error
    assert!(result.is_err(), "Precision 10 should return an error");
}

// ==================== ARGUMENT VALIDATION ====================

#[test]
fn test_current_time_too_many_args() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "CURRENT_TIME",
        vec![
            create_literal(types::SqlValue::Integer(3)),
            create_literal(types::SqlValue::Integer(5)),
        ],
    );
    let result = evaluator.eval(&expr, &row);

    // Should error with too many arguments
    assert!(result.is_err(), "Should error with too many arguments");
}
