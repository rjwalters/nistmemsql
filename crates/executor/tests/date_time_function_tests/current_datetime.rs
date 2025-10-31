//! Tests for current date/time functions (CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP)
//!
//! This module tests the SQL standard CURRENT_* functions and their aliases:
//! - CURRENT_DATE / CURDATE
//! - CURRENT_TIME / CURTIME
//! - CURRENT_TIMESTAMP / NOW

use super::fixtures::*;

// ==================== CURRENT_DATE ====================

#[test]
fn test_current_date_format() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function("CURRENT_DATE", vec![]);
    let result = evaluator.eval(&expr, &row).unwrap();

    // Verify it returns a Date type with YYYY-MM-DD format
    match result {
        types::SqlValue::Date(s) => {
            validate_date_format(&s);
        }
        _ => panic!("CURRENT_DATE should return Date type"),
    }
}

#[test]
fn test_curdate_alias() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function("CURDATE", vec![]);
    let result = evaluator.eval(&expr, &row).unwrap();

    // Verify CURDATE is an alias for CURRENT_DATE
    assert!(matches!(result, types::SqlValue::Date(_)));
}

// ==================== CURRENT_TIME ====================

#[test]
fn test_current_time_format() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function("CURRENT_TIME", vec![]);
    let result = evaluator.eval(&expr, &row).unwrap();

    // Verify it returns a Time type with HH:MM:SS format
    match result {
        types::SqlValue::Time(s) => {
            validate_time_format(&s);
        }
        _ => panic!("CURRENT_TIME should return Time type"),
    }
}

#[test]
fn test_curtime_alias() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function("CURTIME", vec![]);
    let result = evaluator.eval(&expr, &row).unwrap();

    // Verify CURTIME is an alias for CURRENT_TIME
    assert!(matches!(result, types::SqlValue::Time(_)));
}

// ==================== CURRENT_TIMESTAMP ====================

#[test]
fn test_current_timestamp_format() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function("CURRENT_TIMESTAMP", vec![]);
    let result = evaluator.eval(&expr, &row).unwrap();

    // Verify it returns a Timestamp type with YYYY-MM-DD HH:MM:SS format
    match result {
        types::SqlValue::Timestamp(s) => {
            validate_timestamp_format(&s);
        }
        _ => panic!("CURRENT_TIMESTAMP should return Timestamp type"),
    }
}

#[test]
fn test_now_alias() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function("NOW", vec![]);
    let result = evaluator.eval(&expr, &row).unwrap();

    // Verify NOW is an alias for CURRENT_TIMESTAMP
    assert!(matches!(result, types::SqlValue::Timestamp(_)));
}
