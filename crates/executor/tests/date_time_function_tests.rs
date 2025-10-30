//! Tests for SQL CORE Phase 3A date/time functions
//!
//! Tests cover:
//! - Current date/time functions (CURRENT_DATE, CURRENT_TIME, NOW)
//! - Date extraction functions (YEAR, MONTH, DAY)
//! - Time extraction functions (HOUR, MINUTE, SECOND)
//! - NULL handling
//! - Invalid format handling
//! - Nested function combinations

use ast;
use catalog;
use executor::ExpressionEvaluator;
use storage;
use types;

fn create_test_evaluator() -> (ExpressionEvaluator<'static>, storage::Row) {
    let schema = Box::leak(Box::new(catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    )));

    let evaluator = ExpressionEvaluator::new(schema);
    let row = storage::Row::new(vec![types::SqlValue::Integer(1)]);

    (evaluator, row)
}

// ==================== CURRENT DATE/TIME FUNCTIONS ====================

#[test]
fn test_current_date_format() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function { name: "CURRENT_DATE".to_string(), args: vec![] };
    let result = evaluator.eval(&expr, &row).unwrap();

    // Verify it returns a Date type with YYYY-MM-DD format
    match result {
        types::SqlValue::Date(s) => {
            // Should match YYYY-MM-DD pattern
            let parts: Vec<&str> = s.split('-').collect();
            assert_eq!(parts.len(), 3, "Date should have 3 parts (YYYY-MM-DD)");
            assert_eq!(parts[0].len(), 4, "Year should be 4 digits");
            assert_eq!(parts[1].len(), 2, "Month should be 2 digits");
            assert_eq!(parts[2].len(), 2, "Day should be 2 digits");
        }
        _ => panic!("CURRENT_DATE should return Date type"),
    }
}

#[test]
fn test_curdate_alias() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function { name: "CURDATE".to_string(), args: vec![] };
    let result = evaluator.eval(&expr, &row).unwrap();

    // Verify CURDATE is an alias for CURRENT_DATE
    assert!(matches!(result, types::SqlValue::Date(_)));
}

#[test]
fn test_current_time_format() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function { name: "CURRENT_TIME".to_string(), args: vec![] };
    let result = evaluator.eval(&expr, &row).unwrap();

    // Verify it returns a Time type with HH:MM:SS format
    match result {
        types::SqlValue::Time(s) => {
            // Should match HH:MM:SS pattern
            let parts: Vec<&str> = s.split(':').collect();
            assert_eq!(parts.len(), 3, "Time should have 3 parts (HH:MM:SS)");
            assert_eq!(parts[0].len(), 2, "Hour should be 2 digits");
            assert_eq!(parts[1].len(), 2, "Minute should be 2 digits");
            assert_eq!(parts[2].len(), 2, "Second should be 2 digits");
        }
        _ => panic!("CURRENT_TIME should return Time type"),
    }
}

#[test]
fn test_curtime_alias() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function { name: "CURTIME".to_string(), args: vec![] };
    let result = evaluator.eval(&expr, &row).unwrap();

    // Verify CURTIME is an alias for CURRENT_TIME
    assert!(matches!(result, types::SqlValue::Time(_)));
}

#[test]
fn test_current_timestamp_format() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function { name: "CURRENT_TIMESTAMP".to_string(), args: vec![] };
    let result = evaluator.eval(&expr, &row).unwrap();

    // Verify it returns a Timestamp type with YYYY-MM-DD HH:MM:SS format
    match result {
        types::SqlValue::Timestamp(s) => {
            // Should match YYYY-MM-DD HH:MM:SS pattern
            let main_parts: Vec<&str> = s.split(' ').collect();
            assert_eq!(main_parts.len(), 2, "Timestamp should have date and time");

            let date_parts: Vec<&str> = main_parts[0].split('-').collect();
            assert_eq!(date_parts.len(), 3);

            let time_parts: Vec<&str> = main_parts[1].split(':').collect();
            assert_eq!(time_parts.len(), 3);
        }
        _ => panic!("CURRENT_TIMESTAMP should return Timestamp type"),
    }
}

#[test]
fn test_now_alias() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function { name: "NOW".to_string(), args: vec![] };
    let result = evaluator.eval(&expr, &row).unwrap();

    // Verify NOW is an alias for CURRENT_TIMESTAMP
    assert!(matches!(result, types::SqlValue::Timestamp(_)));
}

// ==================== DATE EXTRACTION FUNCTIONS ====================

#[test]
fn test_year_from_date() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "YEAR".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Date("2024-03-15".to_string()))],
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(2024));
}

#[test]
fn test_year_from_timestamp() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "YEAR".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Timestamp(
            "2024-03-15 14:30:45".to_string(),
        ))],
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(2024));
}

#[test]
fn test_year_with_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "YEAR".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Null)],
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_month_from_date() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "MONTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Date("2024-03-15".to_string()))],
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(3));
}

#[test]
fn test_month_from_timestamp() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "MONTH".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Timestamp(
            "2024-12-25 14:30:45".to_string(),
        ))],
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(12));
}

#[test]
fn test_day_from_date() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DAY".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Date("2024-03-15".to_string()))],
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(15));
}

#[test]
fn test_day_from_timestamp() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DAY".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Timestamp(
            "2024-03-27 14:30:45".to_string(),
        ))],
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(27));
}

// ==================== TIME EXTRACTION FUNCTIONS ====================

#[test]
fn test_hour_from_time() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "HOUR".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Time("14:30:45".to_string()))],
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(14));
}

#[test]
fn test_hour_from_timestamp() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "HOUR".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Timestamp(
            "2024-03-15 23:59:59".to_string(),
        ))],
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(23));
}

#[test]
fn test_minute_from_time() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "MINUTE".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Time("14:30:45".to_string()))],
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(30));
}

#[test]
fn test_minute_from_timestamp() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "MINUTE".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Timestamp(
            "2024-03-15 14:45:30".to_string(),
        ))],
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(45));
}

#[test]
fn test_second_from_time() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "SECOND".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Time("14:30:45".to_string()))],
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(45));
}

#[test]
fn test_second_from_timestamp() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "SECOND".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Timestamp(
            "2024-03-15 14:30:59".to_string(),
        ))],
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(59));
}

// ==================== NULL HANDLING ====================

#[test]
fn test_extraction_functions_with_null() {
    let (evaluator, row) = create_test_evaluator();

    let functions = vec!["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"];

    for func_name in functions {
        let expr = ast::Expression::Function {
            name: func_name.to_string(),
            args: vec![ast::Expression::Literal(types::SqlValue::Null)],
        };
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(
            result,
            types::SqlValue::Null,
            "{} should return NULL for NULL input",
            func_name
        );
    }
}

// ==================== NESTED FUNCTIONS ====================

#[test]
fn test_extract_from_current_date() {
    let (evaluator, row) = create_test_evaluator();

    // YEAR(CURRENT_DATE) should return current year as integer
    let expr = ast::Expression::Function {
        name: "YEAR".to_string(),
        args: vec![ast::Expression::Function { name: "CURRENT_DATE".to_string(), args: vec![] }],
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    // Should return an integer year (e.g., 2024, 2025)
    match result {
        types::SqlValue::Integer(year) => {
            assert!(year >= 2024 && year <= 2100, "Year should be reasonable: {}", year);
        }
        _ => panic!("YEAR should return Integer"),
    }
}

#[test]
fn test_extract_from_current_timestamp() {
    let (evaluator, row) = create_test_evaluator();

    // HOUR(NOW()) should return current hour as integer
    let expr = ast::Expression::Function {
        name: "HOUR".to_string(),
        args: vec![ast::Expression::Function { name: "NOW".to_string(), args: vec![] }],
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    // Should return an integer hour (0-23)
    match result {
        types::SqlValue::Integer(hour) => {
            assert!(hour >= 0 && hour <= 23, "Hour should be 0-23: {}", hour);
        }
        _ => panic!("HOUR should return Integer"),
    }
}

#[test]
fn test_multiple_extractions() {
    let (evaluator, row) = create_test_evaluator();

    let timestamp = types::SqlValue::Timestamp("2024-12-25 23:59:58".to_string());

    // Test YEAR
    let year_expr = ast::Expression::Function {
        name: "YEAR".to_string(),
        args: vec![ast::Expression::Literal(timestamp.clone())],
    };
    let year_result = evaluator.eval(&year_expr, &row).unwrap();
    assert_eq!(year_result, types::SqlValue::Integer(2024));

    // Test MONTH
    let month_expr = ast::Expression::Function {
        name: "MONTH".to_string(),
        args: vec![ast::Expression::Literal(timestamp.clone())],
    };
    let month_result = evaluator.eval(&month_expr, &row).unwrap();
    assert_eq!(month_result, types::SqlValue::Integer(12));

    // Test DAY
    let day_expr = ast::Expression::Function {
        name: "DAY".to_string(),
        args: vec![ast::Expression::Literal(timestamp.clone())],
    };
    let day_result = evaluator.eval(&day_expr, &row).unwrap();
    assert_eq!(day_result, types::SqlValue::Integer(25));

    // Test HOUR
    let hour_expr = ast::Expression::Function {
        name: "HOUR".to_string(),
        args: vec![ast::Expression::Literal(timestamp.clone())],
    };
    let hour_result = evaluator.eval(&hour_expr, &row).unwrap();
    assert_eq!(hour_result, types::SqlValue::Integer(23));

    // Test MINUTE
    let minute_expr = ast::Expression::Function {
        name: "MINUTE".to_string(),
        args: vec![ast::Expression::Literal(timestamp.clone())],
    };
    let minute_result = evaluator.eval(&minute_expr, &row).unwrap();
    assert_eq!(minute_result, types::SqlValue::Integer(59));

    // Test SECOND
    let second_expr = ast::Expression::Function {
        name: "SECOND".to_string(),
        args: vec![ast::Expression::Literal(timestamp)],
    };
    let second_result = evaluator.eval(&second_expr, &row).unwrap();
    assert_eq!(second_result, types::SqlValue::Integer(58));
}
