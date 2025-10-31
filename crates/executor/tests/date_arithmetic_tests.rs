//! Tests for date arithmetic functions (DATEDIFF, DATE_ADD, DATE_SUB, EXTRACT, AGE)

mod common;

use common::create_test_evaluator;

// ==================== DATEDIFF TESTS ====================

#[test]
fn test_datediff_basic() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATEDIFF".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-10".to_string())),
            ast::Expression::Literal(types::SqlValue::Date("2024-01-05".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(5)); // 10 - 5 = 5 days
}

#[test]
fn test_datediff_negative() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATEDIFF".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-05".to_string())),
            ast::Expression::Literal(types::SqlValue::Date("2024-01-10".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(-5)); // 5 - 10 = -5 days
}

#[test]
fn test_datediff_same_date() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATEDIFF".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".to_string())),
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(0));
}

#[test]
fn test_datediff_with_timestamps() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATEDIFF".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Timestamp("2024-01-10 15:30:00".to_string())),
            ast::Expression::Literal(types::SqlValue::Timestamp("2024-01-05 08:00:00".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(5)); // Time component ignored
}

#[test]
fn test_datediff_with_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATEDIFF".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Date("2024-01-05".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

// ==================== DATE_ADD TESTS ====================

#[test]
fn test_date_add_days() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(7)),
            ast::Expression::Literal(types::SqlValue::Varchar("DAY".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2024-01-22".to_string()));
}

#[test]
fn test_date_add_months() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
            ast::Expression::Literal(types::SqlValue::Varchar("MONTH".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2024-04-15".to_string()));
}

#[test]
fn test_date_add_years() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(2)),
            ast::Expression::Literal(types::SqlValue::Varchar("YEAR".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2026-01-15".to_string()));
}

#[test]
fn test_date_add_negative() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(-5)),
            ast::Expression::Literal(types::SqlValue::Varchar("DAY".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2024-01-10".to_string()));
}

#[test]
fn test_date_add_timestamp_with_time() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Timestamp("2024-01-15 14:30:00".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(2)),
            ast::Expression::Literal(types::SqlValue::Varchar("HOUR".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Timestamp("2024-01-15 16:30:00".to_string()));
}

#[test]
fn test_adddate_alias() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "ADDDATE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(10)),
            ast::Expression::Literal(types::SqlValue::Varchar("DAY".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2024-01-25".to_string()));
}

// ==================== DATE_SUB TESTS ====================

#[test]
fn test_date_sub_days() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATE_SUB".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(7)),
            ast::Expression::Literal(types::SqlValue::Varchar("DAY".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2024-01-08".to_string()));
}

#[test]
fn test_date_sub_months() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATE_SUB".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-04-15".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(2)),
            ast::Expression::Literal(types::SqlValue::Varchar("MONTH".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2024-02-15".to_string()));
}

#[test]
fn test_subdate_alias() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "SUBDATE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(5)),
            ast::Expression::Literal(types::SqlValue::Varchar("DAY".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2024-01-10".to_string()));
}

// ==================== EXTRACT TESTS ====================

#[test]
fn test_extract_year() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "EXTRACT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("YEAR".to_string())),
            ast::Expression::Literal(types::SqlValue::Date("2024-03-15".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(2024));
}

#[test]
fn test_extract_month() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "EXTRACT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("MONTH".to_string())),
            ast::Expression::Literal(types::SqlValue::Date("2024-03-15".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(3));
}

#[test]
fn test_extract_day() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "EXTRACT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("DAY".to_string())),
            ast::Expression::Literal(types::SqlValue::Date("2024-03-15".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(15));
}

#[test]
fn test_extract_hour_from_timestamp() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "EXTRACT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("HOUR".to_string())),
            ast::Expression::Literal(types::SqlValue::Timestamp("2024-03-15 14:30:45".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(14));
}

// ==================== AGE TESTS ====================

#[test]
fn test_age_two_dates_years_only() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "AGE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".to_string())),
            ast::Expression::Literal(types::SqlValue::Date("2020-01-15".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    if let types::SqlValue::Varchar(age_str) = result {
        assert!(age_str.contains("4 years"));
    } else {
        panic!("Expected VARCHAR result from AGE");
    }
}

#[test]
fn test_age_two_dates_complex() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "AGE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-05-20".to_string())),
            ast::Expression::Literal(types::SqlValue::Date("2022-02-15".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    if let types::SqlValue::Varchar(age_str) = result {
        // Should be approximately 2 years 3 months 5 days
        assert!(age_str.contains("2 years"));
        assert!(age_str.contains("3 months"));
    } else {
        panic!("Expected VARCHAR result from AGE");
    }
}

#[test]
fn test_age_negative() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "AGE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2020-01-15".to_string())),
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    if let types::SqlValue::Varchar(age_str) = result {
        // Should be negative
        assert!(age_str.starts_with('-'));
    } else {
        panic!("Expected VARCHAR result from AGE");
    }
}

#[test]
fn test_age_same_date() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "AGE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".to_string())),
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    if let types::SqlValue::Varchar(age_str) = result {
        assert_eq!(age_str, "0 days");
    } else {
        panic!("Expected VARCHAR result from AGE");
    }
}

// ==================== EDGE CASES AND NULL HANDLING ====================

#[test]
fn test_date_add_with_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Integer(7)),
            ast::Expression::Literal(types::SqlValue::Varchar("DAY".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_extract_with_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "EXTRACT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("YEAR".to_string())),
            ast::Expression::Literal(types::SqlValue::Null),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_age_with_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "AGE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

// ==================== LEAP YEAR EDGE CASES ====================

#[test]
fn test_date_add_leap_year_to_non_leap() {
    let (evaluator, row) = create_test_evaluator();

    // Feb 29, 2024 (leap year) + 1 year → Feb 28, 2025 (non-leap year)
    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-02-29".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Varchar("YEAR".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2025-02-28".to_string()));
}

#[test]
fn test_date_add_leap_year_to_leap() {
    let (evaluator, row) = create_test_evaluator();

    // Feb 29, 2024 (leap year) + 4 years → Feb 29, 2028 (leap year)
    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-02-29".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(4)),
            ast::Expression::Literal(types::SqlValue::Varchar("YEAR".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2028-02-29".to_string()));
}

#[test]
fn test_date_sub_leap_year() {
    let (evaluator, row) = create_test_evaluator();

    // Feb 29, 2024 - 1 year → Feb 28, 2023 (non-leap year)
    let expr = ast::Expression::Function {
        name: "DATE_SUB".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-02-29".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Varchar("YEAR".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2023-02-28".to_string()));
}

#[test]
fn test_datediff_across_leap_year() {
    let (evaluator, row) = create_test_evaluator();

    // Difference between leap year and non-leap year Feb dates
    let expr = ast::Expression::Function {
        name: "DATEDIFF".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-03-01".to_string())),
            ast::Expression::Literal(types::SqlValue::Date("2024-02-01".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(29)); // 29 days in Feb 2024 (leap year)
}

#[test]
fn test_datediff_non_leap_year_february() {
    let (evaluator, row) = create_test_evaluator();

    // Difference in non-leap year February
    let expr = ast::Expression::Function {
        name: "DATEDIFF".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2023-03-01".to_string())),
            ast::Expression::Literal(types::SqlValue::Date("2023-02-01".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(28)); // 28 days in Feb 2023 (non-leap)
}

// ==================== MONTH BOUNDARY EDGE CASES ====================

#[test]
fn test_date_add_month_end_to_shorter_month() {
    let (evaluator, row) = create_test_evaluator();

    // Jan 31 + 1 month → Feb 28/29 (depending on year)
    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-31".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Varchar("MONTH".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2024-02-29".to_string())); // 2024 is leap year
}

#[test]
fn test_date_add_month_end_to_shorter_month_non_leap() {
    let (evaluator, row) = create_test_evaluator();

    // Jan 31, 2023 + 1 month → Feb 28, 2023
    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2023-01-31".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Varchar("MONTH".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2023-02-28".to_string()));
}

#[test]
fn test_date_sub_month_from_march_31() {
    let (evaluator, row) = create_test_evaluator();

    // Mar 31 - 1 month → Feb 29 (in leap year)
    let expr = ast::Expression::Function {
        name: "DATE_SUB".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-03-31".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Varchar("MONTH".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2024-02-29".to_string()));
}

#[test]
fn test_date_add_month_may_31_to_june() {
    let (evaluator, row) = create_test_evaluator();

    // May 31 + 1 month → Jun 30 (June has 30 days)
    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-05-31".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Varchar("MONTH".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2024-06-30".to_string()));
}

#[test]
fn test_date_add_multiple_months_across_year_boundary() {
    let (evaluator, row) = create_test_evaluator();

    // Oct 15 + 5 months → Mar 15 (crosses year boundary)
    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2023-10-15".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(5)),
            ast::Expression::Literal(types::SqlValue::Varchar("MONTH".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2024-03-15".to_string()));
}

#[test]
fn test_date_sub_months_across_year_boundary() {
    let (evaluator, row) = create_test_evaluator();

    // Feb 15, 2024 - 5 months → Sep 15, 2023
    let expr = ast::Expression::Function {
        name: "DATE_SUB".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-02-15".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(5)),
            ast::Expression::Literal(types::SqlValue::Varchar("MONTH".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2023-09-15".to_string()));
}

// ==================== YEAR BOUNDARY TESTS ====================

#[test]
fn test_date_add_days_across_year_boundary() {
    let (evaluator, row) = create_test_evaluator();

    // Dec 31, 2023 + 1 day → Jan 1, 2024
    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2023-12-31".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Varchar("DAY".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2024-01-01".to_string()));
}

#[test]
fn test_date_sub_days_across_year_boundary() {
    let (evaluator, row) = create_test_evaluator();

    // Jan 1, 2024 - 1 day → Dec 31, 2023
    let expr = ast::Expression::Function {
        name: "DATE_SUB".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-01".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Varchar("DAY".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2023-12-31".to_string()));
}

#[test]
fn test_datediff_across_year_boundary() {
    let (evaluator, row) = create_test_evaluator();

    // Difference across year boundary
    let expr = ast::Expression::Function {
        name: "DATEDIFF".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-05".to_string())),
            ast::Expression::Literal(types::SqlValue::Date("2023-12-28".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(8)); // 3 days in Dec + 5 days in Jan
}

// ==================== LARGE INTERVAL TESTS ====================

#[test]
fn test_date_add_large_year_interval() {
    let (evaluator, row) = create_test_evaluator();

    // Add 1000 years (should not panic or overflow)
    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(1000)),
            ast::Expression::Literal(types::SqlValue::Varchar("YEAR".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("3024-01-15".to_string()));
}

#[test]
fn test_date_add_large_day_interval() {
    let (evaluator, row) = create_test_evaluator();

    // Add 365 days (1 year worth)
    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-01".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(365)),
            ast::Expression::Literal(types::SqlValue::Varchar("DAY".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2024-12-31".to_string())); // 2024 is leap year with 366 days
}

#[test]
fn test_date_sub_large_interval() {
    let (evaluator, row) = create_test_evaluator();

    // Subtract 50 years
    let expr = ast::Expression::Function {
        name: "DATE_SUB".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-06-15".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(50)),
            ast::Expression::Literal(types::SqlValue::Varchar("YEAR".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("1974-06-15".to_string()));
}

#[test]
fn test_datediff_large_interval() {
    let (evaluator, row) = create_test_evaluator();

    // Difference over many years (should not panic)
    let expr = ast::Expression::Function {
        name: "DATEDIFF".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-01".to_string())),
            ast::Expression::Literal(types::SqlValue::Date("1900-01-01".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    // Should return a large positive number (approximately 45,290 days)
    if let types::SqlValue::Integer(days) = result {
        assert!(days > 45000 && days < 46000);
    } else {
        panic!("Expected Integer result");
    }
}

// ==================== COMPREHENSIVE NULL PROPAGATION TESTS ====================

#[test]
fn test_date_add_null_amount() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".to_string())),
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Varchar("DAY".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_date_add_null_unit() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(5)),
            ast::Expression::Literal(types::SqlValue::Null),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_date_sub_null_date() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATE_SUB".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Integer(5)),
            ast::Expression::Literal(types::SqlValue::Varchar("DAY".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_date_sub_null_amount() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATE_SUB".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".to_string())),
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Varchar("MONTH".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_datediff_null_second_arg() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATEDIFF".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-10".to_string())),
            ast::Expression::Literal(types::SqlValue::Null),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_extract_null_unit() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "EXTRACT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".to_string())),
        ],
        character_unit: None,
    };
    // EXTRACT requires a string unit, not NULL (unit is a keyword, not a value)
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_age_null_second_date() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "AGE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".to_string())),
            ast::Expression::Literal(types::SqlValue::Null),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

// ==================== ERROR HANDLING TESTS ====================

#[test]
fn test_datediff_invalid_date_format() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATEDIFF".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("invalid-date".to_string())),
            ast::Expression::Literal(types::SqlValue::Date("2024-01-05".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_datediff_wrong_argument_count() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATEDIFF".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Date("2024-01-10".to_string()))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_date_add_wrong_argument_count() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(5)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_date_add_invalid_unit() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(5)),
            ast::Expression::Literal(types::SqlValue::Varchar("INVALID_UNIT".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_date_add_wrong_type_for_amount() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("not-a-number".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("DAY".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_date_add_wrong_type_for_date() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(12345)),
            ast::Expression::Literal(types::SqlValue::Integer(5)),
            ast::Expression::Literal(types::SqlValue::Varchar("DAY".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_datediff_wrong_types() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATEDIFF".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(123)),
            ast::Expression::Literal(types::SqlValue::Varchar("not-a-date".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

// ==================== TIME COMPONENT TESTS ====================

#[test]
fn test_date_add_hours_across_midnight() {
    let (evaluator, row) = create_test_evaluator();

    // 11 PM + 2 hours → 1 AM next day
    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Timestamp("2024-01-15 23:00:00".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(2)),
            ast::Expression::Literal(types::SqlValue::Varchar("HOUR".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Timestamp("2024-01-16 01:00:00".to_string()));
}

#[test]
fn test_date_add_minutes_overflow() {
    let (evaluator, row) = create_test_evaluator();

    // 90 minutes from 11:30 → 1:00 PM
    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Timestamp("2024-01-15 11:30:00".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(90)),
            ast::Expression::Literal(types::SqlValue::Varchar("MINUTE".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Timestamp("2024-01-15 13:00:00".to_string()));
}

#[test]
fn test_date_add_seconds_overflow() {
    let (evaluator, row) = create_test_evaluator();

    // 3661 seconds (1 hour, 1 minute, 1 second)
    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Timestamp("2024-01-15 10:30:30".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(3661)),
            ast::Expression::Literal(types::SqlValue::Varchar("SECOND".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Timestamp("2024-01-15 11:31:31".to_string()));
}

#[test]
fn test_date_sub_hours_across_midnight() {
    let (evaluator, row) = create_test_evaluator();

    // 1 AM - 2 hours → 11 PM previous day
    let expr = ast::Expression::Function {
        name: "DATE_SUB".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Timestamp("2024-01-16 01:00:00".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(2)),
            ast::Expression::Literal(types::SqlValue::Varchar("HOUR".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Timestamp("2024-01-15 23:00:00".to_string()));
}
