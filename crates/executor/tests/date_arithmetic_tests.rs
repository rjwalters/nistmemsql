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
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}
