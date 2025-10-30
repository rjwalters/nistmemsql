//! Tests for SQL literal parsing
//!
//! Covers:
//! - DATE literals: DATE '2024-01-01'
//! - TIME literals: TIME '14:30:00'
//! - TIMESTAMP literals: TIMESTAMP '2024-01-01 14:30:00'
//! - INTERVAL literals: INTERVAL '5' YEAR

use super::*;

// ========================================================================
// DATE Literal Tests
// ========================================================================

#[test]
fn test_parse_date_literal() {
    let result = Parser::parse_sql("SELECT DATE '2024-01-01';");
    assert!(result.is_ok(), "DATE literal should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, alias: _ } => {
                    match expr {
                        ast::Expression::Literal(types::SqlValue::Date(s)) => {
                            assert_eq!(s, "2024-01-01");
                        }
                        _ => panic!("Expected DATE literal, got {:?}", expr),
                    }
                }
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_date_literal_in_where() {
    let result = Parser::parse_sql("SELECT * FROM events WHERE event_date = DATE '2024-12-25';");
    assert!(result.is_ok(), "DATE literal in WHERE should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        ast::Statement::Select(select) => {
            assert!(select.where_clause.is_some());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_date_literal_various_formats() {
    let test_cases = vec![
        "SELECT DATE '2024-01-01';",
        "SELECT DATE '2024-12-31';",
        "SELECT DATE '2000-02-29';", // Leap year
        "SELECT DATE '1999-01-01';",
    ];

    for sql in test_cases {
        let result = Parser::parse_sql(sql);
        assert!(result.is_ok(), "Should parse '{}': {:?}", sql, result);
    }
}

// ========================================================================
// TIME Literal Tests
// ========================================================================

#[test]
fn test_parse_time_literal() {
    let result = Parser::parse_sql("SELECT TIME '14:30:00';");
    assert!(result.is_ok(), "TIME literal should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, alias: _ } => {
                    match expr {
                        ast::Expression::Literal(types::SqlValue::Time(s)) => {
                            assert_eq!(s, "14:30:00");
                        }
                        _ => panic!("Expected TIME literal, got {:?}", expr),
                    }
                }
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_time_literal_with_seconds() {
    let result = Parser::parse_sql("SELECT TIME '23:59:59';");
    assert!(result.is_ok(), "TIME with seconds should parse: {:?}", result);
}

#[test]
fn test_parse_time_literal_midnight() {
    let result = Parser::parse_sql("SELECT TIME '00:00:00';");
    assert!(result.is_ok(), "TIME midnight should parse: {:?}", result);
}

#[test]
fn test_parse_time_literal_with_fractional_seconds() {
    let result = Parser::parse_sql("SELECT TIME '14:30:00.123';");
    assert!(result.is_ok(), "TIME with fractional seconds should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        ast::Statement::Select(select) => {
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, alias: _ } => {
                    match expr {
                        ast::Expression::Literal(types::SqlValue::Time(s)) => {
                            assert_eq!(s, "14:30:00.123");
                        }
                        _ => panic!("Expected TIME literal"),
                    }
                }
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

// ========================================================================
// TIMESTAMP Literal Tests
// ========================================================================

#[test]
fn test_parse_timestamp_literal() {
    let result = Parser::parse_sql("SELECT TIMESTAMP '2024-01-01 14:30:00';");
    assert!(result.is_ok(), "TIMESTAMP literal should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, alias: _ } => {
                    match expr {
                        ast::Expression::Literal(types::SqlValue::Timestamp(s)) => {
                            assert_eq!(s, "2024-01-01 14:30:00");
                        }
                        _ => panic!("Expected TIMESTAMP literal, got {:?}", expr),
                    }
                }
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_timestamp_literal_with_fractional_seconds() {
    let result = Parser::parse_sql("SELECT TIMESTAMP '2024-01-01 14:30:00.123456';");
    assert!(result.is_ok(), "TIMESTAMP with fractional seconds should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        ast::Statement::Select(select) => {
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, alias: _ } => {
                    match expr {
                        ast::Expression::Literal(types::SqlValue::Timestamp(s)) => {
                            assert_eq!(s, "2024-01-01 14:30:00.123456");
                        }
                        _ => panic!("Expected TIMESTAMP literal"),
                    }
                }
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_timestamp_literal_in_insert() {
    let result = Parser::parse_sql(
        "INSERT INTO logs (id, created) VALUES (1, TIMESTAMP '2024-01-01 12:00:00');"
    );
    assert!(result.is_ok(), "TIMESTAMP in INSERT should parse: {:?}", result);
}

// ========================================================================
// INTERVAL Literal Tests
// ========================================================================

#[test]
fn test_parse_interval_year_literal() {
    let result = Parser::parse_sql("SELECT INTERVAL '5' YEAR;");
    assert!(result.is_ok(), "INTERVAL YEAR literal should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, alias: _ } => {
                    match expr {
                        ast::Expression::Literal(types::SqlValue::Interval(s)) => {
                            assert_eq!(s, "5 YEAR");
                        }
                        _ => panic!("Expected INTERVAL literal, got {:?}", expr),
                    }
                }
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_interval_month_literal() {
    let result = Parser::parse_sql("SELECT INTERVAL '3' MONTH;");
    assert!(result.is_ok(), "INTERVAL MONTH literal should parse: {:?}", result);
}

#[test]
fn test_parse_interval_day_literal() {
    let result = Parser::parse_sql("SELECT INTERVAL '30' DAY;");
    assert!(result.is_ok(), "INTERVAL DAY literal should parse: {:?}", result);
}

#[test]
fn test_parse_interval_hour_literal() {
    let result = Parser::parse_sql("SELECT INTERVAL '24' HOUR;");
    assert!(result.is_ok(), "INTERVAL HOUR literal should parse: {:?}", result);
}

#[test]
fn test_parse_interval_year_to_month_literal() {
    let result = Parser::parse_sql("SELECT INTERVAL '1-6' YEAR TO MONTH;");
    assert!(result.is_ok(), "INTERVAL YEAR TO MONTH literal should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        ast::Statement::Select(select) => {
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, alias: _ } => {
                    match expr {
                        ast::Expression::Literal(types::SqlValue::Interval(s)) => {
                            assert_eq!(s, "1-6 YEAR TO MONTH");
                        }
                        _ => panic!("Expected INTERVAL literal"),
                    }
                }
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_interval_day_to_hour_literal() {
    let result = Parser::parse_sql("SELECT INTERVAL '5 12' DAY TO HOUR;");
    assert!(result.is_ok(), "INTERVAL DAY TO HOUR literal should parse: {:?}", result);
}

#[test]
fn test_parse_interval_day_to_second_literal() {
    let result = Parser::parse_sql("SELECT INTERVAL '5 12:30:45' DAY TO SECOND;");
    assert!(result.is_ok(), "INTERVAL DAY TO SECOND literal should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        ast::Statement::Select(select) => {
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, alias: _ } => {
                    match expr {
                        ast::Expression::Literal(types::SqlValue::Interval(s)) => {
                            assert_eq!(s, "5 12:30:45 DAY TO SECOND");
                        }
                        _ => panic!("Expected INTERVAL literal"),
                    }
                }
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

// ========================================================================
// Mixed Literal Tests
// ========================================================================

#[test]
fn test_parse_mixed_date_time_literals() {
    let result = Parser::parse_sql(
        "SELECT DATE '2024-01-01', TIME '14:30:00', TIMESTAMP '2024-01-01 14:30:00';"
    );
    assert!(result.is_ok(), "Mixed date/time literals should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 3);
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_date_time_literals_in_comparison() {
    let result = Parser::parse_sql(
        "SELECT * FROM events WHERE event_date >= DATE '2024-01-01' AND start_time < TIME '18:00:00';"
    );
    assert!(result.is_ok(), "Date/time literals in comparison should parse: {:?}", result);
}

// ========================================================================
// CAST Function Tests
// ========================================================================

#[test]
fn test_parse_cast_integer_to_varchar() {
    let result = Parser::parse_sql("SELECT CAST(123 AS VARCHAR(10));");
    assert!(result.is_ok(), "CAST to VARCHAR should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, alias: _ } => {
                    match expr {
                        ast::Expression::Cast { expr: _, data_type } => {
                            match data_type {
                                types::DataType::Varchar { max_length: Some(10) } => {} // Success
                                _ => panic!("Expected VARCHAR(10), got {:?}", data_type),
                            }
                        }
                        _ => panic!("Expected CAST expression, got {:?}", expr),
                    }
                }
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_cast_varchar_to_integer() {
    let result = Parser::parse_sql("SELECT CAST('123' AS INTEGER);");
    assert!(result.is_ok(), "CAST to INTEGER should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        ast::Statement::Select(select) => {
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, alias: _ } => {
                    match expr {
                        ast::Expression::Cast { expr: _, data_type } => {
                            match data_type {
                                types::DataType::Integer => {} // Success
                                _ => panic!("Expected INTEGER, got {:?}", data_type),
                            }
                        }
                        _ => panic!("Expected CAST expression"),
                    }
                }
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_cast_to_smallint() {
    let result = Parser::parse_sql("SELECT CAST(value AS SMALLINT);");
    assert!(result.is_ok(), "CAST to SMALLINT should parse: {:?}", result);
}

#[test]
fn test_parse_cast_to_bigint() {
    let result = Parser::parse_sql("SELECT CAST(value AS BIGINT);");
    assert!(result.is_ok(), "CAST to BIGINT should parse: {:?}", result);
}

#[test]
fn test_parse_cast_to_float() {
    let result = Parser::parse_sql("SELECT CAST(123 AS FLOAT);");
    assert!(result.is_ok(), "CAST to FLOAT should parse: {:?}", result);
}

#[test]
fn test_parse_cast_to_double_precision() {
    let result = Parser::parse_sql("SELECT CAST(123 AS DOUBLE PRECISION);");
    assert!(result.is_ok(), "CAST to DOUBLE PRECISION should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        ast::Statement::Select(select) => {
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, alias: _ } => {
                    match expr {
                        ast::Expression::Cast { expr: _, data_type } => {
                            match data_type {
                                types::DataType::DoublePrecision => {} // Success
                                _ => panic!("Expected DOUBLE PRECISION, got {:?}", data_type),
                            }
                        }
                        _ => panic!("Expected CAST expression"),
                    }
                }
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_cast_to_numeric() {
    let result = Parser::parse_sql("SELECT CAST(value AS NUMERIC(10, 2));");
    assert!(result.is_ok(), "CAST to NUMERIC should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        ast::Statement::Select(select) => {
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, alias: _ } => {
                    match expr {
                        ast::Expression::Cast { expr: _, data_type } => {
                            match data_type {
                                types::DataType::Numeric { precision: 10, scale: 2 } => {} // Success
                                _ => panic!("Expected NUMERIC(10, 2), got {:?}", data_type),
                            }
                        }
                        _ => panic!("Expected CAST expression"),
                    }
                }
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_cast_to_date() {
    let result = Parser::parse_sql("SELECT CAST('2024-01-01' AS DATE);");
    assert!(result.is_ok(), "CAST to DATE should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        ast::Statement::Select(select) => {
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, alias: _ } => {
                    match expr {
                        ast::Expression::Cast { expr: _, data_type } => {
                            match data_type {
                                types::DataType::Date => {} // Success
                                _ => panic!("Expected DATE, got {:?}", data_type),
                            }
                        }
                        _ => panic!("Expected CAST expression"),
                    }
                }
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_cast_to_time() {
    let result = Parser::parse_sql("SELECT CAST(value AS TIME);");
    assert!(result.is_ok(), "CAST to TIME should parse: {:?}", result);
}

#[test]
fn test_parse_cast_to_timestamp() {
    let result = Parser::parse_sql("SELECT CAST(value AS TIMESTAMP);");
    assert!(result.is_ok(), "CAST to TIMESTAMP should parse: {:?}", result);
}

#[test]
fn test_parse_cast_in_where_clause() {
    let result = Parser::parse_sql(
        "SELECT * FROM users WHERE CAST(age AS VARCHAR(10)) = '25';"
    );
    assert!(result.is_ok(), "CAST in WHERE should parse: {:?}", result);
}

#[test]
fn test_parse_cast_nested_expression() {
    let result = Parser::parse_sql("SELECT CAST((value + 10) AS BIGINT);");
    assert!(result.is_ok(), "CAST with nested expression should parse: {:?}", result);
}

#[test]
fn test_parse_multiple_casts() {
    let result = Parser::parse_sql(
        "SELECT CAST(a AS INTEGER), CAST(b AS VARCHAR(20)), CAST(c AS FLOAT);"
    );
    assert!(result.is_ok(), "Multiple CASTs should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 3, "Should have 3 select items");
        }
        _ => panic!("Expected SELECT statement"),
    }
}
