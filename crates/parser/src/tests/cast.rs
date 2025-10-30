//! Tests for CAST function parsing
//!
//! Covers:
//! - CAST to integer types (SMALLINT, INTEGER, BIGINT)
//! - CAST to floating-point types (FLOAT, DOUBLE PRECISION)
//! - CAST to string types (VARCHAR)
//! - CAST to date/time types (DATE, TIME, TIMESTAMP)
//! - CAST to exact numeric types (NUMERIC)
//! - CAST in various contexts (WHERE clauses, nested expressions)

use super::*;

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
