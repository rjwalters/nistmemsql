use super::super::*;

// ========================================================================
// Phase 2 Type System Tests - All SQL:1999 Core Types
// ========================================================================

#[test]
fn test_parse_create_table_integer_types() {
    let result =
        Parser::parse_sql("CREATE TABLE numbers (small SMALLINT, medium INTEGER, big BIGINT);");
    assert!(result.is_ok(), "Should parse integer types");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 3);

            match create.columns[0].data_type {
                types::DataType::Smallint => {} // Success
                _ => panic!("Expected SMALLINT, got {:?}", create.columns[0].data_type),
            }

            match create.columns[1].data_type {
                types::DataType::Integer => {} // Success
                _ => panic!("Expected INTEGER, got {:?}", create.columns[1].data_type),
            }

            match create.columns[2].data_type {
                types::DataType::Bigint => {} // Success
                _ => panic!("Expected BIGINT, got {:?}", create.columns[2].data_type),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_float_types() {
    let result = Parser::parse_sql("CREATE TABLE floats (a FLOAT, b REAL, c DOUBLE PRECISION);");
    assert!(result.is_ok(), "Should parse floating point types");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 3);

            match create.columns[0].data_type {
                types::DataType::Float { .. } => {} // Success
                _ => panic!("Expected FLOAT, got {:?}", create.columns[0].data_type),
            }

            match create.columns[1].data_type {
                types::DataType::Real => {} // Success
                _ => panic!("Expected REAL, got {:?}", create.columns[1].data_type),
            }

            match create.columns[2].data_type {
                types::DataType::DoublePrecision => {} // Success
                _ => panic!("Expected DOUBLE PRECISION, got {:?}", create.columns[2].data_type),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_double_without_precision() {
    let result = Parser::parse_sql("CREATE TABLE test (value DOUBLE);");
    assert!(result.is_ok(), "DOUBLE without PRECISION should parse");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            match create.columns[0].data_type {
                types::DataType::DoublePrecision => {} // Success
                _ => panic!("Expected DOUBLE to be treated as DOUBLE PRECISION"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_numeric_with_precision_and_scale() {
    let result =
        Parser::parse_sql("CREATE TABLE prices (amount NUMERIC(10, 2), total DECIMAL(15, 4));");
    assert!(result.is_ok(), "Should parse NUMERIC with precision and scale");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 2);

            match create.columns[0].data_type {
                types::DataType::Numeric { precision: 10, scale: 2 } => {} // Success
                _ => panic!("Expected NUMERIC(10, 2), got {:?}", create.columns[0].data_type),
            }

            match create.columns[1].data_type {
                types::DataType::Numeric { precision: 15, scale: 4 } => {} // Success
                _ => panic!("Expected NUMERIC(15, 4), got {:?}", create.columns[1].data_type),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_numeric_with_precision_only() {
    let result = Parser::parse_sql("CREATE TABLE test (price NUMERIC(10));");
    assert!(result.is_ok(), "Should parse NUMERIC with precision only");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            match create.columns[0].data_type {
                types::DataType::Numeric { precision: 10, scale: 0 } => {} // Scale defaults to 0
                _ => panic!("Expected NUMERIC(10, 0), got {:?}", create.columns[0].data_type),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_numeric_without_parameters() {
    let result = Parser::parse_sql("CREATE TABLE test (price NUMERIC, amount DECIMAL);");
    assert!(result.is_ok(), "Should parse NUMERIC/DECIMAL without parameters");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            // Should default to (38, 0) per SQL standard
            match create.columns[0].data_type {
                types::DataType::Numeric { precision: 38, scale: 0 } => {} // Success
                _ => {
                    panic!("Expected NUMERIC(38, 0) default, got {:?}", create.columns[0].data_type)
                }
            }

            match create.columns[1].data_type {
                types::DataType::Numeric { precision: 38, scale: 0 } => {} // Success
                _ => {
                    panic!("Expected NUMERIC(38, 0) default, got {:?}", create.columns[1].data_type)
                }
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_time_without_timezone() {
    let result = Parser::parse_sql("CREATE TABLE events (start_time TIME);");
    assert!(result.is_ok(), "Should parse TIME");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            match create.columns[0].data_type {
                types::DataType::Time { with_timezone: false } => {} // Success
                _ => {
                    panic!("Expected TIME without timezone, got {:?}", create.columns[0].data_type)
                }
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_time_with_timezone() {
    let result = Parser::parse_sql("CREATE TABLE events (start_time TIME WITH TIME ZONE);");
    assert!(result.is_ok(), "Should parse TIME WITH TIME ZONE");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            match create.columns[0].data_type {
                types::DataType::Time { with_timezone: true } => {} // Success
                _ => panic!("Expected TIME WITH TIME ZONE, got {:?}", create.columns[0].data_type),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
#[ignore = "TIME WITHOUT TIME ZONE syntax not yet implemented"]
fn test_parse_create_table_time_without_timezone_explicit() {
    let result = Parser::parse_sql("CREATE TABLE events (start_time TIME WITHOUT TIME ZONE);");
    if let Err(ref e) = result {
        eprintln!("Parse error: {:?}", e);
    }
    assert!(result.is_ok(), "Should parse TIME WITHOUT TIME ZONE");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            match create.columns[0].data_type {
                types::DataType::Time { with_timezone: false } => {} // Success
                _ => {
                    panic!("Expected TIME WITHOUT TIME ZONE, got {:?}", create.columns[0].data_type)
                }
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
#[ignore = "TIMESTAMP WITH/WITHOUT TIME ZONE syntax not yet implemented"]
fn test_parse_create_table_timestamp_types() {
    let result = Parser::parse_sql(
        "CREATE TABLE logs (
            created TIMESTAMP,
            modified TIMESTAMP WITH TIME ZONE,
            deleted TIMESTAMP WITHOUT TIME ZONE
        );",
    );
    assert!(result.is_ok(), "Should parse all TIMESTAMP variants");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 3);

            match create.columns[0].data_type {
                types::DataType::Timestamp { with_timezone: false } => {} // Default
                _ => panic!(
                    "Expected TIMESTAMP without timezone, got {:?}",
                    create.columns[0].data_type
                ),
            }

            match create.columns[1].data_type {
                types::DataType::Timestamp { with_timezone: true } => {} // Success
                _ => panic!(
                    "Expected TIMESTAMP WITH TIME ZONE, got {:?}",
                    create.columns[1].data_type
                ),
            }

            match create.columns[2].data_type {
                types::DataType::Timestamp { with_timezone: false } => {} // Success
                _ => panic!(
                    "Expected TIMESTAMP WITHOUT TIME ZONE, got {:?}",
                    create.columns[2].data_type
                ),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_interval_single_field() {
    let result = Parser::parse_sql(
        "CREATE TABLE intervals (
            years INTERVAL YEAR,
            months INTERVAL MONTH,
            days INTERVAL DAY,
            hours INTERVAL HOUR,
            minutes INTERVAL MINUTE,
            seconds INTERVAL SECOND
        );",
    );
    assert!(result.is_ok(), "Should parse single-field intervals");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 6);

            match &create.columns[0].data_type {
                types::DataType::Interval { start_field, end_field } => {
                    assert!(matches!(start_field, types::IntervalField::Year));
                    assert!(end_field.is_none());
                }
                _ => panic!("Expected INTERVAL YEAR, got {:?}", create.columns[0].data_type),
            }

            match &create.columns[1].data_type {
                types::DataType::Interval { start_field, end_field } => {
                    assert!(matches!(start_field, types::IntervalField::Month));
                    assert!(end_field.is_none());
                }
                _ => panic!("Expected INTERVAL MONTH, got {:?}", create.columns[1].data_type),
            }

            match &create.columns[2].data_type {
                types::DataType::Interval { start_field, end_field } => {
                    assert!(matches!(start_field, types::IntervalField::Day));
                    assert!(end_field.is_none());
                }
                _ => panic!("Expected INTERVAL DAY, got {:?}", create.columns[2].data_type),
            }

            match &create.columns[3].data_type {
                types::DataType::Interval { start_field, end_field } => {
                    assert!(matches!(start_field, types::IntervalField::Hour));
                    assert!(end_field.is_none());
                }
                _ => panic!("Expected INTERVAL HOUR, got {:?}", create.columns[3].data_type),
            }

            match &create.columns[4].data_type {
                types::DataType::Interval { start_field, end_field } => {
                    assert!(matches!(start_field, types::IntervalField::Minute));
                    assert!(end_field.is_none());
                }
                _ => panic!("Expected INTERVAL MINUTE, got {:?}", create.columns[4].data_type),
            }

            match &create.columns[5].data_type {
                types::DataType::Interval { start_field, end_field } => {
                    assert!(matches!(start_field, types::IntervalField::Second));
                    assert!(end_field.is_none());
                }
                _ => panic!("Expected INTERVAL SECOND, got {:?}", create.columns[5].data_type),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_interval_year_to_month() {
    let result = Parser::parse_sql("CREATE TABLE test (duration INTERVAL YEAR TO MONTH);");
    assert!(result.is_ok(), "Should parse INTERVAL YEAR TO MONTH");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => match &create.columns[0].data_type {
            types::DataType::Interval { start_field, end_field } => {
                assert!(matches!(start_field, types::IntervalField::Year));
                assert!(matches!(end_field, Some(types::IntervalField::Month)));
            }
            _ => panic!("Expected INTERVAL YEAR TO MONTH, got {:?}", create.columns[0].data_type),
        },
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_interval_day_to_second() {
    let result = Parser::parse_sql("CREATE TABLE test (duration INTERVAL DAY TO SECOND);");
    assert!(result.is_ok(), "Should parse INTERVAL DAY TO SECOND");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => match &create.columns[0].data_type {
            types::DataType::Interval { start_field, end_field } => {
                assert!(matches!(start_field, types::IntervalField::Day));
                assert!(matches!(end_field, Some(types::IntervalField::Second)));
            }
            _ => panic!("Expected INTERVAL DAY TO SECOND, got {:?}", create.columns[0].data_type),
        },
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_all_phase2_types() {
    let result = Parser::parse_sql(
        "CREATE TABLE comprehensive (
            tiny SMALLINT,
            normal INTEGER,
            huge BIGINT,
            approximate FLOAT,
            precise_float REAL,
            very_precise DOUBLE PRECISION,
            money NUMERIC(10, 2),
            fixed_char CHAR(10),
            var_char VARCHAR(255),
            flag BOOLEAN,
            birth DATE,
            meeting TIME WITH TIME ZONE,
            created TIMESTAMP,
            age INTERVAL YEAR TO MONTH
        );",
    );
    assert!(result.is_ok(), "Should parse all Phase 2 types together");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 14, "Should have 14 columns");
            assert_eq!(create.table_name, "COMPREHENSIVE");
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_text_type() {
    let result = Parser::parse_sql("CREATE TABLE t1(a TEXT, b TEXT);");
    assert!(result.is_ok(), "Should parse TEXT type");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 2);

            // TEXT should map to Varchar with no length limit
            match &create.columns[0].data_type {
                types::DataType::Varchar { max_length: None } => {} // Success
                _ => panic!("Expected TEXT to map to VARCHAR, got {:?}", create.columns[0].data_type),
            }

            match &create.columns[1].data_type {
                types::DataType::Varchar { max_length: None } => {} // Success
                _ => panic!("Expected TEXT to map to VARCHAR, got {:?}", create.columns[1].data_type),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_text_varchar_compatibility() {
    // Verify TEXT and VARCHAR can be used interchangeably
    let text_result = Parser::parse_sql("CREATE TABLE t1(a TEXT);");
    let varchar_result = Parser::parse_sql("CREATE TABLE t2(a VARCHAR);");

    assert!(text_result.is_ok(), "TEXT should parse successfully");
    assert!(varchar_result.is_ok(), "VARCHAR should parse successfully");

    // Both should produce Varchar { max_length: None }
    if let Ok(ast::Statement::CreateTable(text_table)) = text_result {
        if let Ok(ast::Statement::CreateTable(varchar_table)) = varchar_result {
            assert_eq!(
                text_table.columns[0].data_type, varchar_table.columns[0].data_type,
                "TEXT and VARCHAR should be equivalent types"
            );
        }
    }
}
