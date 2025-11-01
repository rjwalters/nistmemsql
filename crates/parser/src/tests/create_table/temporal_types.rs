use super::super::*;

// ========================================================================
// Temporal Data Types - TIME, TIMESTAMP, and INTERVAL Types
// ========================================================================

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
