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

// ========================================================================
// Spatial Data Types (SQL/MM Standard) - Issue #781
// These are not part of SQL:1999 but should parse gracefully as UserDefined types
// ========================================================================

#[test]
fn test_parse_create_table_multipolygon() {
    let result = Parser::parse_sql("CREATE TABLE t1(c1 MULTIPOLYGON, c2 MULTIPOLYGON);");
    assert!(result.is_ok(), "Should parse MULTIPOLYGON type");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 2);

            match &create.columns[0].data_type {
                types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "MULTIPOLYGON");
                }
                _ => panic!("Expected UserDefined MULTIPOLYGON, got {:?}", create.columns[0].data_type),
            }

            match &create.columns[1].data_type {
                types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "MULTIPOLYGON");
                }
                _ => panic!("Expected UserDefined MULTIPOLYGON, got {:?}", create.columns[1].data_type),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_all_spatial_types() {
    let result = Parser::parse_sql(
        "CREATE TABLE spatial (
            pt POINT,
            line LINESTRING,
            poly POLYGON,
            mpt MULTIPOINT,
            mline MULTILINESTRING,
            mpoly MULTIPOLYGON,
            geom GEOMETRY,
            geomcoll GEOMETRYCOLLECTION
        );",
    );
    assert!(result.is_ok(), "Should parse all spatial types");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 8);

            let expected_types = vec![
                "POINT",
                "LINESTRING",
                "POLYGON",
                "MULTIPOINT",
                "MULTILINESTRING",
                "MULTIPOLYGON",
                "GEOMETRY",
                "GEOMETRYCOLLECTION",
            ];

            for (i, expected_type) in expected_types.iter().enumerate() {
                match &create.columns[i].data_type {
                    types::DataType::UserDefined { type_name } => {
                        assert_eq!(
                            type_name, expected_type,
                            "Column {} should have type {}",
                            i, expected_type
                        );
                    }
                    _ => panic!(
                        "Expected UserDefined {}, got {:?}",
                        expected_type, create.columns[i].data_type
                    ),
                }
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_spatial_types_case_insensitive() {
    // Test lowercase, uppercase, and mixed case
    let lowercase_result = Parser::parse_sql("CREATE TABLE t1(c1 multipolygon);");
    let uppercase_result = Parser::parse_sql("CREATE TABLE t2(c1 MULTIPOLYGON);");
    let mixed_result = Parser::parse_sql("CREATE TABLE t3(c1 MultiPolygon);");

    assert!(lowercase_result.is_ok(), "lowercase should parse");
    assert!(uppercase_result.is_ok(), "uppercase should parse");
    assert!(mixed_result.is_ok(), "mixed case should parse");

    // All should produce the same normalized type
    if let Ok(ast::Statement::CreateTable(t1)) = lowercase_result {
        if let Ok(ast::Statement::CreateTable(t2)) = uppercase_result {
            if let Ok(ast::Statement::CreateTable(t3)) = mixed_result {
                match (&t1.columns[0].data_type, &t2.columns[0].data_type, &t3.columns[0].data_type) {
                    (
                        types::DataType::UserDefined { type_name: name1 },
                        types::DataType::UserDefined { type_name: name2 },
                        types::DataType::UserDefined { type_name: name3 },
                    ) => {
                        assert_eq!(name1, "MULTIPOLYGON", "Should normalize to uppercase");
                        assert_eq!(name2, "MULTIPOLYGON", "Should normalize to uppercase");
                        assert_eq!(name3, "MULTIPOLYGON", "Should normalize to uppercase");
                    }
                    _ => panic!("Expected UserDefined types for all variants"),
                }
            }
        }
    }
}

#[test]
fn test_unknown_type_still_fails() {
    // Non-spatial unknown types should still fail
    let result = Parser::parse_sql("CREATE TABLE t1(c1 UNKNOWNTYPE);");
    assert!(result.is_err(), "Unknown non-spatial types should fail");

    if let Err(e) = result {
        assert!(
            e.to_string().contains("Unknown data type: UNKNOWNTYPE"),
            "Error should mention unknown type"
        );
    }
}

#[test]
fn test_sqllogictest_multipolygon_with_comment() {
    // Test from actual SQLLogicTest suite
    // Note: This test is currently ignored because COMMENT clause may not be fully supported
    let result = Parser::parse_sql(
        "CREATE TABLE `t1710a` (`c1` MULTIPOLYGON COMMENT 'text155459', `c2` MULTIPOLYGON COMMENT 'text155461');",
    );

    if let Err(ref e) = result {
        eprintln!("Parse error: {}", e);
    }

    assert!(result.is_ok(), "Should parse MULTIPOLYGON with COMMENT clause");
    let stmt = result.unwrap();

    match stmt {
    ast::Statement::CreateTable(create) => {
    assert_eq!(create.columns.len(), 2);

    // Check first column
    assert_eq!(create.columns[0].name, "c1");
    match &create.columns[0].data_type {
    types::DataType::UserDefined { type_name } => {
        assert_eq!(type_name, "MULTIPOLYGON");
        }
            _ => panic!("Expected UserDefined MULTIPOLYGON, got {:?}", create.columns[0].data_type),
        }
            assert_eq!(create.columns[0].comment, Some("text155459".to_string()));

            // Check second column
            assert_eq!(create.columns[1].name, "c2");
            match &create.columns[1].data_type {
                types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "MULTIPOLYGON");
                }
                _ => panic!("Expected UserDefined MULTIPOLYGON, got {:?}", create.columns[1].data_type),
            }
            assert_eq!(create.columns[1].comment, Some("text155461".to_string()));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_sqllogictest_multipolygon_basic() {
    // Simplified test without COMMENT clause to verify core spatial type parsing
    let result = Parser::parse_sql(
        "CREATE TABLE t1710a (c1 MULTIPOLYGON, c2 MULTIPOLYGON);",
    );
    assert!(result.is_ok(), "Should parse MULTIPOLYGON columns");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 2);

            match &create.columns[0].data_type {
                types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "MULTIPOLYGON");
                }
                _ => panic!("Expected UserDefined MULTIPOLYGON, got {:?}", create.columns[0].data_type),
            }

            match &create.columns[1].data_type {
                types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "MULTIPOLYGON");
                }
                _ => panic!("Expected UserDefined MULTIPOLYGON, got {:?}", create.columns[1].data_type),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_default_before_comment_mysql_standard() {
    // Test MySQL standard order: DEFAULT before COMMENT
    // Per MySQL 8.4 Reference Manual:
    // column_definition: data_type [DEFAULT {literal | (expr)}] [COMMENT 'string']
    let result = Parser::parse_sql(
        "CREATE TABLE t (col INT DEFAULT 5 COMMENT 'test column');",
    );

    if let Err(ref e) = result {
        eprintln!("Parse error: {}", e);
    }

    assert!(result.is_ok(), "Should parse DEFAULT before COMMENT (MySQL standard order)");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 1);

            let col = &create.columns[0];
            assert_eq!(col.name, "COL");

            // Verify DEFAULT value is parsed
            assert!(col.default_value.is_some(), "Should have default value");

            // Verify COMMENT is parsed
            assert_eq!(col.comment, Some("test column".to_string()));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}
