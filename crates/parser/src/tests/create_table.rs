use super::*;

// ========================================================================
// CREATE TABLE Statement Tests
// ========================================================================

#[test]
fn test_parse_create_table_basic() {
    let result = Parser::parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR(100));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "users");
            assert_eq!(create.columns.len(), 2);
            assert_eq!(create.columns[0].name, "id");
            assert_eq!(create.columns[1].name, "name");
            match create.columns[0].data_type {
                types::DataType::Integer => {} // Success
                _ => panic!("Expected Integer data type"),
            }
            match create.columns[1].data_type {
                types::DataType::Varchar { max_length: 100 } => {} // Success
                _ => panic!("Expected VARCHAR(100) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_various_types() {
    let result =
        Parser::parse_sql("CREATE TABLE test (id INT, flag BOOLEAN, birth DATE, code CHAR(5));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "test");
            assert_eq!(create.columns.len(), 4);
            match create.columns[0].data_type {
                types::DataType::Integer => {} // Success
                _ => panic!("Expected Integer"),
            }
            match create.columns[1].data_type {
                types::DataType::Boolean => {} // Success
                _ => panic!("Expected Boolean"),
            }
            match create.columns[2].data_type {
                types::DataType::Date => {} // Success
                _ => panic!("Expected Date"),
            }
            match create.columns[3].data_type {
                types::DataType::Character { length: 5 } => {} // Success
                _ => panic!("Expected CHAR(5)"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

// ========================================================================
// Phase 2 Type System Tests - All SQL:1999 Core Types
// ========================================================================

#[test]
fn test_parse_create_table_integer_types() {
    let result = Parser::parse_sql(
        "CREATE TABLE numbers (small SMALLINT, medium INTEGER, big BIGINT);"
    );
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
    let result = Parser::parse_sql(
        "CREATE TABLE floats (a FLOAT, b REAL, c DOUBLE PRECISION);"
    );
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
    let result = Parser::parse_sql(
        "CREATE TABLE prices (amount NUMERIC(10, 2), total DECIMAL(15, 4));"
    );
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
                types::DataType::Decimal { precision: 15, scale: 4 } => {} // Success
                _ => panic!("Expected DECIMAL(15, 4), got {:?}", create.columns[1].data_type),
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
                _ => panic!("Expected NUMERIC(38, 0) default, got {:?}", create.columns[0].data_type),
            }

            match create.columns[1].data_type {
                types::DataType::Decimal { precision: 38, scale: 0 } => {} // Success
                _ => panic!("Expected DECIMAL(38, 0) default, got {:?}", create.columns[1].data_type),
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
                _ => panic!("Expected TIME without timezone, got {:?}", create.columns[0].data_type),
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
fn test_parse_create_table_time_without_timezone_explicit() {
    let result = Parser::parse_sql("CREATE TABLE events (start_time TIME WITHOUT TIME ZONE);");
    assert!(result.is_ok(), "Should parse TIME WITHOUT TIME ZONE");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            match create.columns[0].data_type {
                types::DataType::Time { with_timezone: false } => {} // Success
                _ => panic!("Expected TIME WITHOUT TIME ZONE, got {:?}", create.columns[0].data_type),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_timestamp_types() {
    let result = Parser::parse_sql(
        "CREATE TABLE logs (
            created TIMESTAMP,
            modified TIMESTAMP WITH TIME ZONE,
            deleted TIMESTAMP WITHOUT TIME ZONE
        );"
    );
    assert!(result.is_ok(), "Should parse all TIMESTAMP variants");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 3);

            match create.columns[0].data_type {
                types::DataType::Timestamp { with_timezone: false } => {} // Default
                _ => panic!("Expected TIMESTAMP without timezone, got {:?}", create.columns[0].data_type),
            }

            match create.columns[1].data_type {
                types::DataType::Timestamp { with_timezone: true } => {} // Success
                _ => panic!("Expected TIMESTAMP WITH TIME ZONE, got {:?}", create.columns[1].data_type),
            }

            match create.columns[2].data_type {
                types::DataType::Timestamp { with_timezone: false } => {} // Success
                _ => panic!("Expected TIMESTAMP WITHOUT TIME ZONE, got {:?}", create.columns[2].data_type),
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
        );"
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
        ast::Statement::CreateTable(create) => {
            match &create.columns[0].data_type {
                types::DataType::Interval { start_field, end_field } => {
                    assert!(matches!(start_field, types::IntervalField::Year));
                    assert!(matches!(end_field, Some(types::IntervalField::Month)));
                }
                _ => panic!("Expected INTERVAL YEAR TO MONTH, got {:?}", create.columns[0].data_type),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_interval_day_to_second() {
    let result = Parser::parse_sql("CREATE TABLE test (duration INTERVAL DAY TO SECOND);");
    assert!(result.is_ok(), "Should parse INTERVAL DAY TO SECOND");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            match &create.columns[0].data_type {
                types::DataType::Interval { start_field, end_field } => {
                    assert!(matches!(start_field, types::IntervalField::Day));
                    assert!(matches!(end_field, Some(types::IntervalField::Second)));
                }
                _ => panic!("Expected INTERVAL DAY TO SECOND, got {:?}", create.columns[0].data_type),
            }
        }
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
        );"
    );
    assert!(result.is_ok(), "Should parse all Phase 2 types together");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 14, "Should have 14 columns");
            assert_eq!(create.table_name, "comprehensive");
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

// ========================================================================
// Constraint Tests (Issue #214)
// ========================================================================

#[test]
fn test_parse_create_table_with_primary_key() {
    let result = Parser::parse_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100));");
    assert!(result.is_ok(), "Should parse column-level PRIMARY KEY");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "users");
            assert_eq!(create.columns.len(), 2);
            assert_eq!(create.columns[0].name, "id");
            assert_eq!(create.columns[0].constraints.len(), 1);
            assert!(matches!(
                create.columns[0].constraints[0],
                ast::ColumnConstraint::PrimaryKey
            ));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_unique() {
    let result = Parser::parse_sql("CREATE TABLE users (email VARCHAR(100) UNIQUE);");
    assert!(result.is_ok(), "Should parse UNIQUE constraint");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].constraints.len(), 1);
            assert!(matches!(
                create.columns[0].constraints[0],
                ast::ColumnConstraint::Unique
            ));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_check_constraint() {
    let result = Parser::parse_sql("CREATE TABLE products (price NUMERIC(10, 2) CHECK (price > 0));");
    assert!(result.is_ok(), "Should parse CHECK constraint");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].constraints.len(), 1);
            assert!(matches!(
                create.columns[0].constraints[0],
                ast::ColumnConstraint::Check(_)
            ));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_references() {
    let result = Parser::parse_sql("CREATE TABLE orders (customer_id INTEGER REFERENCES customers(id));");
    assert!(result.is_ok(), "Should parse REFERENCES constraint");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].constraints.len(), 1);
            match &create.columns[0].constraints[0] {
                ast::ColumnConstraint::References { table, column } => {
                    assert_eq!(table, "customers");
                    assert_eq!(column, "id");
                }
                _ => panic!("Expected REFERENCES constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_table_level_primary_key() {
    let result = Parser::parse_sql(
        "CREATE TABLE order_items (
            order_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            PRIMARY KEY (order_id, product_id)
        );"
    );
    assert!(result.is_ok(), "Should parse table-level PRIMARY KEY");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_constraints.len(), 1);
            match &create.table_constraints[0] {
                ast::TableConstraint::PrimaryKey { columns } => {
                    assert_eq!(columns.len(), 2);
                    assert_eq!(columns[0], "order_id");
                    assert_eq!(columns[1], "product_id");
                }
                _ => panic!("Expected PRIMARY KEY constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_foreign_key() {
    let result = Parser::parse_sql(
        "CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            FOREIGN KEY (customer_id) REFERENCES customers(id)
        );"
    );
    assert!(result.is_ok(), "Should parse FOREIGN KEY constraint");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_constraints.len(), 1);
            match &create.table_constraints[0] {
                ast::TableConstraint::ForeignKey {
                    columns,
                    references_table,
                    references_columns,
                } => {
                    assert_eq!(columns.len(), 1);
                    assert_eq!(columns[0], "customer_id");
                    assert_eq!(references_table, "customers");
                    assert_eq!(references_columns.len(), 1);
                    assert_eq!(references_columns[0], "id");
                }
                _ => panic!("Expected FOREIGN KEY constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_table_level_unique() {
    let result = Parser::parse_sql(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            email VARCHAR(100),
            username VARCHAR(50),
            UNIQUE (email, username)
        );"
    );
    assert!(result.is_ok(), "Should parse table-level UNIQUE constraint");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_constraints.len(), 1);
            match &create.table_constraints[0] {
                ast::TableConstraint::Unique { columns } => {
                    assert_eq!(columns.len(), 2);
                    assert_eq!(columns[0], "email");
                    assert_eq!(columns[1], "username");
                }
                _ => panic!("Expected UNIQUE constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_table_level_check() {
    let result = Parser::parse_sql(
        "CREATE TABLE products (
            price NUMERIC(10, 2),
            discount NUMERIC(10, 2),
            CHECK (discount < price)
        );"
    );
    assert!(result.is_ok(), "Should parse table-level CHECK constraint");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_constraints.len(), 1);
            assert!(matches!(
                create.table_constraints[0],
                ast::TableConstraint::Check { .. }
            ));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_northwind_categories_table() {
    let result = Parser::parse_sql(
        "CREATE TABLE Categories (
            CategoryID INTEGER PRIMARY KEY,
            CategoryName VARCHAR(15),
            Description VARCHAR(255)
        );"
    );
    assert!(result.is_ok(), "Should parse northwind Categories table");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "Categories");
            assert_eq!(create.columns.len(), 3);
            assert_eq!(create.columns[0].name, "CategoryID");
            assert_eq!(create.columns[0].constraints.len(), 1);
            assert!(matches!(
                create.columns[0].constraints[0],
                ast::ColumnConstraint::PrimaryKey
            ));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_multiple_constraints() {
    let result = Parser::parse_sql(
        "CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            email VARCHAR(100) UNIQUE,
            salary NUMERIC(10, 2) CHECK (salary > 0),
            department_id INTEGER REFERENCES departments(id)
        );"
    );
    assert!(result.is_ok(), "Should parse multiple column constraints");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 4);

            // id has PRIMARY KEY
            assert_eq!(create.columns[0].constraints.len(), 1);
            assert!(matches!(
                create.columns[0].constraints[0],
                ast::ColumnConstraint::PrimaryKey
            ));

            // email has UNIQUE
            assert_eq!(create.columns[1].constraints.len(), 1);
            assert!(matches!(
                create.columns[1].constraints[0],
                ast::ColumnConstraint::Unique
            ));

            // salary has CHECK
            assert_eq!(create.columns[2].constraints.len(), 1);
            assert!(matches!(
                create.columns[2].constraints[0],
                ast::ColumnConstraint::Check(_)
            ));

            // department_id has REFERENCES
            assert_eq!(create.columns[3].constraints.len(), 1);
            assert!(matches!(
                create.columns[3].constraints[0],
                ast::ColumnConstraint::References { .. }
            ));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_northwind_products_table() {
    let result = Parser::parse_sql(
        "CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            product_name VARCHAR(100) NOT NULL,
            category_id INTEGER,
            unit_price DECIMAL(10, 2),
            FOREIGN KEY (category_id) REFERENCES categories(category_id)
        );"
    );
    assert!(result.is_ok(), "Should parse northwind products table with FOREIGN KEY");
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "products");
            assert_eq!(create.columns.len(), 4);

            // product_id has PRIMARY KEY
            assert_eq!(create.columns[0].name, "product_id");
            assert_eq!(create.columns[0].constraints.len(), 1);
            assert!(matches!(
                create.columns[0].constraints[0],
                ast::ColumnConstraint::PrimaryKey
            ));

            // product_name has NOT NULL (nullable = false)
            assert_eq!(create.columns[1].name, "product_name");
            assert!(!create.columns[1].nullable, "product_name should be NOT NULL");

            // Table has FOREIGN KEY constraint
            assert_eq!(create.table_constraints.len(), 1);
            match &create.table_constraints[0] {
                ast::TableConstraint::ForeignKey {
                    columns,
                    references_table,
                    references_columns,
                } => {
                    assert_eq!(columns.len(), 1);
                    assert_eq!(columns[0], "category_id");
                    assert_eq!(references_table, "categories");
                    assert_eq!(references_columns.len(), 1);
                    assert_eq!(references_columns[0], "category_id");
                }
                _ => panic!("Expected FOREIGN KEY constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}
