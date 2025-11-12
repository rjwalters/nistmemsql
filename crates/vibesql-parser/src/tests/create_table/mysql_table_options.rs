use super::super::*;

/// Test parsing of MySQL table options
#[test]
fn test_parse_create_table_with_key_block_size() {
    let result = Parser::parse_sql("CREATE TABLE t1 (c1 INT) KEY_BLOCK_SIZE 4;");
    assert!(result.is_ok(), "Should parse KEY_BLOCK_SIZE option");

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_options.len(), 1);
            match &create.table_options[0] {
                vibesql_ast::TableOption::KeyBlockSize(Some(value)) => {
                    assert_eq!(*value, 4);
                }
                _ => panic!("Expected KeyBlockSize option"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_connection() {
    let result = Parser::parse_sql("CREATE TABLE t1 (c1 INT) CONNECTION 'conn_string';");
    assert!(result.is_ok(), "Should parse CONNECTION option");

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_options.len(), 1);
            match &create.table_options[0] {
                vibesql_ast::TableOption::Connection(Some(value)) => {
                    assert_eq!(value, "conn_string");
                }
                _ => panic!("Expected Connection option"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_insert_method() {
    let result = Parser::parse_sql("CREATE TABLE t1 (c1 INT) INSERT_METHOD = LAST;");
    assert!(result.is_ok(), "Should parse INSERT_METHOD option with =");

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_options.len(), 1);
            match &create.table_options[0] {
                vibesql_ast::TableOption::InsertMethod(method) => {
                    assert_eq!(method, &vibesql_ast::InsertMethod::Last);
                }
                _ => panic!("Expected InsertMethod option"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_insert_method_without_equals() {
    let result = Parser::parse_sql("CREATE TABLE t1 (c1 INT) INSERT_METHOD LAST;");
    assert!(result.is_ok(), "Should parse INSERT_METHOD option without =");

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_options.len(), 1);
            match &create.table_options[0] {
                vibesql_ast::TableOption::InsertMethod(method) => {
                    assert_eq!(method, &vibesql_ast::InsertMethod::Last);
                }
                _ => panic!("Expected InsertMethod option"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_insert_method_first() {
    let result = Parser::parse_sql("CREATE TABLE t1 (c1 INT) INSERT_METHOD FIRST;");
    assert!(result.is_ok(), "Should parse INSERT_METHOD FIRST without =");

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_options.len(), 1);
            match &create.table_options[0] {
                vibesql_ast::TableOption::InsertMethod(method) => {
                    assert_eq!(method, &vibesql_ast::InsertMethod::First);
                }
                _ => panic!("Expected InsertMethod option"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_insert_method_no() {
    let result = Parser::parse_sql("CREATE TABLE t1 (c1 INT) INSERT_METHOD NO;");
    assert!(result.is_ok(), "Should parse INSERT_METHOD NO without =");

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_options.len(), 1);
            match &create.table_options[0] {
                vibesql_ast::TableOption::InsertMethod(method) => {
                    assert_eq!(method, &vibesql_ast::InsertMethod::No);
                }
                _ => panic!("Expected InsertMethod option"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_row_format() {
    let result = Parser::parse_sql("CREATE TABLE t1 (c1 INT) ROW_FORMAT COMPRESSED;");
    assert!(result.is_ok(), "Should parse ROW_FORMAT option");

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_options.len(), 1);
            match &create.table_options[0] {
                vibesql_ast::TableOption::RowFormat(Some(format)) => {
                    assert_eq!(format, &vibesql_ast::RowFormat::Compressed);
                }
                _ => panic!("Expected RowFormat option"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_multiple_options() {
    let result = Parser::parse_sql(
        "CREATE TABLE t1 (c1 INT) KEY_BLOCK_SIZE 4 CONNECTION 'conn' ROW_FORMAT COMPRESSED;",
    );
    assert!(result.is_ok(), "Should parse multiple table options");

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_options.len(), 3);

            // Check KEY_BLOCK_SIZE
            match &create.table_options[0] {
                vibesql_ast::TableOption::KeyBlockSize(Some(value)) => {
                    assert_eq!(*value, 4);
                }
                _ => panic!("Expected KeyBlockSize option"),
            }

            // Check CONNECTION
            match &create.table_options[1] {
                vibesql_ast::TableOption::Connection(Some(value)) => {
                    assert_eq!(value, "conn");
                }
                _ => panic!("Expected Connection option"),
            }

            // Check ROW_FORMAT
            match &create.table_options[2] {
                vibesql_ast::TableOption::RowFormat(Some(format)) => {
                    assert_eq!(format, &vibesql_ast::RowFormat::Compressed);
                }
                _ => panic!("Expected RowFormat option"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_comment_option() {
    let result = Parser::parse_sql("CREATE TABLE t1 (c1 INT) COMMENT 'table comment';");
    assert!(result.is_ok(), "Should parse COMMENT table option");

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_options.len(), 1);
            match &create.table_options[0] {
                vibesql_ast::TableOption::Comment(Some(value)) => {
                    assert_eq!(value, "table comment");
                }
                _ => panic!("Expected Comment option"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_sqllogictest_style() {
    // Test a statement similar to what's in the SQLLogicTest file
    let result = Parser::parse_sql("CREATE TABLE `t1710a` (`c1` MULTIPOLYGON COMMENT 'text155459', `c2` MULTIPOLYGON COMMENT 'text155461') KEY_BLOCK_SIZE 4.2;");
    assert!(result.is_ok(), "Should parse SQLLogicTest style CREATE TABLE with table options");

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "t1710a");
            assert_eq!(create.columns.len(), 2);
            assert_eq!(create.table_options.len(), 1);

            // Check KEY_BLOCK_SIZE option
            match &create.table_options[0] {
                vibesql_ast::TableOption::KeyBlockSize(value) => {
                    // Note: 4.2 should be parsed as Some(4) since we truncate floats
                    assert_eq!(*value, Some(4));
                }
                _ => panic!("Expected KeyBlockSize option"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_sqllogictest_with_insert_method() {
    // Test the exact statement from the SQLLogicTest suite that was failing
    let result = Parser::parse_sql(
        "CREATE TABLE `t17126` (`c1` MULTIPOLYGON COMMENT 'text156797', `c2` MULTIPOLYGON COMMENT 'text156799') KEY_BLOCK_SIZE 4.2 INSERT_METHOD LAST;"
    );
    assert!(result.is_ok(), "Should parse SQLLogicTest CREATE TABLE with INSERT_METHOD without =");

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "t17126");
            assert_eq!(create.columns.len(), 2);
            assert_eq!(create.table_options.len(), 2);

            // Check KEY_BLOCK_SIZE option
            match &create.table_options[0] {
                vibesql_ast::TableOption::KeyBlockSize(value) => {
                    assert_eq!(*value, Some(4));
                }
                _ => panic!("Expected KeyBlockSize option"),
            }

            // Check INSERT_METHOD option
            match &create.table_options[1] {
                vibesql_ast::TableOption::InsertMethod(method) => {
                    assert_eq!(method, &vibesql_ast::InsertMethod::Last);
                }
                _ => panic!("Expected InsertMethod option"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}
