use super::super::*;

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
            assert_eq!(create.table_name, "USERS");
            assert_eq!(create.columns.len(), 2);
            assert_eq!(create.columns[0].name, "ID");
            assert_eq!(create.columns[1].name, "NAME");
            match create.columns[0].data_type {
                types::DataType::Integer => {} // Success
                _ => panic!("Expected Integer data type"),
            }
            match create.columns[1].data_type {
                types::DataType::Varchar { max_length: Some(100) } => {} // Success
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
            assert_eq!(create.table_name, "TEST");
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

#[test]
fn test_parse_create_table_without_oids() {
    let result = Parser::parse_sql("CREATE TABLE t1 (id INT) WITHOUT OIDS;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T1");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "ID");
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_oids() {
    let result = Parser::parse_sql("CREATE TABLE t2 (id INT) WITH OIDS;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T2");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "ID");
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_no_oids_clause() {
    // Ensure tables without OIDS clause still work
    let result = Parser::parse_sql("CREATE TABLE t3 (id INT);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T3");
            assert_eq!(create.columns.len(), 1);
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

// ========================================================================
// Backtick Identifier Tests (MySQL-style)
// ========================================================================

#[test]
fn test_parse_create_table_with_backtick_table_name() {
    let result = Parser::parse_sql("CREATE TABLE `user_table` (id INTEGER);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            // Backtick identifiers preserve case
            assert_eq!(create.table_name, "user_table");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "ID");
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_backtick_column_names() {
    let result = Parser::parse_sql("CREATE TABLE users (`user_id` INTEGER, `user_name` VARCHAR(100));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "USERS");
            assert_eq!(create.columns.len(), 2);
            // Backtick identifiers preserve case
            assert_eq!(create.columns[0].name, "user_id");
            assert_eq!(create.columns[1].name, "user_name");
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_backtick_reserved_word() {
    // Reserved words can be used as identifiers when backtick-quoted
    let result = Parser::parse_sql("CREATE TABLE `select` (`from` INTEGER, `where` VARCHAR(50));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "select");
            assert_eq!(create.columns.len(), 2);
            assert_eq!(create.columns[0].name, "from");
            assert_eq!(create.columns[1].name, "where");
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_backtick_spaces() {
    // Backtick identifiers can contain spaces
    let result = Parser::parse_sql("CREATE TABLE `my table` (`first name` INTEGER, `last name` VARCHAR(100));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "my table");
            assert_eq!(create.columns.len(), 2);
            assert_eq!(create.columns[0].name, "first name");
            assert_eq!(create.columns[1].name, "last name");
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_mixed_backtick_and_regular() {
    // Mix backtick and regular identifiers
    let result = Parser::parse_sql("CREATE TABLE `MyTable` (id INTEGER, `userName` VARCHAR(100), status INT);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "MyTable");
            assert_eq!(create.columns.len(), 3);
            assert_eq!(create.columns[0].name, "ID"); // Regular identifier - uppercased
            assert_eq!(create.columns[1].name, "userName"); // Backtick - preserves case
            assert_eq!(create.columns[2].name, "STATUS"); // Regular identifier - uppercased
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}
