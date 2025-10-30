use super::super::*;

// ========================================================================
// String Type Tests with Modifiers
// ========================================================================

#[test]
fn test_parse_character_varying_with_length() {
    let result = Parser::parse_sql("CREATE TABLE t (x CHARACTER VARYING(50));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                types::DataType::Varchar { max_length: Some(50) } => {} // Success
                _ => panic!("Expected VARCHAR(50) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_character_varying_without_length() {
    let result = Parser::parse_sql("CREATE TABLE t (x CHARACTER VARYING);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                types::DataType::Varchar { max_length: None } => {} // Success
                _ => panic!("Expected VARCHAR data type without length"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_char_with_characters_modifier() {
    let result = Parser::parse_sql("CREATE TABLE t (x CHAR(10 CHARACTERS));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                types::DataType::Character { length: 10 } => {} // Success
                _ => panic!("Expected CHAR(10) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_char_with_octets_modifier() {
    let result = Parser::parse_sql("CREATE TABLE t (x CHAR(10 OCTETS));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                types::DataType::Character { length: 10 } => {} // Success
                _ => panic!("Expected CHAR(10) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_varchar_with_characters_modifier() {
    let result = Parser::parse_sql("CREATE TABLE t (x VARCHAR(20 CHARACTERS));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                types::DataType::Varchar { max_length: Some(20) } => {} // Success
                _ => panic!("Expected VARCHAR(20) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_varchar_with_octets_modifier() {
    let result = Parser::parse_sql("CREATE TABLE t (x VARCHAR(20 OCTETS));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                types::DataType::Varchar { max_length: Some(20) } => {} // Success
                _ => panic!("Expected VARCHAR(20) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_character_varying_with_characters_modifier() {
    let result = Parser::parse_sql("CREATE TABLE t (x CHARACTER VARYING(30 CHARACTERS));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                types::DataType::Varchar { max_length: Some(30) } => {} // Success
                _ => panic!("Expected VARCHAR(30) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_character_varying_with_octets_modifier() {
    let result = Parser::parse_sql("CREATE TABLE t (x CHARACTER VARYING(30 OCTETS));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                types::DataType::Varchar { max_length: Some(30) } => {} // Success
                _ => panic!("Expected VARCHAR(30) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_char_without_modifier_still_works() {
    let result = Parser::parse_sql("CREATE TABLE t (x CHAR(10));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                types::DataType::Character { length: 10 } => {} // Success
                _ => panic!("Expected CHAR(10) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_char_without_length() {
    let result = Parser::parse_sql("CREATE TABLE t (A CHAR);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "A");
            match create.columns[0].data_type {
                types::DataType::Character { length: 1 } => {} // Success - defaults to 1
                _ => panic!("Expected CHAR(1) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_character_without_length() {
    let result = Parser::parse_sql("CREATE TABLE t (A CHARACTER);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "A");
            match create.columns[0].data_type {
                types::DataType::Character { length: 1 } => {} // Success - defaults to 1
                _ => panic!("Expected CHARACTER(1) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}
