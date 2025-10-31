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

// ========================================================================
// CHAR VARING Tests (Deprecated SQL:1999 Syntax)
// ========================================================================

#[test]
fn test_parse_char_varing_without_length() {
    // CHAR VARING is a deprecated SQL:1999 variant of CHARACTER VARYING
    let result = Parser::parse_sql("CREATE TABLE t (x CHAR VARING);");
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
fn test_parse_char_varing_with_length() {
    // CHAR VARING(n) should be treated like VARCHAR(n)
    let result = Parser::parse_sql("CREATE TABLE t (x CHAR VARING(50));");
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
fn test_parse_char_varing_equivalence() {
    // CHAR VARING should be identical to VARCHAR
    let varing_result = Parser::parse_sql("CREATE TABLE t1 (x CHAR VARING(100));");
    let varchar_result = Parser::parse_sql("CREATE TABLE t2 (x VARCHAR(100));");

    assert!(varing_result.is_ok());
    assert!(varchar_result.is_ok());

    let varing_stmt = varing_result.unwrap();
    let varchar_stmt = varchar_result.unwrap();

    match (varing_stmt, varchar_stmt) {
        (
            ast::Statement::CreateTable(varing_create),
            ast::Statement::CreateTable(varchar_create),
        ) => {
            // Both should produce the same data type
            assert_eq!(varing_create.columns[0].data_type, varchar_create.columns[0].data_type);
        }
        _ => panic!("Expected CREATE TABLE statements"),
    }
}
