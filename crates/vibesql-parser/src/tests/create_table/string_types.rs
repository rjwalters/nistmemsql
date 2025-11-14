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
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Varchar { max_length: Some(50) } => {} // Success
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
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Varchar { max_length: None } => {} // Success
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
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Character { length: 10 } => {} // Success
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
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Character { length: 10 } => {} // Success
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
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Varchar { max_length: Some(20) } => {} // Success
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
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Varchar { max_length: Some(20) } => {} // Success
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
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Varchar { max_length: Some(30) } => {} // Success
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
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Varchar { max_length: Some(30) } => {} // Success
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
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Character { length: 10 } => {} // Success
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
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "A");
            match create.columns[0].data_type {
                vibesql_types::DataType::Character { length: 1 } => {} // Success - defaults to 1
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
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "A");
            match create.columns[0].data_type {
                vibesql_types::DataType::Character { length: 1 } => {} // Success - defaults to 1
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
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Varchar { max_length: None } => {} // Success
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
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Varchar { max_length: Some(50) } => {} // Success
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
            vibesql_ast::Statement::CreateTable(varing_create),
            vibesql_ast::Statement::CreateTable(varchar_create),
        ) => {
            // Both should produce the same data type
            assert_eq!(varing_create.columns[0].data_type, varchar_create.columns[0].data_type);
        }
        _ => panic!("Expected CREATE TABLE statements"),
    }
}

// ========================================================================
// BINARY and VARBINARY Type Tests (MySQL compatibility)
// ========================================================================

#[test]
fn test_parse_varbinary_with_size() {
    // VARBINARY(n) without space before parenthesis
    let result = Parser::parse_sql("CREATE TABLE t (x VARBINARY(4));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match &create.columns[0].data_type {
                vibesql_types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "VARBINARY");
                }
                _ => panic!("Expected VARBINARY user-defined data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_varbinary_with_size_and_space() {
    // VARBINARY (4) with space before parenthesis - this is the main issue #1662
    let result = Parser::parse_sql("CREATE TABLE t (x VARBINARY (4));");
    assert!(result.is_ok(), "Failed to parse VARBINARY with space: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match &create.columns[0].data_type {
                vibesql_types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "VARBINARY");
                }
                _ => panic!("Expected VARBINARY user-defined data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_varbinary_without_size() {
    // VARBINARY without size specification
    let result = Parser::parse_sql("CREATE TABLE t (x VARBINARY);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match &create.columns[0].data_type {
                vibesql_types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "VARBINARY");
                }
                _ => panic!("Expected VARBINARY user-defined data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_binary_with_size() {
    // BINARY(n) without space before parenthesis
    let result = Parser::parse_sql("CREATE TABLE t (x BINARY(8));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match &create.columns[0].data_type {
                vibesql_types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "BINARY");
                }
                _ => panic!("Expected BINARY user-defined data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_binary_with_size_and_space() {
    // BINARY (8) with space before parenthesis
    let result = Parser::parse_sql("CREATE TABLE t (x BINARY (8));");
    assert!(result.is_ok(), "Failed to parse BINARY with space: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match &create.columns[0].data_type {
                vibesql_types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "BINARY");
                }
                _ => panic!("Expected BINARY user-defined data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_binary_without_size() {
    // BINARY without size specification
    let result = Parser::parse_sql("CREATE TABLE t (x BINARY);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match &create.columns[0].data_type {
                vibesql_types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "BINARY");
                }
                _ => panic!("Expected BINARY user-defined data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_varbinary_with_key_constraint() {
    // From the failing test case: VARBINARY (4) KEY
    let result = Parser::parse_sql("CREATE TABLE t (c1 VARBINARY (4) KEY);");
    assert!(result.is_ok(), "Failed to parse VARBINARY with KEY: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "C1");
            match &create.columns[0].data_type {
                vibesql_types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, "VARBINARY");
                }
                _ => panic!("Expected VARBINARY user-defined data type"),
            }
            // Also verify the KEY constraint was parsed
            assert!(create.columns[0].constraints.iter().any(|c| matches!(
                &c.kind,
                vibesql_ast::ColumnConstraintKind::Key
            )));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

// ========================================================================
// NCHAR and NCHAR VARYING Tests
// ========================================================================

#[test]
fn test_parse_nchar_varying_with_length() {
    let result = Parser::parse_sql("CREATE TABLE t (x NCHAR VARYING (50));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Varchar { max_length: Some(50) } => {} // Success
                _ => panic!("Expected VARCHAR(50) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_nchar_varying_without_length() {
    let result = Parser::parse_sql("CREATE TABLE t (x NCHAR VARYING);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Varchar { max_length: None } => {} // Success
                _ => panic!("Expected VARCHAR data type without length"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_nchar_with_length() {
    let result = Parser::parse_sql("CREATE TABLE t (x NCHAR(20));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Character { length: 20 } => {} // Success
                _ => panic!("Expected CHAR(20) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_nchar_without_length() {
    let result = Parser::parse_sql("CREATE TABLE t (x NCHAR);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Character { length: 1 } => {} // Success - default is 1
                _ => panic!("Expected CHAR(1) data type (default)"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_nchar_varying_with_constraint() {
    // This is the actual failing test case from the SQLLogicTest suite
    let result = Parser::parse_sql(
        "CREATE TABLE `t21006` (`c1` NCHAR VARYING (15) COMMENT 'text1098849', c2 NCHAR VARYING (42) KEY);"
    );
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            // Backtick-quoted identifiers preserve case (lowercase in this case)
            assert_eq!(create.table_name, "t21006");
            assert_eq!(create.columns.len(), 2);

            // Check first column (backtick-quoted, preserves case)
            assert_eq!(create.columns[0].name, "c1");
            match create.columns[0].data_type {
                vibesql_types::DataType::Varchar { max_length: Some(15) } => {} // Success
                _ => panic!("Expected VARCHAR(15) for c1"),
            }
            // Note: COMMENT is parsed but not currently stored in the AST

            // Check second column (unquoted, normalized to uppercase)
            assert_eq!(create.columns[1].name, "C2");
            match create.columns[1].data_type {
                vibesql_types::DataType::Varchar { max_length: Some(42) } => {} // Success
                _ => panic!("Expected VARCHAR(42) for c2"),
            }
            // Verify KEY constraint
            assert!(create.columns[1].constraints.iter().any(|c| matches!(
                &c.kind,
                vibesql_ast::ColumnConstraintKind::Key
            )));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_nchar_with_characters_modifier() {
    let result = Parser::parse_sql("CREATE TABLE t (x NCHAR(10 CHARACTERS));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Character { length: 10 } => {} // Success
                _ => panic!("Expected CHAR(10) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_nchar_varying_with_octets_modifier() {
    let result = Parser::parse_sql("CREATE TABLE t (x NCHAR VARYING(25 OCTETS));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Varchar { max_length: Some(25) } => {} // Success
                _ => panic!("Expected VARCHAR(25) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

// ========================================================================
// NATIONAL VARCHAR and NATIONAL CHARACTER Tests
// ========================================================================

#[test]
fn test_parse_national_varchar_with_length() {
    let result = Parser::parse_sql("CREATE TABLE t (x NATIONAL VARCHAR(20));");
    assert!(result.is_ok(), "Failed to parse NATIONAL VARCHAR: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Varchar { max_length: Some(20) } => {} // Success
                _ => panic!("Expected VARCHAR(20) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_national_varchar_with_space_before_paren() {
    // NATIONAL VARCHAR (4) with space - this is the actual failing test case
    let result = Parser::parse_sql("CREATE TABLE t (x NATIONAL VARCHAR (4));");
    assert!(result.is_ok(), "Failed to parse NATIONAL VARCHAR with space: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Varchar { max_length: Some(4) } => {} // Success
                _ => panic!("Expected VARCHAR(4) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_national_varchar_without_length() {
    let result = Parser::parse_sql("CREATE TABLE t (x NATIONAL VARCHAR);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Varchar { max_length: None } => {} // Success
                _ => panic!("Expected VARCHAR data type without length"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_national_character_with_length() {
    let result = Parser::parse_sql("CREATE TABLE t (x NATIONAL CHARACTER(15));");
    assert!(result.is_ok(), "Failed to parse NATIONAL CHARACTER: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Character { length: 15 } => {} // Success
                _ => panic!("Expected CHAR(15) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_national_character_without_length() {
    let result = Parser::parse_sql("CREATE TABLE t (x NATIONAL CHARACTER);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Character { length: 1 } => {} // Success - default is 1
                _ => panic!("Expected CHAR(1) data type (default)"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_national_char_with_length() {
    let result = Parser::parse_sql("CREATE TABLE t (x NATIONAL CHAR(10));");
    assert!(result.is_ok(), "Failed to parse NATIONAL CHAR: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Character { length: 10 } => {} // Success
                _ => panic!("Expected CHAR(10) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_national_varchar_with_constraint() {
    // This matches the actual failing test case from SQLLogicTest
    let result = Parser::parse_sql(
        "CREATE TABLE `t21659` (`c1` NATIONAL VARCHAR (4) UNIQUE, c2 NATIONAL VARCHAR (19) UNIQUE);"
    );
    assert!(result.is_ok(), "Failed to parse NATIONAL VARCHAR with constraints: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            // Backtick-quoted identifiers preserve case (lowercase in this case)
            assert_eq!(create.table_name, "t21659");
            assert_eq!(create.columns.len(), 2);

            // Check first column (backtick-quoted, preserves case)
            assert_eq!(create.columns[0].name, "c1");
            match create.columns[0].data_type {
                vibesql_types::DataType::Varchar { max_length: Some(4) } => {} // Success
                _ => panic!("Expected VARCHAR(4) for c1"),
            }
            // Verify UNIQUE constraint
            assert!(create.columns[0].constraints.iter().any(|c| matches!(
                &c.kind,
                vibesql_ast::ColumnConstraintKind::Unique
            )));

            // Check second column (unquoted, normalized to uppercase)
            assert_eq!(create.columns[1].name, "C2");
            match create.columns[1].data_type {
                vibesql_types::DataType::Varchar { max_length: Some(19) } => {} // Success
                _ => panic!("Expected VARCHAR(19) for c2"),
            }
            // Verify UNIQUE constraint
            assert!(create.columns[1].constraints.iter().any(|c| matches!(
                &c.kind,
                vibesql_ast::ColumnConstraintKind::Unique
            )));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_national_varchar_equivalence() {
    // NATIONAL VARCHAR should be identical to NVARCHAR
    let national_result = Parser::parse_sql("CREATE TABLE t1 (x NATIONAL VARCHAR(30));");
    let nvarchar_result = Parser::parse_sql("CREATE TABLE t2 (x NVARCHAR(30));");

    assert!(national_result.is_ok());
    assert!(nvarchar_result.is_ok());

    let national_stmt = national_result.unwrap();
    let nvarchar_stmt = nvarchar_result.unwrap();

    match (national_stmt, nvarchar_stmt) {
        (
            vibesql_ast::Statement::CreateTable(national_create),
            vibesql_ast::Statement::CreateTable(nvarchar_create),
        ) => {
            // Both should produce the same data type
            assert_eq!(national_create.columns[0].data_type, nvarchar_create.columns[0].data_type);
        }
        _ => panic!("Expected CREATE TABLE statements"),
    }
}

#[test]
fn test_parse_national_character_equivalence() {
    // NATIONAL CHARACTER should be identical to NCHAR
    let national_result = Parser::parse_sql("CREATE TABLE t1 (x NATIONAL CHARACTER(10));");
    let nchar_result = Parser::parse_sql("CREATE TABLE t2 (x NCHAR(10));");

    assert!(national_result.is_ok());
    assert!(nchar_result.is_ok());

    let national_stmt = national_result.unwrap();
    let nchar_stmt = nchar_result.unwrap();

    match (national_stmt, nchar_stmt) {
        (
            vibesql_ast::Statement::CreateTable(national_create),
            vibesql_ast::Statement::CreateTable(nchar_create),
        ) => {
            // Both should produce the same data type
            assert_eq!(national_create.columns[0].data_type, nchar_create.columns[0].data_type);
        }
        _ => panic!("Expected CREATE TABLE statements"),
    }
}

#[test]
fn test_parse_national_varchar_with_characters_modifier() {
    let result = Parser::parse_sql("CREATE TABLE t (x NATIONAL VARCHAR(25 CHARACTERS));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Varchar { max_length: Some(25) } => {} // Success
                _ => panic!("Expected VARCHAR(25) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_national_character_with_octets_modifier() {
    let result = Parser::parse_sql("CREATE TABLE t (x NATIONAL CHARACTER(12 OCTETS));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Character { length: 12 } => {} // Success
                _ => panic!("Expected CHAR(12) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}
