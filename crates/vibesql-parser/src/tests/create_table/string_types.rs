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
// NVARCHAR Tests (SQL Server/MySQL alias for NCHAR VARYING)
// ========================================================================

#[test]
fn test_parse_nvarchar_with_length() {
    let result = Parser::parse_sql("CREATE TABLE t (x NVARCHAR(13));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Varchar { max_length: Some(13) } => {} // Success
                _ => panic!("Expected VARCHAR(13) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_nvarchar_with_space_before_paren() {
    // NVARCHAR (13) with space before parenthesis
    let result = Parser::parse_sql("CREATE TABLE t (x NVARCHAR (13));");
    assert!(result.is_ok(), "Failed to parse NVARCHAR with space: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Varchar { max_length: Some(13) } => {} // Success
                _ => panic!("Expected VARCHAR(13) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_nvarchar_without_length() {
    let result = Parser::parse_sql("CREATE TABLE t (x NVARCHAR);");
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
fn test_parse_nvarchar_with_constraint() {
    // This is the actual failing test case from SQLLogicTest: NVARCHAR with UNIQUE
    let result = Parser::parse_sql("CREATE TABLE `t21291` (`c1` NVARCHAR (13) UNIQUE, `c2` NVARCHAR (11) KEY);");
    assert!(result.is_ok(), "Failed to parse NVARCHAR with constraints: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            // Backtick-quoted identifiers preserve case (lowercase in this case)
            assert_eq!(create.table_name, "t21291");
            assert_eq!(create.columns.len(), 2);

            // Check first column (backtick-quoted, preserves case)
            assert_eq!(create.columns[0].name, "c1");
            match create.columns[0].data_type {
                vibesql_types::DataType::Varchar { max_length: Some(13) } => {} // Success
                _ => panic!("Expected VARCHAR(13) for c1"),
            }
            // Verify UNIQUE constraint
            assert!(create.columns[0].constraints.iter().any(|c| matches!(
                &c.kind,
                vibesql_ast::ColumnConstraintKind::Unique
            )));

            // Check second column (backtick-quoted, preserves case)
            assert_eq!(create.columns[1].name, "c2");
            match create.columns[1].data_type {
                vibesql_types::DataType::Varchar { max_length: Some(11) } => {} // Success
                _ => panic!("Expected VARCHAR(11) for c2"),
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
fn test_parse_nvarchar_equivalence() {
    // NVARCHAR should be identical to NCHAR VARYING
    let nvarchar_result = Parser::parse_sql("CREATE TABLE t1 (x NVARCHAR(50));");
    let nchar_varying_result = Parser::parse_sql("CREATE TABLE t2 (x NCHAR VARYING(50));");

    assert!(nvarchar_result.is_ok());
    assert!(nchar_varying_result.is_ok());

    let nvarchar_stmt = nvarchar_result.unwrap();
    let nchar_varying_stmt = nchar_varying_result.unwrap();

    match (nvarchar_stmt, nchar_varying_stmt) {
        (
            vibesql_ast::Statement::CreateTable(nvarchar_create),
            vibesql_ast::Statement::CreateTable(nchar_varying_create),
        ) => {
            // Both should produce the same data type
            assert_eq!(nvarchar_create.columns[0].data_type, nchar_varying_create.columns[0].data_type);
        }
        _ => panic!("Expected CREATE TABLE statements"),
    }
}

#[test]
fn test_parse_nvarchar_with_characters_modifier() {
    let result = Parser::parse_sql("CREATE TABLE t (x NVARCHAR(25 CHARACTERS));");
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
fn test_parse_nvarchar_with_octets_modifier() {
    let result = Parser::parse_sql("CREATE TABLE t (x NVARCHAR(30 OCTETS));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].name, "X");
            match create.columns[0].data_type {
                vibesql_types::DataType::Varchar { max_length: Some(30) } => {} // Success
                _ => panic!("Expected VARCHAR(30) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}
