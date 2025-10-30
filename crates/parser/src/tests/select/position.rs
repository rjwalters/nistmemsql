//! Parser tests for POSITION(substring IN string) syntax
//! SQL:1999 Section 6.29: String value functions

use super::super::*;

/// Test parsing: SELECT POSITION('lo' IN 'hello')
#[test]
fn test_parse_position_basic() {
    let result = Parser::parse_sql("SELECT POSITION('lo' IN 'hello');");
    if result.is_err() {
        eprintln!("Parse error: {:?}", result.as_ref().err());
    }
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.select_list.len(), 1);
            match &select_stmt.select_list[0] {
                ast::SelectItem::Expression { expr, .. } => match expr {
                    ast::Expression::Position { substring, string } => {
                        // Check substring
                        assert_eq!(
                            **substring,
                            ast::Expression::Literal(types::SqlValue::Varchar("lo".to_string()))
                        );
                        // Check string
                        assert_eq!(
                            **string,
                            ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string()))
                        );
                    }
                    _ => panic!("Expected Position expression, got {:?}", expr),
                },
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

/// Test parsing: SELECT POSITION('world' IN 'hello world')
#[test]
fn test_parse_position_substring_found() {
    let result = Parser::parse_sql("SELECT POSITION('world' IN 'hello world');");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.select_list.len(), 1);
            match &select_stmt.select_list[0] {
                ast::SelectItem::Expression { expr, .. } => match expr {
                    ast::Expression::Position { substring, string } => {
                        assert_eq!(
                            **substring,
                            ast::Expression::Literal(types::SqlValue::Varchar("world".to_string()))
                        );
                        assert_eq!(
                            **string,
                            ast::Expression::Literal(types::SqlValue::Varchar(
                                "hello world".to_string()
                            ))
                        );
                    }
                    _ => panic!("Expected Position expression"),
                },
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

/// Test parsing: SELECT POSITION('x' IN name) FROM users
/// Tests POSITION with column reference
#[test]
fn test_parse_position_with_column() {
    let result = Parser::parse_sql("SELECT POSITION('x' IN name) FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.select_list.len(), 1);
            match &select_stmt.select_list[0] {
                ast::SelectItem::Expression { expr, .. } => match expr {
                    ast::Expression::Position { substring, string } => {
                        assert_eq!(
                            **substring,
                            ast::Expression::Literal(types::SqlValue::Varchar("x".to_string()))
                        );
                        assert_eq!(
                            **string,
                            ast::Expression::ColumnRef { table: None, column: "NAME".to_string() }
                        );
                    }
                    _ => panic!("Expected Position expression"),
                },
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

/// Test parsing: SELECT POSITION(needle IN haystack) FROM test
/// Tests POSITION with two column references
#[test]
fn test_parse_position_both_columns() {
    let result = Parser::parse_sql("SELECT position(needle IN haystack) FROM test;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.select_list.len(), 1);
            match &select_stmt.select_list[0] {
                ast::SelectItem::Expression { expr, .. } => match expr {
                    ast::Expression::Position { substring, string } => {
                        assert_eq!(
                            **substring,
                            ast::Expression::ColumnRef {
                                table: None,
                                column: "NEEDLE".to_string()
                            }
                        );
                        assert_eq!(
                            **string,
                            ast::Expression::ColumnRef {
                                table: None,
                                column: "HAYSTACK".to_string()
                            }
                        );
                    }
                    _ => panic!("Expected Position expression"),
                },
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

/// Test parsing: SELECT POSITION('a' IN LOWER(name)) FROM users
/// Tests POSITION with function call as string argument
#[test]
fn test_parse_position_with_function() {
    let result = Parser::parse_sql("SELECT POSITION('a' IN LOWER(name)) FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.select_list.len(), 1);
            match &select_stmt.select_list[0] {
                ast::SelectItem::Expression { expr, .. } => match expr {
                    ast::Expression::Position { substring, string } => {
                        assert_eq!(
                            **substring,
                            ast::Expression::Literal(types::SqlValue::Varchar("a".to_string()))
                        );
                        // Check that string is a Function expression
                        match &**string {
                            ast::Expression::Function { name, args } => {
                                assert_eq!(name, "LOWER");
                                assert_eq!(args.len(), 1);
                            }
                            _ => panic!("Expected Function expression in string"),
                        }
                    }
                    _ => panic!("Expected Position expression"),
                },
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}
