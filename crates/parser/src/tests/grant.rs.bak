//! Tests for GRANT statement parsing

use crate::Parser;

#[test]
fn test_parse_grant_select_on_table() {
    let sql = "GRANT SELECT ON TABLE users TO manager";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(grant_stmt.privileges[0], ast::PrivilegeType::Select);
            assert_eq!(grant_stmt.object_type, ast::ObjectType::Table);
            assert_eq!(grant_stmt.object_name.to_string(), "users");
            assert_eq!(grant_stmt.grantees, vec!["manager"]);
            assert!(!grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_case_insensitive() {
    let sql = "grant select on table employees to clerk";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.object_name.to_string(), "employees");
            assert_eq!(grant_stmt.grantees, vec!["clerk"]);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_qualified_table_name() {
    let sql = "GRANT SELECT ON TABLE public.users TO manager";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        ast::Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.object_name.to_string(), "public.users");
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}
