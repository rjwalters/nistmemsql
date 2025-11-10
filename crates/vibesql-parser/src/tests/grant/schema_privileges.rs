//! Tests for GRANT statement with schema privileges
//!
//! Covers SQL:1999 Core Feature E081-03: Schema privileges (USAGE, CREATE).

use vibesql_ast::*;

use crate::Parser;

#[test]
fn test_parse_grant_usage_on_schema() {
    let sql = "GRANT USAGE ON SCHEMA public TO user_role";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::Usage);
            assert_eq!(grant_stmt.object_type, ObjectType::Schema);
            assert_eq!(grant_stmt.object_name.to_string(), "PUBLIC");
            assert_eq!(grant_stmt.grantees, vec!["USER_ROLE"]);
            assert!(!grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_create_on_schema() {
    let sql = "GRANT CREATE ON SCHEMA public TO admin_role";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::Create);
            assert_eq!(grant_stmt.object_type, ObjectType::Schema);
            assert_eq!(grant_stmt.object_name.to_string(), "PUBLIC");
            assert_eq!(grant_stmt.grantees, vec!["ADMIN_ROLE"]);
            assert!(!grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_usage_and_create_on_schema() {
    let sql = "GRANT USAGE, CREATE ON SCHEMA myschema TO developer";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 2);
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::Usage);
            assert_eq!(grant_stmt.privileges[1], PrivilegeType::Create);
            assert_eq!(grant_stmt.object_type, ObjectType::Schema);
            assert_eq!(grant_stmt.object_name.to_string(), "MYSCHEMA");
            assert_eq!(grant_stmt.grantees, vec!["DEVELOPER"]);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_all_on_schema() {
    let sql = "GRANT ALL PRIVILEGES ON SCHEMA public TO admin_role";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::AllPrivileges);
            assert_eq!(grant_stmt.object_type, ObjectType::Schema);
            assert_eq!(grant_stmt.object_name.to_string(), "PUBLIC");
            assert_eq!(grant_stmt.grantees, vec!["ADMIN_ROLE"]);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}
