//! Tests for GRANT statement parsing edge cases and implicit defaults
//!
//! This module contains tests for:
//! - Case-insensitive keyword parsing
//! - Qualified object names (schema.table)
//! - Multiple grantees
//! - Complex privilege/grantee combinations
//! - Implicit object type defaults (Issue #566)

use vibesql_ast::*;

use crate::Parser;

#[test]
fn test_parse_grant_case_insensitive() {
    let sql = "grant select on table employees to clerk";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.object_name.to_string(), "EMPLOYEES");
            assert_eq!(grant_stmt.grantees, vec!["CLERK"]);
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
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.object_name.to_string(), "PUBLIC.USERS");
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_multiple_grantees() {
    let sql = "GRANT SELECT ON TABLE users TO role1, role2, role3";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::Select(None));
            assert_eq!(grant_stmt.grantees.len(), 3);
            assert_eq!(grant_stmt.grantees, vec!["ROLE1", "ROLE2", "ROLE3"]);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_multiple_privileges_and_grantees() {
    let sql = "GRANT SELECT, INSERT ON TABLE users TO r1, r2";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 2);
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::Select(None));
            assert_eq!(grant_stmt.privileges[1], PrivilegeType::Insert(None));
            assert_eq!(grant_stmt.grantees.len(), 2);
            assert_eq!(grant_stmt.grantees, vec!["R1", "R2"]);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_usage_implicit_schema() {
    // When USAGE privilege is specified without object type keyword, it should default to Schema
    let sql = "GRANT USAGE ON my_schema TO user_role";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges.len(), 1);
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::Usage);
            assert_eq!(grant_stmt.object_type, ObjectType::Schema);
            assert_eq!(grant_stmt.object_name.to_string(), "MY_SCHEMA");
            assert_eq!(grant_stmt.grantees, vec!["USER_ROLE"]);
            assert!(!grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_select_implicit_table() {
    // Other privileges (non-USAGE) should still default to Table when object type not specified
    let sql = "GRANT SELECT ON users TO manager";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::Select(None));
            assert_eq!(grant_stmt.object_type, ObjectType::Table);
            assert_eq!(grant_stmt.object_name.to_string(), "USERS");
            assert_eq!(grant_stmt.grantees, vec!["MANAGER"]);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_usage_implicit_schema_with_grant_option() {
    // USAGE with implicit object type + WITH GRANT OPTION
    let sql = "GRANT USAGE ON test_schema TO admin WITH GRANT OPTION";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::Usage);
            assert_eq!(grant_stmt.object_type, ObjectType::Schema);
            assert_eq!(grant_stmt.object_name.to_string(), "TEST_SCHEMA");
            assert_eq!(grant_stmt.grantees, vec!["ADMIN"]);
            assert!(grant_stmt.with_grant_option);
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}

#[test]
fn test_parse_grant_insert_implicit_table() {
    // INSERT privilege should default to Table (existing behavior preserved)
    let sql = "GRANT INSERT ON orders TO clerk";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Grant(grant_stmt) => {
            assert_eq!(grant_stmt.privileges[0], PrivilegeType::Insert(None));
            assert_eq!(grant_stmt.object_type, ObjectType::Table);
            assert_eq!(grant_stmt.object_name.to_string(), "ORDERS");
        }
        other => panic!("Expected Grant statement, got {:?}", other),
    }
}
