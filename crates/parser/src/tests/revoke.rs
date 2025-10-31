//! Tests for REVOKE statement parsing

use crate::Parser;
use ast::*;

/// Helper function to parse a REVOKE statement
fn parse_revoke(sql: &str) -> RevokeStmt {
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::Revoke(stmt) => stmt,
        other => panic!("Expected Revoke statement, got {:?}", other),
    }
}

#[test]
fn test_revoke_basic_select() {
    let sql = "REVOKE SELECT ON TABLE users FROM manager";
    let stmt = parse_revoke(sql);

    assert!(!stmt.grant_option_for);
    assert_eq!(stmt.privileges, vec![PrivilegeType::Select]);
    assert_eq!(stmt.object_type, ObjectType::Table);
    assert_eq!(stmt.object_name, "USERS");
    assert_eq!(stmt.grantees, vec!["MANAGER"]);
    assert_eq!(stmt.granted_by, None);
    assert_eq!(stmt.cascade_option, CascadeOption::None);
}

#[test]
fn test_revoke_multiple_privileges() {
    let sql = "REVOKE SELECT, INSERT, UPDATE ON TABLE employees FROM clerk";
    let stmt = parse_revoke(sql);

    assert_eq!(stmt.privileges.len(), 3);
    assert!(stmt.privileges.contains(&PrivilegeType::Select));
    assert!(stmt.privileges.contains(&PrivilegeType::Insert));
    assert!(stmt.privileges.contains(&PrivilegeType::Update));
    assert_eq!(stmt.grantees, vec!["CLERK"]);
}

#[test]
fn test_revoke_multiple_grantees() {
    let sql = "REVOKE SELECT ON TABLE data FROM user1, user2, user3";
    let stmt = parse_revoke(sql);

    assert_eq!(stmt.privileges, vec![PrivilegeType::Select]);
    assert_eq!(stmt.grantees, vec!["USER1", "USER2", "USER3"]);
}

#[test]
fn test_revoke_all_privileges() {
    let sql = "REVOKE ALL PRIVILEGES ON TABLE products FROM admin";
    let stmt = parse_revoke(sql);

    assert_eq!(stmt.privileges, vec![PrivilegeType::AllPrivileges]);
    assert_eq!(stmt.grantees, vec!["ADMIN"]);
}

#[test]
fn test_revoke_all_without_privileges_keyword() {
    let sql = "REVOKE ALL ON TABLE items FROM role1";
    let stmt = parse_revoke(sql);

    assert_eq!(stmt.privileges, vec![PrivilegeType::AllPrivileges]);
    assert_eq!(stmt.grantees, vec!["ROLE1"]);
}

#[test]
fn test_revoke_with_cascade() {
    let sql = "REVOKE SELECT ON TABLE accounts FROM manager CASCADE";
    let stmt = parse_revoke(sql);

    assert_eq!(stmt.privileges, vec![PrivilegeType::Select]);
    assert_eq!(stmt.grantees, vec!["MANAGER"]);
    assert_eq!(stmt.cascade_option, CascadeOption::Cascade);
}

#[test]
fn test_revoke_with_restrict() {
    let sql = "REVOKE INSERT ON TABLE orders FROM clerk RESTRICT";
    let stmt = parse_revoke(sql);

    assert_eq!(stmt.privileges, vec![PrivilegeType::Insert]);
    assert_eq!(stmt.grantees, vec!["CLERK"]);
    assert_eq!(stmt.cascade_option, CascadeOption::Restrict);
}

#[test]
fn test_revoke_grant_option_for() {
    let sql = "REVOKE GRANT OPTION FOR SELECT ON TABLE reports FROM manager";
    let stmt = parse_revoke(sql);

    assert!(stmt.grant_option_for);
    assert_eq!(stmt.privileges, vec![PrivilegeType::Select]);
    assert_eq!(stmt.grantees, vec!["MANAGER"]);
}

#[test]
fn test_revoke_granted_by() {
    let sql = "REVOKE SELECT ON TABLE data FROM analyst GRANTED BY admin";
    let stmt = parse_revoke(sql);

    assert_eq!(stmt.privileges, vec![PrivilegeType::Select]);
    assert_eq!(stmt.grantees, vec!["ANALYST"]);
    assert_eq!(stmt.granted_by, Some("ADMIN".to_string()));
}

#[test]
fn test_revoke_schema_privilege() {
    let sql = "REVOKE USAGE ON SCHEMA public FROM user1";
    let stmt = parse_revoke(sql);

    assert_eq!(stmt.privileges, vec![PrivilegeType::Usage]);
    assert_eq!(stmt.object_type, ObjectType::Schema);
    assert_eq!(stmt.object_name, "PUBLIC");
    assert_eq!(stmt.grantees, vec!["USER1"]);
}

#[test]
fn test_revoke_create_on_schema() {
    let sql = "REVOKE CREATE ON SCHEMA test_schema FROM developer";
    let stmt = parse_revoke(sql);

    assert_eq!(stmt.privileges, vec![PrivilegeType::Create]);
    assert_eq!(stmt.object_type, ObjectType::Schema);
    assert_eq!(stmt.object_name, "TEST_SCHEMA");
    assert_eq!(stmt.grantees, vec!["DEVELOPER"]);
}

#[test]
fn test_revoke_without_table_keyword() {
    let sql = "REVOKE SELECT ON users FROM manager";
    let stmt = parse_revoke(sql);

    // Should default to TABLE when not specified
    assert_eq!(stmt.object_type, ObjectType::Table);
    assert_eq!(stmt.object_name, "USERS");
}

#[test]
fn test_revoke_complex_combination() {
    let sql = "REVOKE GRANT OPTION FOR SELECT, INSERT ON TABLE sensitive_data FROM role1, role2 GRANTED BY admin CASCADE";
    let stmt = parse_revoke(sql);

    assert!(stmt.grant_option_for);
    assert_eq!(stmt.privileges.len(), 2);
    assert!(stmt.privileges.contains(&PrivilegeType::Select));
    assert!(stmt.privileges.contains(&PrivilegeType::Insert));
    assert_eq!(stmt.object_type, ObjectType::Table);
    assert_eq!(stmt.object_name, "SENSITIVE_DATA");
    assert_eq!(stmt.grantees, vec!["ROLE1", "ROLE2"]);
    assert_eq!(stmt.granted_by, Some("ADMIN".to_string()));
    assert_eq!(stmt.cascade_option, CascadeOption::Cascade);
}

#[test]
fn test_revoke_delete_privilege() {
    let sql = "REVOKE DELETE ON TABLE logs FROM auditor";
    let stmt = parse_revoke(sql);

    assert_eq!(stmt.privileges, vec![PrivilegeType::Delete]);
    assert_eq!(stmt.grantees, vec!["AUDITOR"]);
}

#[test]
fn test_revoke_references_privilege() {
    let sql = "REVOKE REFERENCES ON TABLE parent_table FROM child_schema";
    let stmt = parse_revoke(sql);

    assert_eq!(stmt.privileges, vec![PrivilegeType::References]);
    assert_eq!(stmt.object_name, "PARENT_TABLE");
}

// SQL:1999 Core Feature E081-06: REFERENCES privilege tests

#[test]
fn test_revoke_references_basic() {
    let sql = "REVOKE REFERENCES ON TABLE users FROM manager";
    let stmt = parse_revoke(sql);

    assert_eq!(stmt.privileges, vec![PrivilegeType::References]);
    assert_eq!(stmt.object_type, ObjectType::Table);
    assert_eq!(stmt.object_name, "USERS");
    assert_eq!(stmt.grantees, vec!["MANAGER"]);
}

#[test]
fn test_revoke_references_with_cascade() {
    let sql = "REVOKE REFERENCES ON TABLE orders FROM manager CASCADE";
    let stmt = parse_revoke(sql);

    assert_eq!(stmt.privileges, vec![PrivilegeType::References]);
    assert_eq!(stmt.object_name, "ORDERS");
    assert_eq!(stmt.grantees, vec!["MANAGER"]);
    assert_eq!(stmt.cascade_option, CascadeOption::Cascade);
}

#[test]
fn test_revoke_grant_option_for_references() {
    let sql = "REVOKE GRANT OPTION FOR REFERENCES ON TABLE products FROM admin";
    let stmt = parse_revoke(sql);

    assert!(stmt.grant_option_for);
    assert_eq!(stmt.privileges, vec![PrivilegeType::References]);
    assert_eq!(stmt.object_name, "PRODUCTS");
    assert_eq!(stmt.grantees, vec!["ADMIN"]);
}
