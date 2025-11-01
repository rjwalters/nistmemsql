//! Test that REVOKE succeeds on non-existent objects (SQL:1999 compliance)

use ast::*;
use executor::RevokeExecutor;
use storage::Database;

#[test]
fn test_revoke_on_nonexistent_function() {
    let mut database = Database::new();

    // Create a role for the revoke operation
    database.catalog.create_role("test_role".to_string()).unwrap();

    let stmt = RevokeStmt {
        privileges: vec![PrivilegeType::Execute],
        object_type: ObjectType::Function,
        object_name: "NONEXISTENT_FUNC".to_string(),
        for_type_name: None,
        grantees: vec!["test_role".to_string()],
        granted_by: None,
        grant_option_for: false,
        cascade_option: CascadeOption::Restrict,
    };

    let result = RevokeExecutor::execute_revoke(&stmt, &mut database);
    assert!(result.is_ok());
}

#[test]
fn test_revoke_on_nonexistent_procedure() {
    let mut database = Database::new();

    // Create a role for the revoke operation
    database.catalog.create_role("test_role".to_string()).unwrap();

    let stmt = RevokeStmt {
        privileges: vec![PrivilegeType::Execute],
        object_type: ObjectType::Procedure,
        object_name: "NONEXISTENT_PROC".to_string(),
        for_type_name: None,
        grantees: vec!["test_role".to_string()],
        granted_by: None,
        grant_option_for: false,
        cascade_option: CascadeOption::Restrict,
    };

    let result = RevokeExecutor::execute_revoke(&stmt, &mut database);
    assert!(result.is_ok());
}

#[test]
fn test_revoke_on_nonexistent_routine() {
    let mut database = Database::new();

    // Create a role for the revoke operation
    database.catalog.create_role("test_role".to_string()).unwrap();

    let stmt = RevokeStmt {
        privileges: vec![PrivilegeType::Execute],
        object_type: ObjectType::Routine,
        object_name: "NONEXISTENT_ROUTINE".to_string(),
        for_type_name: None,
        grantees: vec!["test_role".to_string()],
        granted_by: None,
        grant_option_for: false,
        cascade_option: CascadeOption::Restrict,
    };

    let result = RevokeExecutor::execute_revoke(&stmt, &mut database);
    assert!(result.is_ok());
}
