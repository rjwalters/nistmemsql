//! Tests for WITH GRANT OPTION privilege propagation

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::GrantExecutor;
use vibesql_storage::Database;
use vibesql_types::DataType;

#[test]
fn test_grant_with_grant_option_stored_in_catalog() {
    let mut db = Database::new();

    // Create a test table
    let schema = TableSchema::new(
        "users".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Create the role
    db.catalog.create_role("manager".to_string()).unwrap();

    // Grant SELECT privilege WITH GRANT OPTION
    let grant_stmt = vibesql_ast::GrantStmt {
        privileges: vec![vibesql_ast::PrivilegeType::Select(None)],
        object_type: vibesql_ast::ObjectType::Table,
        object_name: "users".to_string(),
        for_type_name: None,
        grantees: vec!["manager".to_string()],
        with_grant_option: true,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Failed to execute GRANT: {:?}", result.err());

    // Verify the with_grant_option flag was stored correctly
    let grants = db.catalog.get_grants_for_grantee("manager");
    assert_eq!(grants.len(), 1, "Should have exactly one grant");
    assert!(grants[0].with_grant_option, "with_grant_option should be true");
}

#[test]
fn test_grant_without_grant_option_stored_in_catalog() {
    let mut db = Database::new();

    // Create a test table
    let schema = TableSchema::new(
        "users".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Create the role
    db.catalog.create_role("clerk".to_string()).unwrap();

    // Grant SELECT privilege WITHOUT GRANT OPTION
    let grant_stmt = vibesql_ast::GrantStmt {
        privileges: vec![vibesql_ast::PrivilegeType::Select(None)],
        object_type: vibesql_ast::ObjectType::Table,
        object_name: "users".to_string(),
        for_type_name: None,
        grantees: vec!["clerk".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Failed to execute GRANT: {:?}", result.err());

    // Verify the with_grant_option flag was stored correctly
    let grants = db.catalog.get_grants_for_grantee("clerk");
    assert_eq!(grants.len(), 1, "Should have exactly one grant");
    assert!(!grants[0].with_grant_option, "with_grant_option should be false");
}

#[test]
fn test_grant_all_privileges_with_grant_option_on_table() {
    let mut db = Database::new();

    // Create a test table
    let schema = TableSchema::new(
        "products".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Create the role
    db.catalog.create_role("admin".to_string()).unwrap();

    // Grant ALL PRIVILEGES WITH GRANT OPTION
    let grant_stmt = vibesql_ast::GrantStmt {
        privileges: vec![vibesql_ast::PrivilegeType::AllPrivileges],
        object_type: vibesql_ast::ObjectType::Table,
        object_name: "products".to_string(),
        for_type_name: None,
        grantees: vec!["admin".to_string()],
        with_grant_option: true,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Failed to execute GRANT: {:?}", result.err());

    // Verify all expanded privileges have with_grant_option = true
    let grants = db.catalog.get_grants_for_grantee("admin");
    assert_eq!(grants.len(), 5, "ALL PRIVILEGES should expand to 5 table privileges");

    // All 5 grants should have with_grant_option = true
    for grant in grants {
        assert!(
            grant.with_grant_option,
            "All expanded privileges should have with_grant_option = true"
        );
    }
}

#[test]
fn test_grant_multiple_grantees_with_grant_option() {
    let mut db = Database::new();

    // Create a test table
    let schema = TableSchema::new(
        "orders".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Create the roles
    db.catalog.create_role("manager".to_string()).unwrap();
    db.catalog.create_role("clerk".to_string()).unwrap();

    // Grant SELECT privilege to multiple grantees WITH GRANT OPTION
    let grant_stmt = vibesql_ast::GrantStmt {
        privileges: vec![vibesql_ast::PrivilegeType::Select(None)],
        object_type: vibesql_ast::ObjectType::Table,
        object_name: "orders".to_string(),
        for_type_name: None,
        grantees: vec!["manager".to_string(), "clerk".to_string()],
        with_grant_option: true,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Failed to execute GRANT: {:?}", result.err());

    // Verify both grantees have with_grant_option = true
    let manager_grants = db.catalog.get_grants_for_grantee("manager");
    assert_eq!(manager_grants.len(), 1);
    assert!(
        manager_grants[0].with_grant_option,
        "Manager's grant should have with_grant_option = true"
    );

    let clerk_grants = db.catalog.get_grants_for_grantee("clerk");
    assert_eq!(clerk_grants.len(), 1);
    assert!(
        clerk_grants[0].with_grant_option,
        "Clerk's grant should have with_grant_option = true"
    );
}
