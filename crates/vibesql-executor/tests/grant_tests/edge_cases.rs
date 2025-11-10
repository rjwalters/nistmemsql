//! Tests for edge cases and validation scenarios

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::GrantExecutor;
use vibesql_storage::Database;
use vibesql_types::DataType;

#[test]
fn test_grant_to_nonexistent_role() {
    let mut db = Database::new();

    // Create a test table
    let schema = TableSchema::new(
        "users".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Try to grant to a role that doesn't exist
    let grant_stmt = vibesql_ast::GrantStmt {
        privileges: vec![vibesql_ast::PrivilegeType::Select(None)],
        object_type: vibesql_ast::ObjectType::Table,
        object_name: "users".to_string(),
        for_type_name: None,
        grantees: vec!["nonexistent_role".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_err(), "Should fail when role doesn't exist");

    match result {
        Err(vibesql_executor::ExecutorError::RoleNotFound(name)) => {
            assert_eq!(name, "nonexistent_role", "Error should report the missing role");
        }
        _ => panic!("Expected RoleNotFound error, got {:?}", result),
    }
}

#[test]
fn test_grant_on_nonexistent_schema() {
    let mut db = Database::new();

    // Create the role so we test schema validation, not role validation
    db.catalog.create_role("manager".to_string()).unwrap();

    // Try to grant on a schema that doesn't exist
    let grant_stmt = vibesql_ast::GrantStmt {
        privileges: vec![vibesql_ast::PrivilegeType::Usage],
        object_type: vibesql_ast::ObjectType::Schema,
        object_name: "nonexistent_schema".to_string(),
        for_type_name: None,
        grantees: vec!["manager".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_err(), "Should fail when schema doesn't exist");

    match result {
        Err(vibesql_executor::ExecutorError::SchemaNotFound(name)) => {
            assert_eq!(name, "nonexistent_schema", "Error should report the missing schema");
        }
        _ => panic!("Expected SchemaNotFound error, got {:?}", result),
    }
}

#[test]
fn test_grant_on_schema_with_existing_role() {
    let mut db = Database::new();

    // Create a schema
    db.catalog.create_schema("myschema".to_string()).unwrap();

    // Create a role
    db.catalog.create_role("developer".to_string()).unwrap();

    // Grant USAGE on schema
    let grant_stmt = vibesql_ast::GrantStmt {
        privileges: vec![vibesql_ast::PrivilegeType::Usage],
        object_type: vibesql_ast::ObjectType::Schema,
        object_name: "myschema".to_string(),
        for_type_name: None,
        grantees: vec!["developer".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Should succeed when both schema and role exist: {:?}", result.err());

    // Verify the privilege was stored
    assert!(
        db.catalog.has_privilege("developer", "myschema", &vibesql_ast::PrivilegeType::Usage),
        "Developer should have USAGE privilege on schema"
    );
}

#[test]
fn test_grant_multiple_privileges_one_invalid_role() {
    let mut db = Database::new();

    // Create a test table
    let schema = TableSchema::new(
        "orders".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Create only one of the two roles
    db.catalog.create_role("manager".to_string()).unwrap();

    // Try to grant to both a valid and invalid role
    let grant_stmt = vibesql_ast::GrantStmt {
        privileges: vec![vibesql_ast::PrivilegeType::Select(None)],
        object_type: vibesql_ast::ObjectType::Table,
        object_name: "orders".to_string(),
        for_type_name: None,
        grantees: vec!["manager".to_string(), "invalid_role".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_err(), "Should fail when any role doesn't exist");

    // Verify no privileges were granted (transaction-like behavior)
    assert!(
        !db.catalog.has_privilege("manager", "orders", &vibesql_ast::PrivilegeType::Select(None)),
        "No privileges should be granted when validation fails"
    );
}
