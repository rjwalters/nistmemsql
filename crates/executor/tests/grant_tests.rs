//! Tests for GRANT statement execution

use catalog::{ColumnSchema, TableSchema};
use executor::GrantExecutor;
use storage::Database;
use types::DataType;

#[test]
fn test_grant_select_on_table() {
    let mut db = Database::new();

    // Create a test table
    let schema = TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Grant SELECT privilege
    let grant_stmt = ast::GrantStmt {
        privileges: vec![ast::PrivilegeType::Select],
        object_type: ast::ObjectType::Table,
        object_name: "users".to_string(),
        grantees: vec!["manager".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Failed to execute GRANT: {:?}", result.err());

    // Verify the privilege was stored
    assert!(
        db.catalog.has_privilege("manager", "users", &ast::PrivilegeType::Select),
        "Privilege was not stored in catalog"
    );
}

#[test]
fn test_grant_on_qualified_table() {
    let mut db = Database::new();

    // Create a schema
    db.catalog.create_schema("myschema".to_string()).unwrap();
    db.catalog.set_current_schema("myschema").unwrap();

    // Create a test table in the schema
    let schema = TableSchema::new(
        "products".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
    );
    db.catalog
        .create_table_in_schema("myschema", schema)
        .unwrap();

    // Grant SELECT privilege with qualified name
    let grant_stmt = ast::GrantStmt {
        privileges: vec![ast::PrivilegeType::Select],
        object_type: ast::ObjectType::Table,
        object_name: "myschema.products".to_string(),
        grantees: vec!["clerk".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Failed to execute GRANT: {:?}", result.err());

    // Verify the privilege was stored
    assert!(
        db.catalog.has_privilege("clerk", "myschema.products", &ast::PrivilegeType::Select),
        "Privilege was not stored in catalog"
    );
}

#[test]
fn test_grant_on_nonexistent_table() {
    let mut db = Database::new();

    // Try to grant on a table that doesn't exist
    let grant_stmt = ast::GrantStmt {
        privileges: vec![ast::PrivilegeType::Select],
        object_type: ast::ObjectType::Table,
        object_name: "nonexistent".to_string(),
        grantees: vec!["manager".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_err(), "Should fail when table doesn't exist");
}

#[test]
fn test_grant_to_multiple_grantees() {
    let mut db = Database::new();

    // Create a test table
    let schema = TableSchema::new(
        "orders".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("amount".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Grant SELECT privilege to multiple users
    let grant_stmt = ast::GrantStmt {
        privileges: vec![ast::PrivilegeType::Select],
        object_type: ast::ObjectType::Table,
        object_name: "orders".to_string(),
        grantees: vec!["manager".to_string(), "clerk".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Failed to execute GRANT: {:?}", result.err());

    // Verify both users have the privilege
    assert!(
        db.catalog.has_privilege("manager", "orders", &ast::PrivilegeType::Select),
        "Manager should have SELECT privilege"
    );
    assert!(
        db.catalog.has_privilege("clerk", "orders", &ast::PrivilegeType::Select),
        "Clerk should have SELECT privilege"
    );
}

#[test]
fn test_grant_multiple_privileges() {
    let mut db = Database::new();

    // Create a test table
    let schema = TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Grant multiple privileges to a single grantee
    let grant_stmt = ast::GrantStmt {
        privileges: vec![
            ast::PrivilegeType::Select,
            ast::PrivilegeType::Insert,
            ast::PrivilegeType::Update,
        ],
        object_type: ast::ObjectType::Table,
        object_name: "users".to_string(),
        grantees: vec!["manager".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Failed to execute GRANT: {:?}", result.err());

    // Verify all privileges were stored
    assert!(
        db.catalog.has_privilege("manager", "users", &ast::PrivilegeType::Select),
        "Manager should have SELECT privilege"
    );
    assert!(
        db.catalog.has_privilege("manager", "users", &ast::PrivilegeType::Insert),
        "Manager should have INSERT privilege"
    );
    assert!(
        db.catalog.has_privilege("manager", "users", &ast::PrivilegeType::Update),
        "Manager should have UPDATE privilege"
    );
}

#[test]
fn test_grant_matrix_multiple_privileges_and_grantees() {
    let mut db = Database::new();

    // Create a test table
    let schema = TableSchema::new(
        "products".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Grant multiple privileges to multiple grantees
    // This should create 2 privileges × 2 grantees = 4 grant records
    let grant_stmt = ast::GrantStmt {
        privileges: vec![ast::PrivilegeType::Select, ast::PrivilegeType::Insert],
        object_type: ast::ObjectType::Table,
        object_name: "products".to_string(),
        grantees: vec!["r1".to_string(), "r2".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Failed to execute GRANT: {:?}", result.err());

    // Verify all 4 combinations (2 privileges × 2 grantees)
    assert!(
        db.catalog.has_privilege("r1", "products", &ast::PrivilegeType::Select),
        "r1 should have SELECT privilege"
    );
    assert!(
        db.catalog.has_privilege("r1", "products", &ast::PrivilegeType::Insert),
        "r1 should have INSERT privilege"
    );
    assert!(
        db.catalog.has_privilege("r2", "products", &ast::PrivilegeType::Select),
        "r2 should have SELECT privilege"
    );
    assert!(
        db.catalog.has_privilege("r2", "products", &ast::PrivilegeType::Insert),
        "r2 should have INSERT privilege"
    );
}

#[test]
fn test_grant_all_four_privilege_types() {
    let mut db = Database::new();

    // Create a test table
    let schema = TableSchema::new(
        "data".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Grant all four privilege types
    let grant_stmt = ast::GrantStmt {
        privileges: vec![
            ast::PrivilegeType::Select,
            ast::PrivilegeType::Insert,
            ast::PrivilegeType::Update,
            ast::PrivilegeType::Delete,
        ],
        object_type: ast::ObjectType::Table,
        object_name: "data".to_string(),
        grantees: vec!["admin".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Failed to execute GRANT: {:?}", result.err());

    // Verify all four privileges were stored
    assert!(
        db.catalog.has_privilege("admin", "data", &ast::PrivilegeType::Select),
        "Admin should have SELECT privilege"
    );
    assert!(
        db.catalog.has_privilege("admin", "data", &ast::PrivilegeType::Insert),
        "Admin should have INSERT privilege"
    );
    assert!(
        db.catalog.has_privilege("admin", "data", &ast::PrivilegeType::Update),
        "Admin should have UPDATE privilege"
    );
    assert!(
        db.catalog.has_privilege("admin", "data", &ast::PrivilegeType::Delete),
        "Admin should have DELETE privilege"
    );
}

#[test]
fn test_grant_all_privileges_expands_to_table_privileges() {
    let mut db = Database::new();

    // Create a test table
    let schema = TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Grant ALL PRIVILEGES
    let grant_stmt = ast::GrantStmt {
        privileges: vec![ast::PrivilegeType::AllPrivileges],
        object_type: ast::ObjectType::Table,
        object_name: "users".to_string(),
        grantees: vec!["manager".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Failed to execute GRANT ALL: {:?}", result.err());

    // Verify all 5 table privileges were granted (SELECT, INSERT, UPDATE, DELETE, REFERENCES)
    assert!(
        db.catalog.has_privilege("manager", "users", &ast::PrivilegeType::Select),
        "Manager should have SELECT privilege"
    );
    assert!(
        db.catalog.has_privilege("manager", "users", &ast::PrivilegeType::Insert),
        "Manager should have INSERT privilege"
    );
    assert!(
        db.catalog.has_privilege("manager", "users", &ast::PrivilegeType::Update),
        "Manager should have UPDATE privilege"
    );
    assert!(
        db.catalog.has_privilege("manager", "users", &ast::PrivilegeType::Delete),
        "Manager should have DELETE privilege"
    );
    assert!(
        db.catalog.has_privilege("manager", "users", &ast::PrivilegeType::References),
        "Manager should have REFERENCES privilege"
    );
}

#[test]
fn test_grant_all_privileges_to_multiple_grantees() {
    let mut db = Database::new();

    // Create a test table
    let schema = TableSchema::new(
        "orders".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("amount".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Grant ALL PRIVILEGES to multiple grantees
    let grant_stmt = ast::GrantStmt {
        privileges: vec![ast::PrivilegeType::AllPrivileges],
        object_type: ast::ObjectType::Table,
        object_name: "orders".to_string(),
        grantees: vec!["manager".to_string(), "clerk".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Failed to execute GRANT ALL: {:?}", result.err());

    // Verify both grantees received all privileges
    // Manager should have all 5 privileges
    assert!(
        db.catalog.has_privilege("manager", "orders", &ast::PrivilegeType::Select),
        "Manager should have SELECT"
    );
    assert!(
        db.catalog.has_privilege("manager", "orders", &ast::PrivilegeType::Insert),
        "Manager should have INSERT"
    );
    assert!(
        db.catalog.has_privilege("manager", "orders", &ast::PrivilegeType::Update),
        "Manager should have UPDATE"
    );
    assert!(
        db.catalog.has_privilege("manager", "orders", &ast::PrivilegeType::Delete),
        "Manager should have DELETE"
    );
    assert!(
        db.catalog.has_privilege("manager", "orders", &ast::PrivilegeType::References),
        "Manager should have REFERENCES"
    );

    // Clerk should have all 5 privileges
    assert!(
        db.catalog.has_privilege("clerk", "orders", &ast::PrivilegeType::Select),
        "Clerk should have SELECT"
    );
    assert!(
        db.catalog.has_privilege("clerk", "orders", &ast::PrivilegeType::Insert),
        "Clerk should have INSERT"
    );
    assert!(
        db.catalog.has_privilege("clerk", "orders", &ast::PrivilegeType::Update),
        "Clerk should have UPDATE"
    );
    assert!(
        db.catalog.has_privilege("clerk", "orders", &ast::PrivilegeType::Delete),
        "Clerk should have DELETE"
    );
    assert!(
        db.catalog.has_privilege("clerk", "orders", &ast::PrivilegeType::References),
        "Clerk should have REFERENCES"
    );
}

#[test]
fn test_grant_with_grant_option_stored_in_catalog() {
    let mut db = Database::new();

    // Create a test table
    let schema = TableSchema::new(
        "users".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Grant SELECT privilege WITH GRANT OPTION
    let grant_stmt = ast::GrantStmt {
        privileges: vec![ast::PrivilegeType::Select],
        object_type: ast::ObjectType::Table,
        object_name: "users".to_string(),
        grantees: vec!["manager".to_string()],
        with_grant_option: true,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Failed to execute GRANT: {:?}", result.err());

    // Verify the with_grant_option flag was stored correctly
    let grants = db.catalog.get_grants_for_grantee("manager");
    assert_eq!(grants.len(), 1, "Should have exactly one grant");
    assert!(
        grants[0].with_grant_option,
        "with_grant_option should be true"
    );
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

    // Grant SELECT privilege WITHOUT GRANT OPTION
    let grant_stmt = ast::GrantStmt {
        privileges: vec![ast::PrivilegeType::Select],
        object_type: ast::ObjectType::Table,
        object_name: "users".to_string(),
        grantees: vec!["clerk".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Failed to execute GRANT: {:?}", result.err());

    // Verify the with_grant_option flag was stored correctly
    let grants = db.catalog.get_grants_for_grantee("clerk");
    assert_eq!(grants.len(), 1, "Should have exactly one grant");
    assert!(
        !grants[0].with_grant_option,
        "with_grant_option should be false"
    );
}

#[test]
fn test_grant_all_privileges_with_grant_option() {
    let mut db = Database::new();

    // Create a test table
    let schema = TableSchema::new(
        "products".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Grant ALL PRIVILEGES WITH GRANT OPTION
    let grant_stmt = ast::GrantStmt {
        privileges: vec![ast::PrivilegeType::AllPrivileges],
        object_type: ast::ObjectType::Table,
        object_name: "products".to_string(),
        grantees: vec!["admin".to_string()],
        with_grant_option: true,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Failed to execute GRANT: {:?}", result.err());

    // Verify all expanded privileges have with_grant_option = true
    let grants = db.catalog.get_grants_for_grantee("admin");
    assert_eq!(
        grants.len(),
        5,
        "ALL PRIVILEGES should expand to 5 table privileges"
    );

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

    // Grant SELECT privilege to multiple grantees WITH GRANT OPTION
    let grant_stmt = ast::GrantStmt {
        privileges: vec![ast::PrivilegeType::Select],
        object_type: ast::ObjectType::Table,
        object_name: "orders".to_string(),
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
