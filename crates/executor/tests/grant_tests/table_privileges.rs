//! Tests for table-level privilege grants (SELECT, INSERT, UPDATE, DELETE, REFERENCES, ALL PRIVILEGES)

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

    // Create the role
    db.catalog.create_role("manager".to_string()).unwrap();

    // Grant SELECT privilege
    let grant_stmt = ast::GrantStmt {
        privileges: vec![ast::PrivilegeType::Select],
        object_type: ast::ObjectType::Table,
        object_name: "users".to_string(),
        for_type_name: None,
        for_type_name: None,
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
    db.catalog.create_table_in_schema("myschema", schema).unwrap();

    // Create the role
    db.catalog.create_role("clerk".to_string()).unwrap();

    // Grant SELECT privilege with qualified name
    let grant_stmt = ast::GrantStmt {
        privileges: vec![ast::PrivilegeType::Select],
        object_type: ast::ObjectType::Table,
        object_name: "myschema.products".to_string(),
        for_type_name: None,
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
        for_type_name: None,
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

    // Create the roles
    db.catalog.create_role("manager".to_string()).unwrap();
    db.catalog.create_role("clerk".to_string()).unwrap();

    // Grant SELECT privilege to multiple users
    let grant_stmt = ast::GrantStmt {
        privileges: vec![ast::PrivilegeType::Select],
        object_type: ast::ObjectType::Table,
        object_name: "orders".to_string(),
        for_type_name: None,
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

    // Create the role
    db.catalog.create_role("manager".to_string()).unwrap();

    // Grant multiple privileges to a single grantee
    let grant_stmt = ast::GrantStmt {
        privileges: vec![
            ast::PrivilegeType::Select,
            ast::PrivilegeType::Insert,
            ast::PrivilegeType::Update(None),
        ],
        object_type: ast::ObjectType::Table,
        object_name: "users".to_string(),
        for_type_name: None,
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
        db.catalog.has_privilege("manager", "users", &ast::PrivilegeType::Update(None)),
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

    // Create the roles
    db.catalog.create_role("r1".to_string()).unwrap();
    db.catalog.create_role("r2".to_string()).unwrap();

    // Grant multiple privileges to multiple grantees
    // This should create 2 privileges × 2 grantees = 4 grant records
    let grant_stmt = ast::GrantStmt {
        privileges: vec![ast::PrivilegeType::Select, ast::PrivilegeType::Insert],
        object_type: ast::ObjectType::Table,
        object_name: "products".to_string(),
        for_type_name: None,
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

    // Create the role
    db.catalog.create_role("admin".to_string()).unwrap();

    // Grant all four privilege types
    let grant_stmt = ast::GrantStmt {
        privileges: vec![
            ast::PrivilegeType::Select,
            ast::PrivilegeType::Insert,
            ast::PrivilegeType::Update(None),
            ast::PrivilegeType::Delete,
        ],
        object_type: ast::ObjectType::Table,
        object_name: "data".to_string(),
        for_type_name: None,
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
        db.catalog.has_privilege("admin", "data", &ast::PrivilegeType::Update(None)),
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

    // Create the role
    db.catalog.create_role("manager".to_string()).unwrap();

    // Grant ALL PRIVILEGES
    let grant_stmt = ast::GrantStmt {
        privileges: vec![ast::PrivilegeType::AllPrivileges],
        object_type: ast::ObjectType::Table,
        object_name: "users".to_string(),
        for_type_name: None,
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
        db.catalog.has_privilege("manager", "users", &ast::PrivilegeType::Update(None)),
        "Manager should have UPDATE privilege"
    );
    assert!(
        db.catalog.has_privilege("manager", "users", &ast::PrivilegeType::Delete),
        "Manager should have DELETE privilege"
    );
    assert!(
        db.catalog.has_privilege("manager", "users", &ast::PrivilegeType::References(None)),
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

    // Create the roles
    db.catalog.create_role("manager".to_string()).unwrap();
    db.catalog.create_role("clerk".to_string()).unwrap();

    // Grant ALL PRIVILEGES to multiple grantees
    let grant_stmt = ast::GrantStmt {
        privileges: vec![ast::PrivilegeType::AllPrivileges],
        object_type: ast::ObjectType::Table,
        object_name: "orders".to_string(),
        for_type_name: None,
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
        db.catalog.has_privilege("manager", "orders", &ast::PrivilegeType::Update(None)),
        "Manager should have UPDATE"
    );
    assert!(
        db.catalog.has_privilege("manager", "orders", &ast::PrivilegeType::Delete),
        "Manager should have DELETE"
    );
    assert!(
        db.catalog.has_privilege("manager", "orders", &ast::PrivilegeType::References(None)),
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
        db.catalog.has_privilege("clerk", "orders", &ast::PrivilegeType::Update(None)),
        "Clerk should have UPDATE"
    );
    assert!(
        db.catalog.has_privilege("clerk", "orders", &ast::PrivilegeType::Delete),
        "Clerk should have DELETE"
    );
    assert!(
        db.catalog.has_privilege("clerk", "orders", &ast::PrivilegeType::References(None)),
        "Clerk should have REFERENCES"
    );
}
