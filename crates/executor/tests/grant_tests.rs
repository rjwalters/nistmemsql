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

// Phase 2.5: Schema privilege executor tests

#[test]
fn test_grant_usage_on_schema() {
    let mut db = Database::new();

    // Create a schema
    db.catalog.create_schema("app_schema".to_string()).unwrap();

    // Grant USAGE privilege on schema
    let grant_stmt = ast::GrantStmt {
        privileges: vec![ast::PrivilegeType::Usage],
        object_type: ast::ObjectType::Schema,
        object_name: "app_schema".to_string(),
        grantees: vec!["user_role".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Failed to execute GRANT USAGE: {:?}", result.err());

    // Verify the privilege was stored
    assert!(
        db.catalog.has_privilege("user_role", "app_schema", &ast::PrivilegeType::Usage),
        "user_role should have USAGE privilege on app_schema schema"
    );
}

#[test]
fn test_grant_create_on_schema() {
    let mut db = Database::new();

    // Create a schema
    db.catalog.create_schema("admin_schema".to_string()).unwrap();

    // Grant CREATE privilege on schema
    let grant_stmt = ast::GrantStmt {
        privileges: vec![ast::PrivilegeType::Create],
        object_type: ast::ObjectType::Schema,
        object_name: "admin_schema".to_string(),
        grantees: vec!["admin_role".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Failed to execute GRANT CREATE: {:?}", result.err());

    // Verify the privilege was stored
    assert!(
        db.catalog.has_privilege("admin_role", "admin_schema", &ast::PrivilegeType::Create),
        "admin_role should have CREATE privilege on admin_schema schema"
    );
}

#[test]
fn test_grant_all_privileges_on_schema_expands_correctly() {
    let mut db = Database::new();

    // Create a schema
    db.catalog.create_schema("myschema".to_string()).unwrap();

    // Grant ALL PRIVILEGES on schema
    let grant_stmt = ast::GrantStmt {
        privileges: vec![ast::PrivilegeType::AllPrivileges],
        object_type: ast::ObjectType::Schema,
        object_name: "myschema".to_string(),
        grantees: vec!["developer".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Failed to execute GRANT ALL: {:?}", result.err());

    // Verify ALL PRIVILEGES expands to [Usage, Create] for schemas
    assert!(
        db.catalog.has_privilege("developer", "myschema", &ast::PrivilegeType::Usage),
        "developer should have USAGE privilege from ALL PRIVILEGES"
    );
    assert!(
        db.catalog.has_privilege("developer", "myschema", &ast::PrivilegeType::Create),
        "developer should have CREATE privilege from ALL PRIVILEGES"
    );

    // Verify only schema-specific privileges were granted (not table privileges)
    assert!(
        !db.catalog.has_privilege("developer", "myschema", &ast::PrivilegeType::Select),
        "developer should NOT have SELECT privilege (table-only)"
    );
}

#[test]
fn test_grant_on_nonexistent_schema() {
    let mut db = Database::new();

    // Try to grant on a schema that doesn't exist
    let grant_stmt = ast::GrantStmt {
        privileges: vec![ast::PrivilegeType::Usage],
        object_type: ast::ObjectType::Schema,
        object_name: "nonexistent_schema".to_string(),
        grantees: vec!["user_role".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_err(), "Should fail when schema doesn't exist");

    // Verify it's the correct error type
    match result {
        Err(executor::ExecutorError::SchemaNotFound(name)) => {
            assert_eq!(name, "nonexistent_schema", "Error should reference the correct schema");
        }
        _ => panic!("Expected SchemaNotFound error, got {:?}", result),
    }
}

#[test]
fn test_grant_schema_with_grant_option() {
    let mut db = Database::new();

    // Create a schema
    db.catalog.create_schema("power_schema".to_string()).unwrap();

    // Grant USAGE privilege with GRANT OPTION
    let grant_stmt = ast::GrantStmt {
        privileges: vec![ast::PrivilegeType::Usage],
        object_type: ast::ObjectType::Schema,
        object_name: "power_schema".to_string(),
        grantees: vec!["power_user".to_string()],
        with_grant_option: true,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Failed to execute GRANT with GRANT OPTION: {:?}", result.err());

    // Verify the privilege was stored
    assert!(
        db.catalog.has_privilege("power_user", "power_schema", &ast::PrivilegeType::Usage),
        "power_user should have USAGE privilege"
    );

    // Verify with_grant_option flag is set
    let grants = db.catalog.get_grants_for_grantee("power_user");
    assert!(!grants.is_empty(), "Should have at least one grant");

    let grant = grants.iter()
        .find(|g| g.object == "power_schema" && g.privilege == ast::PrivilegeType::Usage)
        .expect("Should find the USAGE grant on power_schema");

    assert!(
        grant.with_grant_option,
        "Grant should have with_grant_option set to true"
    );
}

#[test]
fn test_grant_multiple_schema_privileges_to_multiple_grantees() {
    let mut db = Database::new();

    // Create a schema
    db.catalog.create_schema("shared".to_string()).unwrap();

    // Grant both USAGE and CREATE to multiple grantees
    let grant_stmt = ast::GrantStmt {
        privileges: vec![ast::PrivilegeType::Usage, ast::PrivilegeType::Create],
        object_type: ast::ObjectType::Schema,
        object_name: "shared".to_string(),
        grantees: vec!["dev1".to_string(), "dev2".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_ok(), "Failed to execute GRANT: {:?}", result.err());

    // Verify all 4 combinations (2 privileges × 2 grantees)
    assert!(
        db.catalog.has_privilege("dev1", "shared", &ast::PrivilegeType::Usage),
        "dev1 should have USAGE privilege"
    );
    assert!(
        db.catalog.has_privilege("dev1", "shared", &ast::PrivilegeType::Create),
        "dev1 should have CREATE privilege"
    );
    assert!(
        db.catalog.has_privilege("dev2", "shared", &ast::PrivilegeType::Usage),
        "dev2 should have USAGE privilege"
    );
    assert!(
        db.catalog.has_privilege("dev2", "shared", &ast::PrivilegeType::Create),
        "dev2 should have CREATE privilege"
    );
}
