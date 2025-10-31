//! Comprehensive GRANT privilege tests
//!
//! Tests for GRANT operations including ALL PRIVILEGES, WITH GRANT OPTION,
//! schema vs table privileges, and error cases.

use ast::*;
use catalog::*;
use executor::*;
use storage::*;
use types::*;

#[test]
fn test_grant_specific_privilege() {
    let mut db = Database::new();

    // Create schema and table
    db.catalog.create_schema("test_schema".to_string()).unwrap();
    let table_schema = TableSchema::new(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(100) }, true),
        ],
    );
    db.create_table(table_schema).unwrap();

    // Create role
    db.catalog.create_role("user1".to_string()).unwrap();

    // Grant SELECT privilege
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::Select],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        grantees: vec!["user1".to_string()],
        with_grant_option: false,
    };
    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();

    // Verify grant
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Select));
    assert!(result.contains("Granted"));
}

#[test]
fn test_grant_all_privileges_table() {
    let mut db = Database::new();

    // Create schema and table
    db.catalog.create_schema("test_schema".to_string()).unwrap();
    let table_schema = TableSchema::new(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(100) }, true),
        ],
    );
    db.create_table(table_schema).unwrap();

    // Create role
    db.catalog.create_role("user1".to_string()).unwrap();

    // Grant ALL PRIVILEGES on table
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::AllPrivileges],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        grantees: vec!["user1".to_string()],
        with_grant_option: false,
    };
    GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();

    // Verify all table privileges granted
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Select));
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Insert));
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Update));
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Delete));
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::References));
}

#[test]
fn test_grant_all_privileges_schema() {
    let mut db = Database::new();

    // Create schema
    db.catalog.create_schema("test_schema".to_string()).unwrap();

    // Create role
    db.catalog.create_role("user1".to_string()).unwrap();

    // Grant ALL PRIVILEGES on schema
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::AllPrivileges],
        object_type: ObjectType::Schema,
        object_name: "test_schema".to_string(),
        grantees: vec!["user1".to_string()],
        with_grant_option: false,
    };
    GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();

    // Verify schema privileges granted
    assert!(db.catalog.has_privilege("user1", "test_schema", &PrivilegeType::Usage));
    assert!(db.catalog.has_privilege("user1", "test_schema", &PrivilegeType::Create));
}

#[test]
fn test_grant_with_grant_option() {
    let mut db = Database::new();

    // Create schema and table
    db.catalog.create_schema("test_schema".to_string()).unwrap();
    let table_schema = TableSchema::new(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(100) }, true),
        ],
    );
    db.create_table(table_schema).unwrap();

    // Create roles
    db.catalog.create_role("user1".to_string()).unwrap();

    // Grant SELECT with GRANT OPTION
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::Select],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        grantees: vec!["user1".to_string()],
        with_grant_option: true,
    };
    GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();

    // Verify both privilege and grant option granted
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Select));
    let grants = db.catalog.get_grants_for_grantee("user1");
    let grant = grants.iter().find(|g| g.object == "test_table" && g.privilege == PrivilegeType::Select).unwrap();
    assert!(grant.with_grant_option);
}

#[test]
fn test_grant_multiple_privileges() {
    let mut db = Database::new();

    // Create schema and table
    db.catalog.create_schema("test_schema".to_string()).unwrap();
    let table_schema = TableSchema::new(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(100) }, true),
        ],
    );
    db.create_table(table_schema).unwrap();

    // Create role
    db.catalog.create_role("user1".to_string()).unwrap();

    // Grant multiple privileges
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::Select, PrivilegeType::Insert, PrivilegeType::Update],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        grantees: vec!["user1".to_string()],
        with_grant_option: false,
    };
    GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();

    // Verify all privileges granted
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Select));
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Insert));
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Update));
    // Not granted
    assert!(!db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Delete));
}

#[test]
fn test_grant_multiple_grantees() {
    let mut db = Database::new();

    // Create schema and table
    db.catalog.create_schema("test_schema".to_string()).unwrap();
    let table_schema = TableSchema::new(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(100) }, true),
        ],
    );
    db.create_table(table_schema).unwrap();

    // Create roles
    db.catalog.create_role("user1".to_string()).unwrap();
    db.catalog.create_role("user2".to_string()).unwrap();
    db.catalog.create_role("user3".to_string()).unwrap();

    // Grant to multiple grantees
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::Select],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        grantees: vec!["user1".to_string(), "user2".to_string(), "user3".to_string()],
        with_grant_option: false,
    };
    GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();

    // Verify all grantees have privilege
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Select));
    assert!(db.catalog.has_privilege("user2", "test_table", &PrivilegeType::Select));
    assert!(db.catalog.has_privilege("user3", "test_table", &PrivilegeType::Select));
}

#[test]
fn test_grant_schema_privileges() {
    let mut db = Database::new();

    // Create schema
    db.catalog.create_schema("test_schema".to_string()).unwrap();

    // Create role
    db.catalog.create_role("user1".to_string()).unwrap();

    // Grant schema privileges
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::Usage, PrivilegeType::Create],
        object_type: ObjectType::Schema,
        object_name: "test_schema".to_string(),
        grantees: vec!["user1".to_string()],
        with_grant_option: false,
    };
    GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();

    // Verify schema privileges
    assert!(db.catalog.has_privilege("user1", "test_schema", &PrivilegeType::Usage));
    assert!(db.catalog.has_privilege("user1", "test_schema", &PrivilegeType::Create));
}

#[test]
fn test_grant_to_non_existent_role() {
    let mut db = Database::new();

    // Create schema and table
    db.catalog.create_schema("test_schema".to_string()).unwrap();
    let table_schema = TableSchema::new(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(100) }, true),
        ],
    );
    db.create_table(table_schema).unwrap();

    // Try to grant to non-existent role
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::Select],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        grantees: vec!["non_existent_role".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::RoleNotFound(_)));
}

#[test]
fn test_grant_on_non_existent_table() {
    let mut db = Database::new();

    // Create role
    db.catalog.create_role("user1".to_string()).unwrap();

    // Try to grant on non-existent table
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::Select],
        object_type: ObjectType::Table,
        object_name: "non_existent_table".to_string(),
        grantees: vec!["user1".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::TableNotFound(_)));
}

#[test]
fn test_grant_on_non_existent_schema() {
    let mut db = Database::new();

    // Create role
    db.catalog.create_role("user1".to_string()).unwrap();

    // Try to grant on non-existent schema
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::Usage],
        object_type: ObjectType::Schema,
        object_name: "non_existent_schema".to_string(),
        grantees: vec!["user1".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::SchemaNotFound(_)));
}

#[test]
fn test_grant_idempotent() {
    let mut db = Database::new();

    // Create schema and table
    db.catalog.create_schema("test_schema".to_string()).unwrap();
    let table_schema = TableSchema::new(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(100) }, true),
        ],
    );
    db.create_table(table_schema).unwrap();

    // Create role
    db.catalog.create_role("user1".to_string()).unwrap();

    // Grant same privilege twice (should be idempotent)
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::Select],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        grantees: vec!["user1".to_string()],
        with_grant_option: false,
    };

    GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();
    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();

    // Should succeed both times
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Select));
    assert!(result.contains("Granted"));
}
