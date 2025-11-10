//! Privilege checker tests
//!
//! Tests for PrivilegeChecker methods that enforce access control.

use vibesql_ast::*;
use vibesql_catalog::*;
use vibesql_storage::*;
use vibesql_types::*;

use super::super::*;

#[test]
fn test_check_select_with_privilege() {
    let mut db = Database::new();

    // Create schema and table
    db.catalog.create_schema("test_schema".to_string()).unwrap();
    let table_schema = TableSchema::new(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(table_schema).unwrap();

    // Create role and grant SELECT
    db.catalog.create_role("user1".to_string()).unwrap();
    let grant = PrivilegeGrant {
        object: "test_table".to_string(),
        object_type: ObjectType::Table,
        privilege: PrivilegeType::Select(None),
        grantee: "user1".to_string(),
        grantor: "admin".to_string(),
        with_grant_option: false,
    };
    db.catalog.add_grant(grant);

    // Set current role
    db.set_role(Some("user1".to_string()));

    // Enable security
    db.enable_security();

    // Check SELECT should succeed
    let result = PrivilegeChecker::check_select(&db, "test_table");
    assert!(result.is_ok());
}

#[test]
fn test_check_select_without_privilege() {
    let mut db = Database::new();

    // Create schema and table
    db.catalog.create_schema("test_schema".to_string()).unwrap();
    let table_schema = TableSchema::new(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(table_schema).unwrap();

    // Create role but don't grant privilege
    db.catalog.create_role("user1".to_string()).unwrap();
    db.set_role(Some("user1".to_string()));
    db.enable_security();

    // Check SELECT should fail
    let result = PrivilegeChecker::check_select(&db, "test_table");
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::PermissionDenied { .. }));
}

#[test]
fn test_check_insert_with_privilege() {
    let mut db = Database::new();

    // Create schema and table
    db.catalog.create_schema("test_schema".to_string()).unwrap();
    let table_schema = TableSchema::new(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(table_schema).unwrap();

    // Create role and grant INSERT
    db.catalog.create_role("user1".to_string()).unwrap();
    let grant = PrivilegeGrant {
        object: "test_table".to_string(),
        object_type: ObjectType::Table,
        privilege: PrivilegeType::Insert(None),
        grantee: "user1".to_string(),
        grantor: "admin".to_string(),
        with_grant_option: false,
    };
    db.catalog.add_grant(grant);

    db.set_role(Some("user1".to_string()));
    db.enable_security();

    let result = PrivilegeChecker::check_insert(&db, "test_table");
    assert!(result.is_ok());
}

#[test]
fn test_check_update_with_privilege() {
    let mut db = Database::new();

    // Create schema and table
    db.catalog.create_schema("test_schema".to_string()).unwrap();
    let table_schema = TableSchema::new(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(table_schema).unwrap();

    // Create role and grant UPDATE
    db.catalog.create_role("user1".to_string()).unwrap();
    let grant = PrivilegeGrant {
        object: "test_table".to_string(),
        object_type: ObjectType::Table,
        privilege: PrivilegeType::Update(None),
        grantee: "user1".to_string(),
        grantor: "admin".to_string(),
        with_grant_option: false,
    };
    db.catalog.add_grant(grant);

    db.set_role(Some("user1".to_string()));
    db.enable_security();

    let result = PrivilegeChecker::check_update(&db, "test_table");
    assert!(result.is_ok());
}

#[test]
fn test_check_delete_with_privilege() {
    let mut db = Database::new();

    // Create schema and table
    db.catalog.create_schema("test_schema".to_string()).unwrap();
    let table_schema = TableSchema::new(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(table_schema).unwrap();

    // Create role and grant DELETE
    db.catalog.create_role("user1".to_string()).unwrap();
    let grant = PrivilegeGrant {
        object: "test_table".to_string(),
        object_type: ObjectType::Table,
        privilege: PrivilegeType::Delete,
        grantee: "user1".to_string(),
        grantor: "admin".to_string(),
        with_grant_option: false,
    };
    db.catalog.add_grant(grant);

    db.set_role(Some("user1".to_string()));
    db.enable_security();

    let result = PrivilegeChecker::check_delete(&db, "test_table");
    assert!(result.is_ok());
}

#[test]
fn test_check_create_with_privilege() {
    let mut db = Database::new();

    // Create schema
    db.catalog.create_schema("test_schema".to_string()).unwrap();

    // Create role and grant CREATE on schema
    db.catalog.create_role("user1".to_string()).unwrap();
    let grant = PrivilegeGrant {
        object: "test_schema".to_string(),
        object_type: ObjectType::Schema,
        privilege: PrivilegeType::Create,
        grantee: "user1".to_string(),
        grantor: "admin".to_string(),
        with_grant_option: false,
    };
    db.catalog.add_grant(grant);

    db.set_role(Some("user1".to_string()));
    db.enable_security();

    let result = PrivilegeChecker::check_create(&db, "test_schema");
    assert!(result.is_ok());
}

#[test]
fn test_check_drop_with_privilege() {
    let mut db = Database::new();

    // Create schema and table
    db.catalog.create_schema("test_schema".to_string()).unwrap();
    let table_schema = TableSchema::new(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(table_schema).unwrap();

    // Create role and grant DELETE (used for DROP)
    db.catalog.create_role("user1".to_string()).unwrap();
    let grant = PrivilegeGrant {
        object: "test_table".to_string(),
        object_type: ObjectType::Table,
        privilege: PrivilegeType::Delete,
        grantee: "user1".to_string(),
        grantor: "admin".to_string(),
        with_grant_option: false,
    };
    db.catalog.add_grant(grant);

    db.set_role(Some("user1".to_string()));
    db.enable_security();

    let result = PrivilegeChecker::check_drop(&db, "test_table");
    assert!(result.is_ok());
}

#[test]
fn test_check_alter_with_privilege() {
    let mut db = Database::new();

    // Create schema and table
    db.catalog.create_schema("test_schema".to_string()).unwrap();
    let table_schema = TableSchema::new(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(table_schema).unwrap();

    // Create role and grant CREATE on table (used for ALTER)
    db.catalog.create_role("user1".to_string()).unwrap();
    let grant = PrivilegeGrant {
        object: "test_table".to_string(),
        object_type: ObjectType::Table,
        privilege: PrivilegeType::Create,
        grantee: "user1".to_string(),
        grantor: "admin".to_string(),
        with_grant_option: false,
    };
    db.catalog.add_grant(grant);

    db.set_role(Some("user1".to_string()));
    db.enable_security();

    let result = PrivilegeChecker::check_alter(&db, "test_table");
    assert!(result.is_ok());
}

#[test]
fn test_admin_role_bypasses_checks() {
    let mut db = Database::new();

    // Create schema and table
    db.catalog.create_schema("test_schema".to_string()).unwrap();
    let table_schema = TableSchema::new(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(table_schema).unwrap();

    // Create admin role
    db.catalog.create_role("ADMIN".to_string()).unwrap();
    db.set_role(Some("ADMIN".to_string()));
    db.enable_security();

    // All checks should pass for admin
    assert!(PrivilegeChecker::check_select(&db, "test_table").is_ok());
    assert!(PrivilegeChecker::check_insert(&db, "test_table").is_ok());
    assert!(PrivilegeChecker::check_update(&db, "test_table").is_ok());
    assert!(PrivilegeChecker::check_delete(&db, "test_table").is_ok());
    assert!(PrivilegeChecker::check_create(&db, "test_schema").is_ok());
    assert!(PrivilegeChecker::check_drop(&db, "test_table").is_ok());
    assert!(PrivilegeChecker::check_alter(&db, "test_table").is_ok());
}

#[test]
fn test_dba_role_bypasses_checks() {
    let mut db = Database::new();

    // Create schema and table
    db.catalog.create_schema("test_schema".to_string()).unwrap();
    let table_schema = TableSchema::new(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(table_schema).unwrap();

    // Create DBA role
    db.catalog.create_role("DBA".to_string()).unwrap();
    db.set_role(Some("DBA".to_string()));
    db.enable_security();

    // All checks should pass for DBA
    assert!(PrivilegeChecker::check_select(&db, "test_table").is_ok());
    assert!(PrivilegeChecker::check_insert(&db, "test_table").is_ok());
    assert!(PrivilegeChecker::check_update(&db, "test_table").is_ok());
    assert!(PrivilegeChecker::check_delete(&db, "test_table").is_ok());
    assert!(PrivilegeChecker::check_create(&db, "test_schema").is_ok());
    assert!(PrivilegeChecker::check_drop(&db, "test_table").is_ok());
    assert!(PrivilegeChecker::check_alter(&db, "test_table").is_ok());
}

#[test]
fn test_security_disabled_bypasses_checks() {
    let mut db = Database::new();

    // Create schema and table
    db.catalog.create_schema("test_schema".to_string()).unwrap();
    let table_schema = TableSchema::new(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(table_schema).unwrap();

    // Create regular role, don't grant privileges
    db.catalog.create_role("user1".to_string()).unwrap();
    db.set_role(Some("user1".to_string()));
    // Security is disabled by default

    // All checks should pass when security is disabled
    assert!(PrivilegeChecker::check_select(&db, "test_table").is_ok());
    assert!(PrivilegeChecker::check_insert(&db, "test_table").is_ok());
    assert!(PrivilegeChecker::check_update(&db, "test_table").is_ok());
    assert!(PrivilegeChecker::check_delete(&db, "test_table").is_ok());
    assert!(PrivilegeChecker::check_create(&db, "test_schema").is_ok());
    assert!(PrivilegeChecker::check_drop(&db, "test_table").is_ok());
    assert!(PrivilegeChecker::check_alter(&db, "test_table").is_ok());
}

#[test]
fn test_hierarchical_privilege_checks() {
    let mut db = Database::new();

    // Create schema and table
    db.catalog.create_schema("test_schema".to_string()).unwrap();
    let table_schema = TableSchema::new(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(table_schema).unwrap();

    // Create role
    db.catalog.create_role("user1".to_string()).unwrap();

    // Grant SELECT privilege
    let grant = PrivilegeGrant {
        object: "test_table".to_string(),
        object_type: ObjectType::Table,
        privilege: PrivilegeType::Select(None),
        grantee: "user1".to_string(),
        grantor: "admin".to_string(),
        with_grant_option: false,
    };
    db.catalog.add_grant(grant);

    db.set_role(Some("user1".to_string()));
    db.enable_security();

    // Should have SELECT but not other privileges
    assert!(PrivilegeChecker::check_select(&db, "test_table").is_ok());
    assert!(PrivilegeChecker::check_insert(&db, "test_table").is_err());
    assert!(PrivilegeChecker::check_update(&db, "test_table").is_err());
    assert!(PrivilegeChecker::check_delete(&db, "test_table").is_err());
}

#[test]
fn test_schema_level_privileges() {
    let mut db = Database::new();

    // Create schema
    db.catalog.create_schema("test_schema".to_string()).unwrap();

    // Create role
    db.catalog.create_role("user1".to_string()).unwrap();

    // Grant USAGE on schema
    let grant = PrivilegeGrant {
        object: "test_schema".to_string(),
        object_type: ObjectType::Schema,
        privilege: PrivilegeType::Usage,
        grantee: "user1".to_string(),
        grantor: "admin".to_string(),
        with_grant_option: false,
    };
    db.catalog.add_grant(grant);

    db.set_role(Some("user1".to_string()));
    db.enable_security();

    // Should have USAGE privilege on schema
    assert!(PrivilegeChecker::check_create(&db, "test_schema").is_err()); // Need CREATE, not USAGE
}

#[test]
fn test_role_based_privileges() {
    let mut db = Database::new();

    // Create schema and table
    db.catalog.create_schema("test_schema".to_string()).unwrap();
    let table_schema = TableSchema::new(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(table_schema).unwrap();

    // Create two roles
    db.catalog.create_role("role1".to_string()).unwrap();
    db.catalog.create_role("role2".to_string()).unwrap();

    // Grant privilege to role1
    let grant = PrivilegeGrant {
        object: "test_table".to_string(),
        object_type: ObjectType::Table,
        privilege: PrivilegeType::Select(None),
        grantee: "role1".to_string(),
        grantor: "admin".to_string(),
        with_grant_option: false,
    };
    db.catalog.add_grant(grant);

    // Test role1 has privilege
    db.set_role(Some("role1".to_string()));
    db.enable_security();
    assert!(PrivilegeChecker::check_select(&db, "test_table").is_ok());

    // Test role2 does not have privilege
    db.set_role(Some("role2".to_string()));
    assert!(PrivilegeChecker::check_select(&db, "test_table").is_err());
}
