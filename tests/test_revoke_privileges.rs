//! Comprehensive REVOKE privilege tests
//!
//! Tests for REVOKE operations including CASCADE/RESTRICT behavior,
//! GRANT OPTION FOR, multiple grantees, and error cases.

use ast::*;
use catalog::*;
use executor::*;
use storage::*;
use types::*;

#[test]
fn test_revoke_specific_privilege() {
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

    // Create roles
    db.catalog.create_role("user1".to_string()).unwrap();
    db.catalog.create_role("admin".to_string()).unwrap();

    // Grant privileges first
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::Select(None)],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        with_grant_option: false,
    };
    GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();

    // Verify grant exists
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Select(None)));

    // Revoke the privilege
    let revoke_stmt = RevokeStmt {
        privileges: vec![PrivilegeType::Select(None)],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        cascade_option: CascadeOption::Restrict,
        grant_option_for: false,
        granted_by: None,
    };
    let result = RevokeExecutor::execute_revoke(&revoke_stmt, &mut db).unwrap();

    // Verify privilege was revoked
    assert!(!db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Select(None)));
    assert!(result.contains("Revoked"));
}

#[test]
fn test_revoke_all_privileges() {
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

    // Grant ALL PRIVILEGES
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::AllPrivileges],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        with_grant_option: false,
    };
    GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();

    // Verify all privileges granted
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Select(None)));
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Insert(None)));
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Update(None)));
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Delete));
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::References(None)));

    // Revoke ALL PRIVILEGES
    let revoke_stmt = RevokeStmt {
        privileges: vec![PrivilegeType::AllPrivileges],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        cascade_option: CascadeOption::Restrict,
        grant_option_for: false,
        granted_by: None,
    };
    RevokeExecutor::execute_revoke(&revoke_stmt, &mut db).unwrap();

    // Verify all privileges revoked
    assert!(!db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Select(None)));
    assert!(!db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Insert(None)));
    assert!(!db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Update(None)));
    assert!(!db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Delete));
    assert!(!db.catalog.has_privilege("user1", "test_table", &PrivilegeType::References(None)));
}

#[test]
fn test_revoke_with_grant_option_for() {
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

    // Create roles
    db.catalog.create_role("user1".to_string()).unwrap();
    db.catalog.create_role("admin".to_string()).unwrap();

    // Grant with GRANT OPTION
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::Select(None)],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        with_grant_option: true,
    };
    GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();

    // Verify both privilege and grant option exist
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Select(None)));
    let grants = db.catalog.get_grants_for_grantee("user1");
    let grant = grants
        .iter()
        .find(|g| g.object == "test_table" && g.privilege == PrivilegeType::Select(None))
        .unwrap();
    assert!(grant.with_grant_option);

    // Revoke GRANT OPTION FOR (not the privilege itself)
    let revoke_stmt = RevokeStmt {
        privileges: vec![PrivilegeType::Select(None)],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        cascade_option: CascadeOption::Restrict,
        grant_option_for: true,
        granted_by: None,
    };
    let result = RevokeExecutor::execute_revoke(&revoke_stmt, &mut db).unwrap();

    // Verify grant option was revoked but privilege remains
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Select(None)));
    let grants = db.catalog.get_grants_for_grantee("user1");
    let grant = grants
        .iter()
        .find(|g| g.object == "test_table" && g.privilege == PrivilegeType::Select(None))
        .unwrap();
    assert!(!grant.with_grant_option);
    assert!(result.contains("Revoked grant option for"));
}

#[test]
fn test_revoke_cascade_behavior() {
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

    // Create roles
    db.catalog.create_role("admin".to_string()).unwrap();
    db.catalog.create_role("user1".to_string()).unwrap();
    db.catalog.create_role("user2".to_string()).unwrap();

    // Grant SELECT to user1 WITH GRANT OPTION
    let grant_stmt1 = GrantStmt {
        privileges: vec![PrivilegeType::Select(None)],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        with_grant_option: true,
    };
    GrantExecutor::execute_grant(&grant_stmt1, &mut db).unwrap();

    // user1 grants SELECT to user2
    let grant_stmt2 = GrantStmt {
        privileges: vec![PrivilegeType::Select(None)],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        for_type_name: None,
        grantees: vec!["user2".to_string()],
        with_grant_option: false,
    };
    // Simulate user1 as grantor by setting current role
    db.set_role(Some("user1".to_string()));
    GrantExecutor::execute_grant(&grant_stmt2, &mut db).unwrap();

    // Verify both users have privilege
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Select(None)));
    assert!(db.catalog.has_privilege("user2", "test_table", &PrivilegeType::Select(None)));

    // Reset to admin role and revoke CASCADE from user1
    db.set_role(Some("admin".to_string()));
    let revoke_stmt = RevokeStmt {
        privileges: vec![PrivilegeType::Select(None)],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        cascade_option: CascadeOption::Cascade,
        grant_option_for: false,
        granted_by: None,
    };
    RevokeExecutor::execute_revoke(&revoke_stmt, &mut db).unwrap();

    // Verify CASCADE: both user1 and user2 privileges revoked
    assert!(!db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Select(None)));
    assert!(!db.catalog.has_privilege("user2", "test_table", &PrivilegeType::Select(None)));
}

#[test]
fn test_revoke_restrict_with_dependent_grants() {
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

    // Create roles
    db.catalog.create_role("admin".to_string()).unwrap();
    db.catalog.create_role("user1".to_string()).unwrap();
    db.catalog.create_role("user2".to_string()).unwrap();

    // Grant SELECT to user1 WITH GRANT OPTION
    let grant_stmt1 = GrantStmt {
        privileges: vec![PrivilegeType::Select(None)],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        with_grant_option: true,
    };
    GrantExecutor::execute_grant(&grant_stmt1, &mut db).unwrap();

    // user1 grants SELECT to user2
    let grant_stmt2 = GrantStmt {
        privileges: vec![PrivilegeType::Select(None)],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        for_type_name: None,
        grantees: vec!["user2".to_string()],
        with_grant_option: false,
    };
    db.set_role(Some("user1".to_string()));
    GrantExecutor::execute_grant(&grant_stmt2, &mut db).unwrap();

    // Reset to admin and try to revoke RESTRICT from user1 (should fail)
    db.set_role(Some("admin".to_string()));
    let revoke_stmt = RevokeStmt {
        privileges: vec![PrivilegeType::Select(None)],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        cascade_option: CascadeOption::Restrict,
        grant_option_for: false,
        granted_by: None,
    };

    let result = RevokeExecutor::execute_revoke(&revoke_stmt, &mut db);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("dependent grants exist"));
}

#[test]
fn test_revoke_multiple_grantees() {
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

    // Create roles
    db.catalog.create_role("user1".to_string()).unwrap();
    db.catalog.create_role("user2".to_string()).unwrap();
    db.catalog.create_role("user3".to_string()).unwrap();

    // Grant SELECT to all users
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::Select(None)],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string(), "user2".to_string(), "user3".to_string()],
        with_grant_option: false,
    };
    GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();

    // Verify all have privilege
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Select(None)));
    assert!(db.catalog.has_privilege("user2", "test_table", &PrivilegeType::Select(None)));
    assert!(db.catalog.has_privilege("user3", "test_table", &PrivilegeType::Select(None)));

    // Revoke from multiple grantees at once
    let revoke_stmt = RevokeStmt {
        privileges: vec![PrivilegeType::Select(None)],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string(), "user3".to_string()],
        cascade_option: CascadeOption::Restrict,
        grant_option_for: false,
        granted_by: None,
    };
    RevokeExecutor::execute_revoke(&revoke_stmt, &mut db).unwrap();

    // Verify selective revoke
    assert!(!db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Select(None)));
    assert!(db.catalog.has_privilege("user2", "test_table", &PrivilegeType::Select(None))); // not revoked
    assert!(!db.catalog.has_privilege("user3", "test_table", &PrivilegeType::Select(None)));
}

#[test]
fn test_revoke_non_existent_privilege() {
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

    // Try to revoke privilege that was never granted
    let revoke_stmt = RevokeStmt {
        privileges: vec![PrivilegeType::Select(None)],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        cascade_option: CascadeOption::Restrict,
        grant_option_for: false,
        granted_by: None,
    };
    let result = RevokeExecutor::execute_revoke(&revoke_stmt, &mut db).unwrap();

    // Should succeed (idempotent operation)
    assert!(result.contains("Revoked"));
}

#[test]
fn test_revoke_from_non_existent_role() {
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

    // Try to revoke from non-existent role
    let revoke_stmt = RevokeStmt {
        privileges: vec![PrivilegeType::Select(None)],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        for_type_name: None,
        grantees: vec!["non_existent_role".to_string()],
        cascade_option: CascadeOption::Restrict,
        grant_option_for: false,
        granted_by: None,
    };

    let result = RevokeExecutor::execute_revoke(&revoke_stmt, &mut db);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::RoleNotFound(_)));
}

#[test]
fn test_revoke_from_non_existent_table() {
    let mut db = Database::new();

    // Create schema but no table
    db.catalog.create_schema("test_schema".to_string()).unwrap();

    // Create role
    db.catalog.create_role("user1".to_string()).unwrap();

    // Try to revoke from non-existent table
    let revoke_stmt = RevokeStmt {
        privileges: vec![PrivilegeType::Select(None)],
        object_type: ObjectType::Table,
        object_name: "non_existent_table".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        cascade_option: CascadeOption::Restrict,
        grant_option_for: false,
        granted_by: None,
    };

    let result = RevokeExecutor::execute_revoke(&revoke_stmt, &mut db);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::TableNotFound(_)));
}

#[test]
fn test_revoke_schema_privileges() {
    let mut db = Database::new();

    // Create schema
    db.catalog.create_schema("test_schema".to_string()).unwrap();

    // Create roles
    db.catalog.create_role("user1".to_string()).unwrap();

    // Grant schema privileges
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::Usage, PrivilegeType::Create],
        object_type: ObjectType::Schema,
        object_name: "test_schema".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        with_grant_option: false,
    };
    GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();

    // Verify grants
    assert!(db.catalog.has_privilege("user1", "test_schema", &PrivilegeType::Usage));
    assert!(db.catalog.has_privilege("user1", "test_schema", &PrivilegeType::Create));

    // Revoke schema privileges
    let revoke_stmt = RevokeStmt {
        privileges: vec![PrivilegeType::Usage, PrivilegeType::Create],
        object_type: ObjectType::Schema,
        object_name: "test_schema".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        cascade_option: CascadeOption::Restrict,
        grant_option_for: false,
        granted_by: None,
    };
    RevokeExecutor::execute_revoke(&revoke_stmt, &mut db).unwrap();

    // Verify revokes
    assert!(!db.catalog.has_privilege("user1", "test_schema", &PrivilegeType::Usage));
    assert!(!db.catalog.has_privilege("user1", "test_schema", &PrivilegeType::Create));
}
