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
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::Select(None)],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        with_grant_option: false,
    };
    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();

    // Verify grant
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Select(None)));
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

    // Grant ALL PRIVILEGES on table
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::AllPrivileges],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        with_grant_option: false,
    };
    GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();

    // Verify all table privileges granted
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Select(None)));
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Insert(None)));
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Update(None)));
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Delete));
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::References(None)));
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
        for_type_name: None,
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

    // Grant SELECT with GRANT OPTION
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::Select(None)],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        with_grant_option: true,
    };
    GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();

    // Verify both privilege and grant option granted
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Select(None)));
    let grants = db.catalog.get_grants_for_grantee("user1");
    let grant = grants
        .iter()
        .find(|g| g.object == "test_table" && g.privilege == PrivilegeType::Select(None))
        .unwrap();
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

    // Grant multiple privileges
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::Select(None), PrivilegeType::Insert(None), PrivilegeType::Update(None)],
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

    // Grant to multiple grantees
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::Select(None)],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string(), "user2".to_string(), "user3".to_string()],
        with_grant_option: false,
    };
    GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();

    // Verify all grantees have privilege
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Select(None)));
    assert!(db.catalog.has_privilege("user2", "test_table", &PrivilegeType::Select(None)));
    assert!(db.catalog.has_privilege("user3", "test_table", &PrivilegeType::Select(None)));
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
        for_type_name: None,
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
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(table_schema).unwrap();

    // Try to grant to non-existent role
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::Select(None)],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        for_type_name: None,
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
        privileges: vec![PrivilegeType::Select(None)],
        object_type: ObjectType::Table,
        object_name: "non_existent_table".to_string(),
        for_type_name: None,
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
        for_type_name: None,
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

    // Grant same privilege twice (should be idempotent)
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::Select(None)],
        object_type: ObjectType::Table,
        object_name: "test_table".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        with_grant_option: false,
    };

    GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();
    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();

    // Should succeed both times
    assert!(db.catalog.has_privilege("user1", "test_table", &PrivilegeType::Select(None)));
    assert!(result.contains("Granted"));
}

// ============================================================================
// Function/Procedure Privilege Tests (Issue #678)
// ============================================================================

#[test]
fn test_grant_execute_on_function() {
    let mut db = Database::new();

    // Create role
    db.catalog.create_role("user1".to_string()).unwrap();

    // Grant EXECUTE on function (stub will be auto-created)
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::Execute],
        object_type: ObjectType::Function,
        object_name: "my_func".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();

    // Verify privilege was granted
    assert!(db.catalog.has_privilege("user1", "my_func", &PrivilegeType::Execute));
    assert!(result.contains("Granted"));

    // Verify function stub was created
    assert!(db.catalog.function_exists("my_func"));
}

#[test]
fn test_grant_execute_on_procedure() {
    let mut db = Database::new();

    // Create role
    db.catalog.create_role("user1".to_string()).unwrap();

    // Grant EXECUTE on procedure (stub will be auto-created)
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::Execute],
        object_type: ObjectType::Procedure,
        object_name: "my_proc".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();

    // Verify privilege was granted
    assert!(db.catalog.has_privilege("user1", "my_proc", &PrivilegeType::Execute));
    assert!(result.contains("Granted"));

    // Verify procedure stub was created
    assert!(db.catalog.procedure_exists("my_proc"));
}

#[test]
fn test_grant_execute_on_routine() {
    let mut db = Database::new();

    // Create role
    db.catalog.create_role("user1".to_string()).unwrap();

    // Grant EXECUTE on routine (function stub will be auto-created)
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::Execute],
        object_type: ObjectType::Routine,
        object_name: "my_routine".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();

    // Verify privilege was granted
    assert!(db.catalog.has_privilege("user1", "my_routine", &PrivilegeType::Execute));
    assert!(result.contains("Granted"));

    // Verify function stub was created (routine defaults to function)
    assert!(db.catalog.function_exists("my_routine"));
}

#[test]
fn test_grant_execute_on_method() {
    let mut db = Database::new();

    // Create role
    db.catalog.create_role("user1".to_string()).unwrap();

    // Grant EXECUTE on method (no stub needed, just privilege tracking)
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::Execute],
        object_type: ObjectType::Method,
        object_name: "my_method".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        with_grant_option: false,
    };

    let result = GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();

    // Verify privilege was granted
    assert!(db.catalog.has_privilege("user1", "my_method", &PrivilegeType::Execute));
    assert!(result.contains("Granted"));
}

#[test]
fn test_grant_all_privileges_on_function() {
    let mut db = Database::new();

    // Create role
    db.catalog.create_role("user1".to_string()).unwrap();

    // Grant ALL PRIVILEGES on function (should expand to EXECUTE)
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::AllPrivileges],
        object_type: ObjectType::Function,
        object_name: "my_func".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        with_grant_option: false,
    };

    GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();

    // Verify EXECUTE privilege was granted
    assert!(db.catalog.has_privilege("user1", "my_func", &PrivilegeType::Execute));
}

#[test]
fn test_revoke_execute_from_function() {
    let mut db = Database::new();

    // Create role
    db.catalog.create_role("user1".to_string()).unwrap();

    // Grant EXECUTE privilege first
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::Execute],
        object_type: ObjectType::Function,
        object_name: "my_func".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        with_grant_option: false,
    };
    GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();

    // Verify privilege was granted
    assert!(db.catalog.has_privilege("user1", "my_func", &PrivilegeType::Execute));

    // Revoke EXECUTE privilege
    let revoke_stmt = RevokeStmt {
        grant_option_for: false,
        privileges: vec![PrivilegeType::Execute],
        object_type: ObjectType::Function,
        object_name: "my_func".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        granted_by: None,
        cascade_option: CascadeOption::None,
    };
    let result = RevokeExecutor::execute_revoke(&revoke_stmt, &mut db).unwrap();

    // Verify privilege was revoked
    assert!(!db.catalog.has_privilege("user1", "my_func", &PrivilegeType::Execute));
    assert!(result.contains("Revoked"));
}

#[test]
fn test_revoke_execute_from_procedure() {
    let mut db = Database::new();

    // Create role
    db.catalog.create_role("user1".to_string()).unwrap();

    // Grant EXECUTE privilege first
    let grant_stmt = GrantStmt {
        privileges: vec![PrivilegeType::Execute],
        object_type: ObjectType::Procedure,
        object_name: "my_proc".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        with_grant_option: false,
    };
    GrantExecutor::execute_grant(&grant_stmt, &mut db).unwrap();

    // Verify privilege was granted
    assert!(db.catalog.has_privilege("user1", "my_proc", &PrivilegeType::Execute));

    // Revoke EXECUTE privilege
    let revoke_stmt = RevokeStmt {
        grant_option_for: false,
        privileges: vec![PrivilegeType::Execute],
        object_type: ObjectType::Procedure,
        object_name: "my_proc".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        granted_by: None,
        cascade_option: CascadeOption::None,
    };
    let result = RevokeExecutor::execute_revoke(&revoke_stmt, &mut db).unwrap();

    // Verify privilege was revoked
    assert!(!db.catalog.has_privilege("user1", "my_proc", &PrivilegeType::Execute));
    assert!(result.contains("Revoked"));
}

#[test]
fn test_revoke_from_nonexistent_function_fails() {
    let mut db = Database::new();

    // Create role
    db.catalog.create_role("user1".to_string()).unwrap();

    // Try to revoke from function that doesn't exist
    let revoke_stmt = RevokeStmt {
        grant_option_for: false,
        privileges: vec![PrivilegeType::Execute],
        object_type: ObjectType::Function,
        object_name: "nonexistent_func".to_string(),
        for_type_name: None,
        grantees: vec!["user1".to_string()],
        granted_by: None,
        cascade_option: CascadeOption::None,
    };

    // Should fail with ObjectNotFound error
    let result = RevokeExecutor::execute_revoke(&revoke_stmt, &mut db);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}
