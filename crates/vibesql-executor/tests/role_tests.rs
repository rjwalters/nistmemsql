//! Tests for role management

use vibesql_executor::RoleExecutor;
use vibesql_storage::Database;

#[test]
fn test_create_role_success() {
    let mut db = Database::new();
    let stmt = vibesql_ast::CreateRoleStmt { role_name: "manager".to_string() };

    let result = RoleExecutor::execute_create_role(&stmt, &mut db);
    assert!(result.is_ok());
    assert!(db.catalog.role_exists("manager"));
}

#[test]
fn test_create_role_duplicate() {
    let mut db = Database::new();
    let stmt = vibesql_ast::CreateRoleStmt { role_name: "manager".to_string() };

    // Create first time - should succeed
    RoleExecutor::execute_create_role(&stmt, &mut db).unwrap();

    // Create second time - should fail
    let result = RoleExecutor::execute_create_role(&stmt, &mut db);
    assert!(result.is_err());
}

#[test]
fn test_drop_role_success() {
    let mut db = Database::new();
    let create_stmt = vibesql_ast::CreateRoleStmt { role_name: "manager".to_string() };
    RoleExecutor::execute_create_role(&create_stmt, &mut db).unwrap();

    let drop_stmt = vibesql_ast::DropRoleStmt { role_name: "manager".to_string() };
    let result = RoleExecutor::execute_drop_role(&drop_stmt, &mut db);

    assert!(result.is_ok());
    assert!(!db.catalog.role_exists("manager"));
}

#[test]
fn test_drop_role_not_found() {
    let mut db = Database::new();
    let drop_stmt = vibesql_ast::DropRoleStmt { role_name: "nonexistent".to_string() };

    let result = RoleExecutor::execute_drop_role(&drop_stmt, &mut db);
    assert!(result.is_err());
}

#[test]
fn test_multiple_roles() {
    let mut db = Database::new();

    // Create multiple roles
    let roles = vec!["manager", "analyst", "developer"];
    for role_name in &roles {
        let stmt = vibesql_ast::CreateRoleStmt { role_name: role_name.to_string() };
        RoleExecutor::execute_create_role(&stmt, &mut db).unwrap();
    }

    // Verify all exist
    for role_name in &roles {
        assert!(db.catalog.role_exists(role_name));
    }

    // Drop one
    let drop_stmt = vibesql_ast::DropRoleStmt { role_name: "analyst".to_string() };
    RoleExecutor::execute_drop_role(&drop_stmt, &mut db).unwrap();

    // Verify analyst is gone, others still exist
    assert!(db.catalog.role_exists("manager"));
    assert!(!db.catalog.role_exists("analyst"));
    assert!(db.catalog.role_exists("developer"));
}
