use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Centralized privilege checking for all database operations
pub struct PrivilegeChecker;

impl PrivilegeChecker {
    /// Check if current role has SELECT privilege on table
    pub fn check_select(db: &Database, table_name: &str) -> Result<(), ExecutorError> {
        Self::check_privilege(db, table_name, &vibesql_ast::PrivilegeType::Select(None), "SELECT")
    }

    /// Check if current role has INSERT privilege on table
    pub fn check_insert(db: &Database, table_name: &str) -> Result<(), ExecutorError> {
        Self::check_privilege(db, table_name, &vibesql_ast::PrivilegeType::Insert(None), "INSERT")
    }

    /// Check if current role has UPDATE privilege on table
    pub fn check_update(db: &Database, table_name: &str) -> Result<(), ExecutorError> {
        Self::check_privilege(db, table_name, &vibesql_ast::PrivilegeType::Update(None), "UPDATE")
    }

    /// Check if current role has DELETE privilege on table
    pub fn check_delete(db: &Database, table_name: &str) -> Result<(), ExecutorError> {
        Self::check_privilege(db, table_name, &vibesql_ast::PrivilegeType::Delete, "DELETE")
    }

    /// Check if current role has CREATE privilege (for DDL operations)
    pub fn check_create(db: &Database, schema_name: &str) -> Result<(), ExecutorError> {
        // For CREATE TABLE, check privilege on the schema
        Self::check_privilege(db, schema_name, &vibesql_ast::PrivilegeType::Create, "CREATE")
    }

    /// Check if current role has DROP privilege (uses Delete privilege for DDL drops)
    pub fn check_drop(db: &Database, object_name: &str) -> Result<(), ExecutorError> {
        Self::check_privilege(db, object_name, &vibesql_ast::PrivilegeType::Delete, "DROP")
    }

    /// Check if current role has ALTER privilege (uses Create privilege for schema modifications)
    pub fn check_alter(db: &Database, table_name: &str) -> Result<(), ExecutorError> {
        // ALTER TABLE requires CREATE privilege on the table (schema modification)
        Self::check_privilege(db, table_name, &vibesql_ast::PrivilegeType::Create, "ALTER")
    }

    /// Core privilege checking logic
    fn check_privilege(
        db: &Database,
        object: &str,
        privilege: &vibesql_ast::PrivilegeType,
        privilege_name: &str,
    ) -> Result<(), ExecutorError> {
        // Skip checks if security is disabled (for testing)
        if !db.is_security_enabled() {
            return Ok(());
        }

        let role = db.get_current_role();

        // Admin roles bypass all checks
        if role == "ADMIN" || role == "DBA" {
            return Ok(());
        }

        // Check if role has the required privilege
        if db.catalog.has_privilege(&role, object, privilege) {
            Ok(())
        } else {
            Err(ExecutorError::PermissionDenied {
                role,
                privilege: privilege_name.to_string(),
                object: object.to_string(),
            })
        }
    }
}
