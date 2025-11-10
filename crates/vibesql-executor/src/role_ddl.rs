//! Role DDL executor

use vibesql_ast::*;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Executor for role DDL statements
pub struct RoleExecutor;

impl RoleExecutor {
    /// Execute CREATE ROLE
    pub fn execute_create_role(
        stmt: &CreateRoleStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        database
            .catalog
            .create_role(stmt.role_name.clone())
            .map_err(|e| ExecutorError::StorageError(format!("Catalog error: {:?}", e)))?;
        Ok(format!("Role '{}' created", stmt.role_name))
    }

    /// Execute DROP ROLE
    pub fn execute_drop_role(
        stmt: &DropRoleStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        database
            .catalog
            .drop_role(&stmt.role_name)
            .map_err(|e| ExecutorError::StorageError(format!("Catalog error: {:?}", e)))?;
        Ok(format!("Role '{}' dropped", stmt.role_name))
    }
}
