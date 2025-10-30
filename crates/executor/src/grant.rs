//! GRANT statement executor

use crate::errors::ExecutorError;
use ast::*;
use catalog::PrivilegeGrant;
use storage::Database;

/// Executor for GRANT statements
pub struct GrantExecutor;

impl GrantExecutor {
    /// Execute GRANT statement
    ///
    /// Phase 2.6: Supports validation and conformance
    pub fn execute_grant(
        stmt: &GrantStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        // Validate object exists based on object type
        match stmt.object_type {
            ObjectType::Table => {
                if !database.catalog.table_exists(&stmt.object_name) {
                    return Err(ExecutorError::TableNotFound(stmt.object_name.clone()));
                }
            }
            ObjectType::Schema => {
                if !database.catalog.schema_exists(&stmt.object_name) {
                    return Err(ExecutorError::SchemaNotFound(stmt.object_name.clone()));
                }
            }
        }

        // Validate all grantees (roles) exist
        for grantee in &stmt.grantees {
            if !database.catalog.role_exists(grantee) {
                return Err(ExecutorError::RoleNotFound(grantee.clone()));
            }
        }

        // Expand ALL PRIVILEGES based on object type
        let expanded_privileges = if stmt.privileges.contains(&PrivilegeType::AllPrivileges) {
            match stmt.object_type {
                ObjectType::Table => vec![
                    PrivilegeType::Select,
                    PrivilegeType::Insert,
                    PrivilegeType::Update,
                    PrivilegeType::Delete,
                    PrivilegeType::References,
                ],
                ObjectType::Schema => vec![
                    PrivilegeType::Usage,
                    // Future: add CREATE when schema support is complete
                ],
            }
        } else {
            stmt.privileges.clone()
        };

        // For each grantee and privilege, create a grant
        for grantee in &stmt.grantees {
            for privilege in &expanded_privileges {
                let grant = PrivilegeGrant {
                    object: stmt.object_name.clone(),
                    object_type: stmt.object_type.clone(),
                    privilege: privilege.clone(),
                    grantee: grantee.clone(),
                    grantor: "admin".to_string(), // Default for Phase 2.1
                    with_grant_option: stmt.with_grant_option,
                };
                database.catalog.add_grant(grant);
            }
        }

        let grantees_str = stmt.grantees.join(", ");
        let privileges_str =
            stmt.privileges.iter().map(|p| format!("{:?}", p)).collect::<Vec<_>>().join(", ");

        Ok(format!("Granted {} on {} to {}", privileges_str, stmt.object_name, grantees_str))
    }
}
