//! View DDL executor

use vibesql_ast::{CreateViewStmt, DropViewStmt};
use vibesql_catalog::ViewDefinition;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Executor for view DDL statements
pub struct ViewExecutor;

impl ViewExecutor {
    /// Execute CREATE VIEW or CREATE OR REPLACE VIEW
    pub fn execute_create_view(
        stmt: &CreateViewStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        // If OR REPLACE, drop the view first if it exists
        if stmt.or_replace {
            let view_exists = database.catalog.get_view(&stmt.view_name).is_some();
            if view_exists {
                // Drop the existing view (no cascade needed for OR REPLACE)
                database
                    .catalog
                    .drop_view(&stmt.view_name, false)
                    .map_err(|e| ExecutorError::StorageError(format!("Failed to drop existing view: {:?}", e)))?;
            }
        }

        // Create the view definition
        let view_def = ViewDefinition::new(
            stmt.view_name.clone(),
            stmt.columns.clone(),
            *stmt.query.clone(),
            stmt.with_check_option,
        );

        // Add to catalog
        database
            .catalog
            .create_view(view_def)
            .map_err(|e| ExecutorError::StorageError(format!("Failed to create view: {:?}", e)))?;

        if stmt.or_replace {
            Ok(format!("View '{}' created or replaced", stmt.view_name))
        } else {
            Ok(format!("View '{}' created", stmt.view_name))
        }
    }

    /// Execute DROP VIEW
    pub fn execute_drop_view(
        stmt: &DropViewStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        // Drop the view
        let result = database
            .catalog
            .drop_view(&stmt.view_name, stmt.cascade);

        match result {
            Ok(()) => Ok(format!("View '{}' dropped", stmt.view_name)),
            Err(e) => {
                // If IF EXISTS and view doesn't exist, that's OK
                if stmt.if_exists && matches!(e, vibesql_catalog::errors::CatalogError::ViewNotFound(_)) {
                    Ok(format!("View '{}' does not exist (skipped)", stmt.view_name))
                } else {
                    Err(ExecutorError::StorageError(format!("Failed to drop view: {:?}", e)))
                }
            }
        }
    }
}
