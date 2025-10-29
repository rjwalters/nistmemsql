//! Schema DDL executor

use crate::errors::ExecutorError;
use ast::*;
use storage::Database;

/// Executor for schema DDL statements
pub struct SchemaExecutor;

impl SchemaExecutor {
    /// Execute CREATE SCHEMA
    pub fn execute_create_schema(stmt: &CreateSchemaStmt, database: &mut Database) -> Result<String, ExecutorError> {
        if !stmt.if_not_exists || !database.catalog.schema_exists(&stmt.schema_name) {
            database.catalog.create_schema(stmt.schema_name.clone())
                .map_err(|e| ExecutorError::StorageError(format!("Catalog error: {:?}", e)))?;
        }
        Ok(format!("Schema '{}' created", stmt.schema_name))
    }

    /// Execute DROP SCHEMA
    pub fn execute_drop_schema(stmt: &DropSchemaStmt, database: &mut Database) -> Result<String, ExecutorError> {
        if stmt.if_exists && !database.catalog.schema_exists(&stmt.schema_name) {
            return Ok(format!("Schema '{}' does not exist, skipping", stmt.schema_name));
        }

        database.catalog.drop_schema(&stmt.schema_name, stmt.cascade)
            .map_err(|e| ExecutorError::StorageError(format!("Catalog error: {:?}", e)))?;
        Ok(format!("Schema '{}' dropped", stmt.schema_name))
    }

    /// Execute SET SCHEMA
    pub fn execute_set_schema(stmt: &SetSchemaStmt, database: &mut Database) -> Result<String, ExecutorError> {
        database.catalog.set_current_schema(&stmt.schema_name)
            .map_err(|e| ExecutorError::StorageError(format!("Catalog error: {:?}", e)))?;
        Ok(format!("Current schema set to '{}'", stmt.schema_name))
    }
}
