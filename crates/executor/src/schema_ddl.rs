//! Schema DDL executor

use crate::create_table::CreateTableExecutor;
use crate::errors::ExecutorError;
use ast::*;
use storage::Database;

/// Executor for schema DDL statements
pub struct SchemaExecutor;

impl SchemaExecutor {
    /// Execute CREATE SCHEMA
    pub fn execute_create_schema(
        stmt: &CreateSchemaStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        // Create the schema first
        if !stmt.if_not_exists || !database.catalog.schema_exists(&stmt.schema_name) {
            database
                .catalog
                .create_schema(stmt.schema_name.clone())
                .map_err(|e| ExecutorError::StorageError(format!("Catalog error: {:?}", e)))?;
        }

        // Save the current schema and switch to the new schema for element execution
        let original_schema = database.catalog.get_current_schema().to_string();
        database
            .catalog
            .set_current_schema(&stmt.schema_name)
            .map_err(|e| ExecutorError::StorageError(format!("Schema error: {:?}", e)))?;

        // Execute embedded schema elements (CREATE TABLE, etc.)
        let mut element_errors = Vec::new();
        for element in &stmt.schema_elements {
            let result = match element {
                SchemaElement::CreateTable(table_stmt) => {
                    CreateTableExecutor::execute(table_stmt, database)
                }
            };

            if let Err(e) = result {
                element_errors.push(format!("Failed to execute schema element: {}", e));
                // For now, continue executing remaining elements
                // TODO: Implement proper rollback in Phase 3
            }
        }

        // Restore original schema
        database
            .catalog
            .set_current_schema(&original_schema)
            .map_err(|e| ExecutorError::StorageError(format!("Schema error: {:?}", e)))?;

        // Report any errors
        if !element_errors.is_empty() {
            return Err(ExecutorError::StorageError(format!(
                "Schema '{}' created but some elements failed: {}",
                stmt.schema_name,
                element_errors.join("; ")
            )));
        }

        let element_count = stmt.schema_elements.len();
        if element_count > 0 {
            Ok(format!(
                "Schema '{}' created with {} element(s)",
                stmt.schema_name, element_count
            ))
        } else {
            Ok(format!("Schema '{}' created", stmt.schema_name))
        }
    }

    /// Execute DROP SCHEMA
    pub fn execute_drop_schema(
        stmt: &DropSchemaStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        if stmt.if_exists && !database.catalog.schema_exists(&stmt.schema_name) {
            return Ok(format!("Schema '{}' does not exist, skipping", stmt.schema_name));
        }

        database
            .catalog
            .drop_schema(&stmt.schema_name, stmt.cascade)
            .map_err(|e| ExecutorError::StorageError(format!("Catalog error: {:?}", e)))?;
        Ok(format!("Schema '{}' dropped", stmt.schema_name))
    }

    /// Execute SET SCHEMA
    pub fn execute_set_schema(
        stmt: &SetSchemaStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        database
            .catalog
            .set_current_schema(&stmt.schema_name)
            .map_err(|e| ExecutorError::StorageError(format!("Catalog error: {:?}", e)))?;
        Ok(format!("Current schema set to '{}'", stmt.schema_name))
    }
}
