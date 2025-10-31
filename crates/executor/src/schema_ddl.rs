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
        // Begin transaction for atomic execution
        database.begin_transaction().map_err(|e| {
            ExecutorError::StorageError(format!("Failed to begin transaction: {}", e))
        })?;

        // Execute the schema creation with transaction protection
        let result = Self::execute_create_schema_internal(stmt, database);

        // Commit or rollback based on result
        match result {
            Ok(msg) => {
                database.commit_transaction().map_err(|e| {
                    ExecutorError::StorageError(format!("Failed to commit transaction: {}", e))
                })?;
                Ok(msg)
            }
            Err(e) => {
                database.rollback_transaction().map_err(|rollback_err| {
                    ExecutorError::StorageError(format!(
                        "Failed to rollback transaction after error: {}. Original error: {}",
                        rollback_err, e
                    ))
                })?;
                Err(e)
            }
        }
    }

    /// Internal implementation of CREATE SCHEMA (without transaction management)
    fn execute_create_schema_internal(
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
        for element in &stmt.schema_elements {
            let result = match element {
                SchemaElement::CreateTable(table_stmt) => {
                    CreateTableExecutor::execute(table_stmt, database)
                }
            };

            // On first error, restore original schema and return error
            // Transaction will be rolled back by the outer function
            if let Err(e) = result {
                // Attempt to restore original schema before failing
                let _ = database.catalog.set_current_schema(&original_schema);
                return Err(ExecutorError::StorageError(format!(
                    "Failed to execute schema element: {}",
                    e
                )));
            }
        }

        // Restore original schema
        database
            .catalog
            .set_current_schema(&original_schema)
            .map_err(|e| ExecutorError::StorageError(format!("Schema error: {:?}", e)))?;

        let element_count = stmt.schema_elements.len();
        if element_count > 0 {
            Ok(format!("Schema '{}' created with {} element(s)", stmt.schema_name, element_count))
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

    /// Execute SET CATALOG
    pub fn execute_set_catalog(
        stmt: &ast::SetCatalogStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        database.catalog.set_current_catalog(Some(stmt.catalog_name.clone()));
        Ok(format!("Current catalog set to '{}'", stmt.catalog_name))
    }

    /// Execute SET NAMES
    pub fn execute_set_names(
        stmt: &ast::SetNamesStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        database.catalog.set_current_charset(stmt.charset_name.clone());

        if let Some(ref collation) = stmt.collation {
            database.catalog.set_current_collation(Some(collation.clone()));
            Ok(format!(
                "Character set set to '{}' with collation '{}'",
                stmt.charset_name, collation
            ))
        } else {
            database.catalog.set_current_collation(None);
            Ok(format!("Character set set to '{}'", stmt.charset_name))
        }
    }

    /// Execute SET TIME ZONE
    pub fn execute_set_time_zone(
        stmt: &ast::SetTimeZoneStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        let timezone_str = match &stmt.zone {
            ast::TimeZoneSpec::Local => {
                database.catalog.set_current_timezone("LOCAL".to_string());
                "LOCAL".to_string()
            }
            ast::TimeZoneSpec::Interval(interval) => {
                database.catalog.set_current_timezone(interval.clone());
                interval.clone()
            }
        };

        Ok(format!("Time zone set to '{}'", timezone_str))
    }
}
