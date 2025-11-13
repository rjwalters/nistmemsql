// ============================================================================
// Database - Coordinates between focused modules
// ============================================================================

use super::lifecycle::Lifecycle;
use super::metadata::Metadata;
use super::operations::{Operations, SpatialIndexMetadata};
use super::transactions::TransactionChange;
use crate::{Row, StorageError, Table};
use std::collections::HashMap;
use vibesql_ast::IndexColumn;

pub use super::operations::SpatialIndexMetadata as ExportedSpatialIndexMetadata;

/// In-memory database - manages catalog and tables through focused modules
#[derive(Debug, Clone)]
pub struct Database {
    /// Public catalog access for backward compatibility
    pub catalog: vibesql_catalog::Catalog,
    lifecycle: Lifecycle,
    metadata: Metadata,
    operations: Operations,
    pub tables: HashMap<String, Table>,
}

impl Database {
    /// Create a new empty database
    ///
    /// Note: Security is disabled by default for backward compatibility with existing code.
    /// Call `enable_security()` to turn on access control enforcement.
    pub fn new() -> Self {
        Database {
            catalog: vibesql_catalog::Catalog::new(),
            lifecycle: Lifecycle::new(),
            metadata: Metadata::new(),
            operations: Operations::new(),
            tables: HashMap::new(),
        }
    }

    /// Reset the database to empty state (more efficient than creating a new instance).
    ///
    /// Clears all tables, resets catalog to default state, and clears all indexes and transactions.
    /// Useful for test scenarios where you need to reuse a Database instance.
    pub fn reset(&mut self) {
        self.catalog = vibesql_catalog::Catalog::new();
        self.lifecycle.reset();
        self.metadata = Metadata::new();
        self.operations = Operations::new();
        self.tables.clear();
    }

    // ============================================================================
    // Transaction Management
    // ============================================================================

    /// Record a change in the current transaction (if any)
    pub fn record_change(&mut self, change: TransactionChange) {
        self.lifecycle.transaction_manager_mut().record_change(change);
    }

    /// Begin a new transaction
    pub fn begin_transaction(&mut self) -> Result<(), StorageError> {
        let catalog = &self.catalog.clone();
        self.lifecycle
            .transaction_manager_mut()
            .begin_transaction(&catalog, &self.tables)
    }

    /// Commit the current transaction
    pub fn commit_transaction(&mut self) -> Result<(), StorageError> {
        self.lifecycle.transaction_manager_mut().commit_transaction()
    }

    /// Rollback the current transaction
    pub fn rollback_transaction(&mut self) -> Result<(), StorageError> {
        self.lifecycle.perform_rollback(&mut self.catalog)
    }

    /// Check if we're currently in a transaction
    pub fn in_transaction(&self) -> bool {
        self.lifecycle.transaction_manager().in_transaction()
    }

    /// Get current transaction ID (for debugging)
    pub fn transaction_id(&self) -> Option<u64> {
        self.lifecycle.transaction_manager().transaction_id()
    }

    /// Create a savepoint within the current transaction
    pub fn create_savepoint(&mut self, name: String) -> Result<(), StorageError> {
        self.lifecycle.transaction_manager_mut().create_savepoint(name)
    }

    /// Rollback to a named savepoint
    pub fn rollback_to_savepoint(&mut self, name: String) -> Result<(), StorageError> {
        let changes_to_undo = self.lifecycle.transaction_manager_mut().rollback_to_savepoint(name)?;

        for change in changes_to_undo.into_iter().rev() {
            self.undo_change(change)?;
        }

        Ok(())
    }

    /// Undo a single transaction change
    fn undo_change(&mut self, change: TransactionChange) -> Result<(), StorageError> {
        match change {
            TransactionChange::Insert { table_name, row } => {
                let table = self
                    .get_table_mut(&table_name)
                    .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;
                table.remove_row(&row)?;
            }
            TransactionChange::Update { table_name, old_row, new_row: _ } => {
                let table = self
                    .get_table_mut(&table_name)
                    .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;
                table.remove_row(&old_row)?;
                table.insert(old_row)?;
            }
            TransactionChange::Delete { table_name, row } => {
                let table = self
                    .get_table_mut(&table_name)
                    .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;
                table.insert(row)?;
            }
        }
        Ok(())
    }

    /// Release (destroy) a named savepoint
    pub fn release_savepoint(&mut self, name: String) -> Result<(), StorageError> {
        self.lifecycle.transaction_manager_mut().release_savepoint(name)
    }

    // ============================================================================
    // Table Operations
    // ============================================================================

    /// Create a table
    pub fn create_table(
        &mut self,
        schema: vibesql_catalog::TableSchema,
    ) -> Result<(), StorageError> {
        let table_name = schema.name.clone();

        self.operations
            .create_table(&mut self.catalog, schema.clone())?;

        // Normalize table name for storage (matches catalog normalization)
        let normalized_table_name = if self.catalog.is_case_sensitive_identifiers() {
            table_name.clone()
        } else {
            table_name.to_uppercase()
        };

        let current_schema = &self.catalog.get_current_schema();
        let qualified_name = format!("{}.{}", current_schema, normalized_table_name);

        let table = Table::new(schema);
        self.tables.insert(qualified_name, table);

        Ok(())
    }

    /// Get a table for reading
    pub fn get_table(&self, name: &str) -> Option<&Table> {
        // Normalize table name for lookup (matches catalog normalization)
        let normalized_name = if self.catalog.is_case_sensitive_identifiers() {
            name.to_string()
        } else {
            name.to_uppercase()
        };

        if let Some(table) = self.tables.get(&normalized_name) {
            return Some(table);
        }

        if !name.contains('.') {
            let current_schema = &self.catalog.get_current_schema();
            let qualified_name = format!("{}.{}", current_schema, normalized_name);
            return self.tables.get(&qualified_name);
        }

        None
    }

    /// Get a table for writing
    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        // Normalize table name for lookup (matches catalog normalization)
        let normalized_name = if self.catalog.is_case_sensitive_identifiers() {
            name.to_string()
        } else {
            name.to_uppercase()
        };

        if self.tables.contains_key(&normalized_name) {
            return self.tables.get_mut(&normalized_name);
        }

        if !name.contains('.') {
            let current_schema = &self.catalog.get_current_schema().to_string();
            let qualified_name = format!("{}.{}", current_schema, normalized_name);
            return self.tables.get_mut(&qualified_name);
        }

        None
    }

    /// Drop a table
    pub fn drop_table(&mut self, name: &str) -> Result<(), StorageError> {
        self.operations.drop_table(&mut self.catalog, &mut self.tables, name)
    }

    /// Insert a row into a table
    pub fn insert_row(&mut self, table_name: &str, row: Row) -> Result<(), StorageError> {
        let _row_index = self.operations.insert_row(
            &self.catalog,
            &mut self.tables,
            table_name,
            row.clone(),
        )?;

        self.record_change(TransactionChange::Insert {
            table_name: table_name.to_string(),
            row,
        });

        Ok(())
    }

    /// List all table names
    pub fn list_tables(&self) -> Vec<String> {
        self.catalog.list_tables()
    }

    // ============================================================================
    // Debug and Utilities
    // ============================================================================

    /// Get debug information about database state
    pub fn debug_info(&self) -> String {
        let mut output = String::new();
        output.push_str("=== Database Debug Info ===\n");
        output.push_str(&format!("Tables: {}\n", self.list_tables().len()));
        for table_name in self.list_tables() {
            if let Some(table) = self.get_table(&table_name) {
                output.push_str(&format!(
                    "  - {} ({} rows, {} columns)\n",
                    table_name,
                    table.row_count(),
                    table.schema.column_count()
                ));
            }
        }
        output
    }

    /// Dump all table contents in readable format
    pub fn dump_tables(&self) -> String {
        let mut output = String::new();
        for table_name in self.list_tables() {
            if let Ok(dump) = self.dump_table(&table_name) {
                output.push_str(&dump);
                output.push('\n');
            }
        }
        output
    }

    /// Dump a specific table's contents
    pub fn dump_table(&self, name: &str) -> Result<String, StorageError> {
        let table =
            self.get_table(name).ok_or_else(|| StorageError::TableNotFound(name.to_string()))?;

        let mut output = String::new();
        output.push_str(&format!("=== Table: {} ===\n", name));

        let col_names: Vec<String> = table.schema.columns.iter().map(|c| c.name.clone()).collect();
        output.push_str(&format!("{}\n", col_names.join(" | ")));
        output.push_str(&format!("{}\n", "-".repeat(col_names.join(" | ").len())));

        for row in table.scan() {
            let values: Vec<String> = row.values.iter().map(|v| format!("{}", v)).collect();
            output.push_str(&format!("{}\n", values.join(" | ")));
        }

        output.push_str(&format!("({} rows)\n", table.row_count()));
        Ok(output)
    }

    // ============================================================================
    // Security and Role Management
    // ============================================================================

    /// Set the current session role for privilege checks
    pub fn set_role(&mut self, role: Option<String>) {
        self.lifecycle.set_role(role);
    }

    /// Get the current session role (defaults to "PUBLIC" if not set)
    pub fn get_current_role(&self) -> String {
        self.lifecycle
            .current_role()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "PUBLIC".to_string())
    }

    /// Check if security enforcement is enabled
    pub fn is_security_enabled(&self) -> bool {
        self.lifecycle.is_security_enabled()
    }

    /// Disable security checks (for testing)
    pub fn disable_security(&mut self) {
        self.lifecycle.disable_security();
    }

    /// Enable security checks
    pub fn enable_security(&mut self) {
        self.lifecycle.enable_security();
    }

    // ============================================================================
    // Session Variables
    // ============================================================================

    /// Set a session variable (MySQL-style @variable)
    pub fn set_session_variable(&mut self, name: &str, value: vibesql_types::SqlValue) {
        self.metadata.set_session_variable(name, value);
    }

    /// Get a session variable value
    pub fn get_session_variable(&self, name: &str) -> Option<&vibesql_types::SqlValue> {
        self.metadata.get_session_variable(name)
    }

    /// Clear all session variables
    pub fn clear_session_variables(&mut self) {
        self.metadata.clear_session_variables();
    }

    // ============================================================================
    // Index Management
    // ============================================================================

    /// Create an index
    pub fn create_index(
        &mut self,
        index_name: String,
        table_name: String,
        unique: bool,
        columns: Vec<IndexColumn>,
    ) -> Result<(), StorageError> {
        self.operations.create_index(
            &self.catalog,
            &self.tables,
            index_name,
            table_name,
            unique,
            columns,
        )
    }

    /// Check if an index exists
    pub fn index_exists(&self, index_name: &str) -> bool {
        self.operations.index_exists(index_name)
    }

    /// Get index metadata
    pub fn get_index(&self, index_name: &str) -> Option<&super::indexes::IndexMetadata> {
        self.operations.get_index(index_name)
    }

    /// Get index data
    pub fn get_index_data(&self, index_name: &str) -> Option<&super::indexes::IndexData> {
        self.operations.get_index_data(index_name)
    }

    /// Update user-defined indexes for update operation
    pub fn update_indexes_for_update(
        &mut self,
        table_name: &str,
        old_row: &Row,
        new_row: &Row,
        row_index: usize,
    ) {
        self.operations.update_indexes_for_update(
            &self.catalog,
            table_name,
            old_row,
            new_row,
            row_index,
        );
    }

    /// Update user-defined indexes for delete operation
    pub fn update_indexes_for_delete(&mut self, table_name: &str, row: &Row, row_index: usize) {
        self.operations
            .update_indexes_for_delete(&self.catalog, table_name, row, row_index);
    }

    /// Rebuild user-defined indexes after bulk operations that change row indices
    pub fn rebuild_indexes(&mut self, table_name: &str) {
        self.operations
            .rebuild_indexes(&self.catalog, &self.tables, table_name);
    }

    /// Drop an index
    pub fn drop_index(&mut self, index_name: &str) -> Result<(), StorageError> {
        self.operations.drop_index(index_name)
    }

    /// List all indexes
    pub fn list_indexes(&self) -> Vec<String> {
        self.operations.list_indexes()
    }

    /// List all indexes for a specific table
    pub fn list_indexes_for_table(&self, table_name: &str) -> Vec<String> {
        self.operations.list_indexes_for_table(table_name)
    }

    // ============================================================================
    // Spatial Index Methods
    // ============================================================================

    /// Create a spatial index
    pub fn create_spatial_index(
        &mut self,
        metadata: SpatialIndexMetadata,
        spatial_index: crate::index::SpatialIndex,
    ) -> Result<(), StorageError> {
        self.operations.create_spatial_index(metadata, spatial_index)
    }

    /// Check if a spatial index exists
    pub fn spatial_index_exists(&self, index_name: &str) -> bool {
        self.operations.spatial_index_exists(index_name)
    }

    /// Get spatial index metadata
    pub fn get_spatial_index_metadata(&self, index_name: &str) -> Option<&SpatialIndexMetadata> {
        self.operations.get_spatial_index_metadata(index_name)
    }

    /// Get spatial index (immutable)
    pub fn get_spatial_index(&self, index_name: &str) -> Option<&crate::index::SpatialIndex> {
        self.operations.get_spatial_index(index_name)
    }

    /// Get spatial index (mutable)
    pub fn get_spatial_index_mut(&mut self, index_name: &str) -> Option<&mut crate::index::SpatialIndex> {
        self.operations.get_spatial_index_mut(index_name)
    }

    /// Get all spatial indexes for a specific table
    pub fn get_spatial_indexes_for_table(
        &self,
        table_name: &str,
    ) -> Vec<(&SpatialIndexMetadata, &crate::index::SpatialIndex)> {
        self.operations.get_spatial_indexes_for_table(table_name)
    }

    /// Get all spatial indexes for a specific table (mutable)
    pub fn get_spatial_indexes_for_table_mut(
        &mut self,
        table_name: &str,
    ) -> Vec<(&SpatialIndexMetadata, &mut crate::index::SpatialIndex)> {
        self.operations.get_spatial_indexes_for_table_mut(table_name)
    }

    /// Drop a spatial index
    pub fn drop_spatial_index(&mut self, index_name: &str) -> Result<(), StorageError> {
        self.operations.drop_spatial_index(index_name)
    }

    /// Drop all spatial indexes associated with a table (CASCADE behavior)
    pub fn drop_spatial_indexes_for_table(&mut self, table_name: &str) -> Vec<String> {
        self.operations.drop_spatial_indexes_for_table(table_name)
    }

    /// List all spatial indexes
    pub fn list_spatial_indexes(&self) -> Vec<String> {
        self.operations.list_spatial_indexes()
    }

    // ============================================================================
    // Procedure/Function Body Cache Methods (Phase 6 Performance)
    // ============================================================================

    /// Get cached procedure body or cache it on first access
    pub fn get_cached_procedure_body(
        &mut self,
        name: &str,
    ) -> Result<&vibesql_catalog::ProcedureBody, StorageError> {
        if !self.metadata.get_cached_procedure_body(name).is_some() {
            let procedure = &self.catalog.get_procedure(name).ok_or_else(|| {
                StorageError::CatalogError(format!("Procedure '{}' not found", name))
            })?;

            self.metadata.cache_procedure_body(name.to_string(), procedure.body.clone());
        }

        Ok(self.metadata.get_cached_procedure_body(name).unwrap())
    }

    /// Invalidate cached procedure body (call when procedure is dropped or replaced)
    pub fn invalidate_procedure_cache(&mut self, name: &str) {
        self.metadata.invalidate_procedure_cache(name);
    }

    /// Clear all cached procedure/function bodies
    pub fn clear_routine_cache(&mut self) {
        self.metadata.clear_routine_cache();
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}
