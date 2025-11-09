// ============================================================================
// Database
// ============================================================================

use super::indexes::IndexManager;
use crate::{Row, StorageError, Table};
use ast::IndexColumn;
use std::collections::HashMap;

/// A single change made during a transaction
#[derive(Debug, Clone)]
pub enum TransactionChange {
    Insert { table_name: String, row: Row },
    Update { table_name: String, old_row: Row, new_row: Row },
    Delete { table_name: String, row: Row },
}

/// A savepoint within a transaction
#[derive(Debug, Clone)]
pub struct Savepoint {
    pub name: String,
    /// Index in the changes vector where this savepoint was created
    pub snapshot_index: usize,
}

/// Transaction state
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum TransactionState {
    /// No active transaction
    None,
    /// Transaction is active
    Active {
        /// Transaction ID for debugging
        id: u64,
        /// Original catalog snapshot for full rollback
        original_catalog: catalog::Catalog,
        /// Original table snapshots for full rollback
        original_tables: HashMap<String, Table>,
        /// Stack of savepoints (newest at end)
        savepoints: Vec<Savepoint>,
        /// All changes made since transaction start (for incremental undo)
        changes: Vec<TransactionChange>,
    },
}

/// In-memory database - manages catalog and tables
#[derive(Debug, Clone)]
pub struct Database {
    pub catalog: catalog::Catalog,
    tables: HashMap<String, Table>,
    /// User-defined index manager
    index_manager: IndexManager,
    /// Current transaction state
    transaction_state: TransactionState,
    /// Next transaction ID
    next_transaction_id: u64,
    /// Current session role for privilege checks
    current_role: Option<String>,
    /// Whether security checks are enabled (can be disabled for testing)
    security_enabled: bool,
}

impl Database {
    /// Create a new empty database
    ///
    /// Note: Security is disabled by default for backward compatibility with existing code.
    /// Call `enable_security()` to turn on access control enforcement.
    pub fn new() -> Self {
        Database {
            catalog: catalog::Catalog::new(),
            tables: HashMap::new(),
            index_manager: IndexManager::new(),
            transaction_state: TransactionState::None,
            next_transaction_id: 1,
            current_role: None,
            // Disabled by default for backward compatibility
            security_enabled: false,
        }
    }

    /// Record a change in the current transaction (if any)
    pub fn record_change(&mut self, change: TransactionChange) {
        if let TransactionState::Active { changes, .. } = &mut self.transaction_state {
            changes.push(change);
        }
    }

    /// Create a table
    pub fn create_table(&mut self, schema: catalog::TableSchema) -> Result<(), StorageError> {
        let table_name = schema.name.clone();

        // Add to catalog
        self.catalog
            .create_table(schema.clone())
            .map_err(|e| StorageError::CatalogError(e.to_string()))?;

        // Store with fully qualified name (schema.table)
        let current_schema = self.catalog.get_current_schema();
        let qualified_name = format!("{}.{}", current_schema, table_name);

        // Create empty table
        let table = Table::new(schema);
        self.tables.insert(qualified_name, table);

        Ok(())
    }

    /// Get a table for reading
    pub fn get_table(&self, name: &str) -> Option<&Table> {
        // Try as fully qualified name first
        if let Some(table) = self.tables.get(name) {
            return Some(table);
        }

        // If not found and name is unqualified, try with current schema prefix
        if !name.contains('.') {
            let current_schema = self.catalog.get_current_schema();
            let qualified_name = format!("{}.{}", current_schema, name);
            return self.tables.get(&qualified_name);
        }

        None
    }

    /// Get a table for writing
    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        // Try as fully qualified name first
        if self.tables.contains_key(name) {
            return self.tables.get_mut(name);
        }

        // If not found and name is unqualified, try with current schema prefix
        if !name.contains('.') {
            let current_schema = self.catalog.get_current_schema().to_string();
            let qualified_name = format!("{}.{}", current_schema, name);
            return self.tables.get_mut(&qualified_name);
        }

        None
    }

    /// Drop a table
    pub fn drop_table(&mut self, name: &str) -> Result<(), StorageError> {
        // Remove from catalog
        self.catalog.drop_table(name).map_err(|e| StorageError::CatalogError(e.to_string()))?;

        // Remove table data - try exact name first, then try with schema prefix
        if self.tables.remove(name).is_none() {
            // If not found and name is unqualified, try with current schema prefix
            if !name.contains('.') {
                let current_schema = self.catalog.get_current_schema();
                let qualified_name = format!("{}.{}", current_schema, name);
                self.tables.remove(&qualified_name);
            }
        }

        Ok(())
    }

    /// Insert a row into a table
    pub fn insert_row(&mut self, table_name: &str, row: Row) -> Result<(), StorageError> {
        let table = self
            .get_table_mut(table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;

        // Get row index before insertion
        let row_index = table.row_count();
        table.insert(row.clone())?;

        // Update user-defined indexes
        if let Some(table_schema) = self.catalog.get_table(table_name) {
            self.index_manager.update_indexes_for_insert(table_name, table_schema, &row, row_index);
        }

        // Record the change for transaction rollback
        self.record_change(TransactionChange::Insert { table_name: table_name.to_string(), row });

        Ok(())
    }

    /// List all table names
    pub fn list_tables(&self) -> Vec<String> {
        self.catalog.list_tables()
    }

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

        // Column headers
        let col_names: Vec<String> = table.schema.columns.iter().map(|c| c.name.clone()).collect();
        output.push_str(&format!("{}\n", col_names.join(" | ")));
        output.push_str(&format!("{}\n", "-".repeat(col_names.join(" | ").len())));

        // Rows
        for row in table.scan() {
            let values: Vec<String> = row.values.iter().map(|v| format!("{}", v)).collect();
            output.push_str(&format!("{}\n", values.join(" | ")));
        }

        output.push_str(&format!("({} rows)\n", table.row_count()));
        Ok(output)
    }

    /// Begin a new transaction
    pub fn begin_transaction(&mut self) -> Result<(), StorageError> {
        match self.transaction_state {
            TransactionState::None => {
                // Create snapshots of catalog and all current tables
                let original_catalog = self.catalog.clone();
                let mut original_tables = HashMap::new();
                for (name, table) in &self.tables {
                    original_tables.insert(name.clone(), table.clone());
                }

                let transaction_id = self.next_transaction_id;
                self.next_transaction_id += 1;

                self.transaction_state = TransactionState::Active {
                    id: transaction_id,
                    original_catalog,
                    original_tables,
                    savepoints: Vec::new(),
                    changes: Vec::new(),
                };
                Ok(())
            }
            TransactionState::Active { .. } => {
                Err(StorageError::TransactionError("Transaction already active".to_string()))
            }
        }
    }

    /// Commit the current transaction
    pub fn commit_transaction(&mut self) -> Result<(), StorageError> {
        match self.transaction_state {
            TransactionState::None => {
                Err(StorageError::TransactionError("No active transaction to commit".to_string()))
            }
            TransactionState::Active { .. } => {
                // Transaction committed - just clear the state
                // Changes are already in the tables
                self.transaction_state = TransactionState::None;
                Ok(())
            }
        }
    }

    /// Rollback the current transaction
    pub fn rollback_transaction(&mut self) -> Result<(), StorageError> {
        match &self.transaction_state {
            TransactionState::None => {
                Err(StorageError::TransactionError("No active transaction to rollback".to_string()))
            }
            TransactionState::Active { original_catalog, original_tables, .. } => {
                // Restore catalog and all tables from snapshots
                self.catalog = original_catalog.clone();
                self.tables = original_tables.clone();
                self.transaction_state = TransactionState::None;
                Ok(())
            }
        }
    }

    /// Check if we're currently in a transaction
    pub fn in_transaction(&self) -> bool {
        matches!(self.transaction_state, TransactionState::Active { .. })
    }

    /// Get current transaction ID (for debugging)
    pub fn transaction_id(&self) -> Option<u64> {
        match &self.transaction_state {
            TransactionState::Active { id, .. } => Some(*id),
            TransactionState::None => None,
        }
    }

    /// Create a savepoint within the current transaction
    pub fn create_savepoint(&mut self, name: String) -> Result<(), StorageError> {
        match &mut self.transaction_state {
            TransactionState::None => {
                Err(StorageError::TransactionError("No active transaction".to_string()))
            }
            TransactionState::Active { savepoints, changes, .. } => {
                let savepoint = Savepoint { name, snapshot_index: changes.len() };
                savepoints.push(savepoint);
                Ok(())
            }
        }
    }

    /// Rollback to a named savepoint
    pub fn rollback_to_savepoint(&mut self, name: String) -> Result<(), StorageError> {
        let (_snapshot_index, changes_to_undo) = match &mut self.transaction_state {
            TransactionState::None => {
                return Err(StorageError::TransactionError("No active transaction".to_string()))
            }
            TransactionState::Active { savepoints, changes, .. } => {
                // Find the savepoint
                let savepoint_idx =
                    savepoints.iter().position(|sp| sp.name == name).ok_or_else(|| {
                        StorageError::TransactionError(format!("Savepoint '{}' not found", name))
                    })?;

                let snapshot_index = savepoints[savepoint_idx].snapshot_index;

                // Collect changes to undo (from snapshot_index to end)
                let changes_to_undo: Vec<_> = changes.drain(snapshot_index..).collect();

                // Destroy later savepoints
                savepoints.truncate(savepoint_idx + 1);

                (snapshot_index, changes_to_undo)
            }
        };

        // Undo the changes (now we don't have the mutable borrow conflict)
        for change in changes_to_undo.into_iter().rev() {
            self.undo_change(change)?;
        }

        Ok(())
    }

    /// Release (destroy) a named savepoint
    pub fn release_savepoint(&mut self, name: String) -> Result<(), StorageError> {
        match &mut self.transaction_state {
            TransactionState::None => {
                Err(StorageError::TransactionError("No active transaction".to_string()))
            }
            TransactionState::Active { savepoints, .. } => {
                let savepoint_idx =
                    savepoints.iter().position(|sp| sp.name == name).ok_or_else(|| {
                        StorageError::TransactionError(format!("Savepoint '{}' not found", name))
                    })?;

                // Remove the savepoint
                savepoints.remove(savepoint_idx);

                Ok(())
            }
        }
    }

    /// Undo a single transaction change
    fn undo_change(&mut self, change: TransactionChange) -> Result<(), StorageError> {
        match change {
            TransactionChange::Insert { table_name, row } => {
                // Remove the inserted row
                let table = self
                    .get_table_mut(&table_name)
                    .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;
                table.remove_row(&row)?;
            }
            TransactionChange::Update { table_name, old_row, new_row: _ } => {
                // Restore the old row (this is simplified - real implementation would need row IDs)
                let table = self
                    .get_table_mut(&table_name)
                    .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;
                // For now, just remove the new row and re-insert old (simplified)
                table.remove_row(&old_row)?;
                table.insert(old_row)?;
            }
            TransactionChange::Delete { table_name, row } => {
                // Re-insert the deleted row
                let table = self
                    .get_table_mut(&table_name)
                    .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;
                table.insert(row)?;
            }
        }
        Ok(())
    }

    // ============================================================================
    // Security and Role Management
    // ============================================================================

    /// Set the current session role for privilege checks
    pub fn set_role(&mut self, role: Option<String>) {
        self.current_role = role;
    }

    /// Get the current session role (defaults to "PUBLIC" if not set)
    pub fn get_current_role(&self) -> String {
        self.current_role.clone().unwrap_or_else(|| "PUBLIC".to_string())
    }

    /// Check if security enforcement is enabled
    pub fn is_security_enabled(&self) -> bool {
        self.security_enabled
    }

    /// Disable security checks (for testing)
    pub fn disable_security(&mut self) {
        self.security_enabled = false;
    }

    /// Enable security checks
    pub fn enable_security(&mut self) {
        self.security_enabled = true;
    }

    // ============================================================================
    // Index Management - Delegates to IndexManager
    // ============================================================================

    /// Create an index
    pub fn create_index(
        &mut self,
        index_name: String,
        table_name: String,
        unique: bool,
        columns: Vec<IndexColumn>,
    ) -> Result<(), StorageError> {
        // Get the table to build the index
        let table = self.tables.get(&table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

        // Get the table schema
        let table_schema = self.catalog.get_table(&table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

        // Collect table rows
        let table_rows: Vec<Row> = table.scan().iter().cloned().collect();

        // Delegate to index manager
        self.index_manager.create_index(index_name, table_name, table_schema, &table_rows, unique, columns)
    }

    /// Check if an index exists
    pub fn index_exists(&self, index_name: &str) -> bool {
        self.index_manager.index_exists(index_name)
    }

    /// Get index metadata
    pub fn get_index(&self, index_name: &str) -> Option<&super::indexes::IndexMetadata> {
        self.index_manager.get_index(index_name)
    }

    /// Get index data
    pub fn get_index_data(&self, index_name: &str) -> Option<&super::indexes::IndexData> {
        self.index_manager.get_index_data(index_name)
    }

    /// Update user-defined indexes for update operation
    pub fn update_indexes_for_update(&mut self, table_name: &str, old_row: &Row, new_row: &Row, row_index: usize) {
        if let Some(table_schema) = self.catalog.get_table(table_name) {
            self.index_manager.update_indexes_for_update(table_name, table_schema, old_row, new_row, row_index);
        }
    }

    /// Update user-defined indexes for delete operation
    pub fn update_indexes_for_delete(&mut self, table_name: &str, row: &Row, row_index: usize) {
        if let Some(table_schema) = self.catalog.get_table(table_name) {
            self.index_manager.update_indexes_for_delete(table_name, table_schema, row, row_index);
        }
    }

    /// Rebuild user-defined indexes after bulk operations that change row indices
    pub fn rebuild_indexes(&mut self, table_name: &str) {
        // Get table and schema first to avoid borrow checker issues
        let table_rows: Vec<Row> = if let Some(table) = self.get_table(table_name) {
            table.scan().iter().cloned().collect()
        } else {
            return;
        };

        let table_schema = match self.catalog.get_table(table_name) {
            Some(schema) => schema,
            None => return,
        };

        // Delegate to index manager
        self.index_manager.rebuild_indexes(table_name, table_schema, &table_rows);
    }

    /// Drop an index
    pub fn drop_index(&mut self, index_name: &str) -> Result<(), StorageError> {
        self.index_manager.drop_index(index_name)
    }

    /// List all indexes
    pub fn list_indexes(&self) -> Vec<String> {
        self.index_manager.list_indexes()
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}
