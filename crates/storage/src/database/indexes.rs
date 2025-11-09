// ============================================================================
// Index Manager - User-defined index management (CREATE INDEX statements)
// ============================================================================

use crate::{Row, StorageError};
use ast::IndexColumn;
use std::collections::HashMap;
use types::SqlValue;

/// Normalize an index name to uppercase for case-insensitive comparison
/// This follows SQL standard identifier rules
fn normalize_index_name(name: &str) -> String {
    name.to_uppercase()
}

/// Index metadata
#[derive(Debug, Clone)]
pub struct IndexMetadata {
    pub index_name: String,
    pub table_name: String,
    pub unique: bool,
    pub columns: Vec<IndexColumn>,
}

/// Actual index data structure - maps key values to row indices
#[derive(Debug, Clone)]
pub struct IndexData {
    /// HashMap of (key_values, row_indices) for fast lookups
    /// For multi-column indexes, key_values contains multiple SqlValue entries
    pub data: HashMap<Vec<SqlValue>, Vec<usize>>,
}

/// Manages user-defined indexes (CREATE INDEX statements)
///
/// This component encapsulates all user-defined index operations, maintaining
/// index metadata and data structures for efficient query optimization.
#[derive(Debug, Clone)]
pub struct IndexManager {
    /// Index metadata storage (normalized_index_name -> metadata)
    indexes: HashMap<String, IndexMetadata>,
    /// Actual index data (normalized_index_name -> data)
    index_data: HashMap<String, IndexData>,
}

impl IndexManager {
    /// Create a new empty IndexManager
    pub fn new() -> Self {
        IndexManager { indexes: HashMap::new(), index_data: HashMap::new() }
    }

    /// Create an index
    pub fn create_index(
        &mut self,
        index_name: String,
        table_name: String,
        table_schema: &catalog::TableSchema,
        table_rows: &[Row],
        unique: bool,
        columns: Vec<IndexColumn>,
    ) -> Result<(), StorageError> {
        // Normalize index name for case-insensitive comparison
        let normalized_name = normalize_index_name(&index_name);

        // Check if index already exists
        if self.indexes.contains_key(&normalized_name) {
            return Err(StorageError::IndexAlreadyExists(index_name));
        }

        // Get column indices in the table for all indexed columns
        let mut column_indices = Vec::new();
        for index_col in &columns {
            let column_idx = table_schema
                .get_column_index(&index_col.column_name)
                .ok_or_else(|| StorageError::ColumnNotFound {
                    column_name: index_col.column_name.clone(),
                    table_name: table_name.clone(),
                })?;
            column_indices.push(column_idx);
        }

        // Build the index data
        let mut index_data_map: HashMap<Vec<SqlValue>, Vec<usize>> = HashMap::new();
        for (row_idx, row) in table_rows.iter().enumerate() {
            let key_values: Vec<SqlValue> =
                column_indices.iter().map(|&idx| row.values[idx].clone()).collect();
            index_data_map.entry(key_values).or_insert_with(Vec::new).push(row_idx);
        }

        // Store index metadata (use normalized name as key)
        let metadata =
            IndexMetadata { index_name: index_name.clone(), table_name, unique, columns };

        self.indexes.insert(normalized_name.clone(), metadata);
        self.index_data.insert(normalized_name, IndexData { data: index_data_map });

        Ok(())
    }

    /// Check if an index exists
    pub fn index_exists(&self, index_name: &str) -> bool {
        let normalized = normalize_index_name(index_name);
        self.indexes.contains_key(&normalized)
    }

    /// Get index metadata
    pub fn get_index(&self, index_name: &str) -> Option<&IndexMetadata> {
        let normalized = normalize_index_name(index_name);
        self.indexes.get(&normalized)
    }

    /// Get index data
    pub fn get_index_data(&self, index_name: &str) -> Option<&IndexData> {
        let normalized = normalize_index_name(index_name);
        self.index_data.get(&normalized)
    }

    /// Update user-defined indexes for insert operation
    pub fn update_indexes_for_insert(
        &mut self,
        table_name: &str,
        table_schema: &catalog::TableSchema,
        row: &Row,
        row_index: usize,
    ) {
        for (index_name, metadata) in &self.indexes {
            if metadata.table_name == table_name {
                if let Some(index_data) = self.index_data.get_mut(index_name) {
                    // Build composite key from the indexed columns
                    let key_values: Vec<SqlValue> = metadata
                        .columns
                        .iter()
                        .map(|col| {
                            let col_idx = table_schema
                                .get_column_index(&col.column_name)
                                .expect("Index column should exist");
                            row.values[col_idx].clone()
                        })
                        .collect();

                    // Insert into the index data
                    index_data.data.entry(key_values).or_insert_with(Vec::new).push(row_index);
                }
            }
        }
    }

    /// Update user-defined indexes for update operation
    pub fn update_indexes_for_update(
        &mut self,
        table_name: &str,
        table_schema: &catalog::TableSchema,
        old_row: &Row,
        new_row: &Row,
        row_index: usize,
    ) {
        for (index_name, metadata) in &self.indexes {
            if metadata.table_name == table_name {
                if let Some(index_data) = self.index_data.get_mut(index_name) {
                    // Build keys from old and new rows
                    let old_key_values: Vec<SqlValue> = metadata
                        .columns
                        .iter()
                        .map(|col| {
                            let col_idx = table_schema
                                .get_column_index(&col.column_name)
                                .expect("Index column should exist");
                            old_row.values[col_idx].clone()
                        })
                        .collect();

                    let new_key_values: Vec<SqlValue> = metadata
                        .columns
                        .iter()
                        .map(|col| {
                            let col_idx = table_schema
                                .get_column_index(&col.column_name)
                                .expect("Index column should exist");
                            new_row.values[col_idx].clone()
                        })
                        .collect();

                    // If keys are different, remove old and add new
                    if old_key_values != new_key_values {
                        // Remove old key
                        if let Some(row_indices) = index_data.data.get_mut(&old_key_values) {
                            row_indices.retain(|&idx| idx != row_index);
                            // Remove empty entries
                            if row_indices.is_empty() {
                                index_data.data.remove(&old_key_values);
                            }
                        }

                        // Add new key
                        index_data.data.entry(new_key_values).or_insert_with(Vec::new).push(row_index);
                    }
                    // If keys are the same, no change needed
                }
            }
        }
    }

    /// Update user-defined indexes for delete operation
    pub fn update_indexes_for_delete(
        &mut self,
        table_name: &str,
        table_schema: &catalog::TableSchema,
        row: &Row,
        row_index: usize,
    ) {
        for (index_name, metadata) in &self.indexes {
            if metadata.table_name == table_name {
                if let Some(index_data) = self.index_data.get_mut(index_name) {
                    // Build key from the row
                    let key_values: Vec<SqlValue> = metadata
                        .columns
                        .iter()
                        .map(|col| {
                            let col_idx = table_schema
                                .get_column_index(&col.column_name)
                                .expect("Index column should exist");
                            row.values[col_idx].clone()
                        })
                        .collect();

                    // Remove the row index from this key
                    if let Some(row_indices) = index_data.data.get_mut(&key_values) {
                        row_indices.retain(|&idx| idx != row_index);
                        // Remove empty entries
                        if row_indices.is_empty() {
                            index_data.data.remove(&key_values);
                        }
                    }
                }
            }
        }
    }

    /// Rebuild user-defined indexes after bulk operations that change row indices
    pub fn rebuild_indexes(
        &mut self,
        table_name: &str,
        table_schema: &catalog::TableSchema,
        table_rows: &[Row],
    ) {
        // Collect index names that need rebuilding
        let indexes_to_rebuild: Vec<String> = self
            .indexes
            .iter()
            .filter(|(_, metadata)| metadata.table_name == table_name)
            .map(|(name, _)| name.clone())
            .collect();

        // Rebuild each index
        for index_name in indexes_to_rebuild {
            if let Some(index_data) = self.index_data.get_mut(&index_name) {
                if let Some(metadata) = self.indexes.get(&index_name) {
                    // Clear existing data
                    index_data.data.clear();

                    // Rebuild from current table rows
                    for (row_index, row) in table_rows.iter().enumerate() {
                        let key_values: Vec<SqlValue> = metadata
                            .columns
                            .iter()
                            .map(|col| {
                                let col_idx = table_schema
                                    .get_column_index(&col.column_name)
                                    .expect("Index column should exist");
                                row.values[col_idx].clone()
                            })
                            .collect();

                        index_data.data.entry(key_values).or_insert_with(Vec::new).push(row_index);
                    }
                }
            }
        }
    }

    /// Drop an index
    pub fn drop_index(&mut self, index_name: &str) -> Result<(), StorageError> {
        // Normalize index name for case-insensitive comparison
        let normalized = normalize_index_name(index_name);

        if self.indexes.remove(&normalized).is_none() {
            return Err(StorageError::IndexNotFound(index_name.to_string()));
        }
        // Also remove the index data
        self.index_data.remove(&normalized);
        Ok(())
    }

    /// List all indexes
    pub fn list_indexes(&self) -> Vec<String> {
        self.indexes.keys().cloned().collect()
    }
}

impl Default for IndexManager {
    fn default() -> Self {
        Self::new()
    }
}
