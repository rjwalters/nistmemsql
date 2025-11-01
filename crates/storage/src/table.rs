// ============================================================================
// Table
// ============================================================================

use crate::{Row, StorageError};
use std::collections::HashMap;
use types::SqlValue;

/// In-memory table - stores rows
#[derive(Debug, Clone)]
pub struct Table {
    pub schema: catalog::TableSchema,
    rows: Vec<Row>,

    // Hash indexes for constraint validation
    primary_key_index: Option<HashMap<Vec<SqlValue>, usize>>, // Composite key → row index
    unique_indexes: Vec<HashMap<Vec<SqlValue>, usize>>,       // One HashMap per unique constraint
}

impl Table {
    /// Create a new empty table with given schema
    pub fn new(schema: catalog::TableSchema) -> Self {
        let primary_key_index =
            if schema.primary_key.is_some() { Some(HashMap::new()) } else { None };

        let unique_indexes = (0..schema.unique_constraints.len()).map(|_| HashMap::new()).collect();

        Table { schema, rows: Vec::new(), primary_key_index, unique_indexes }
    }

    /// Insert a row into the table
    pub fn insert(&mut self, row: Row) -> Result<(), StorageError> {
        // Validate row has correct number of columns
        if row.len() != self.schema.column_count() {
            return Err(StorageError::ColumnCountMismatch {
                expected: self.schema.column_count(),
                actual: row.len(),
            });
        }

        // TODO: Type checking - verify each value matches column type
        // TODO: NULL checking - verify non-nullable columns have values

        // Normalize row values (e.g., CHAR padding/truncation)
        let normalized_row = self.normalize_row(row);

        // Add row to table
        let row_index = self.rows.len();
        self.rows.push(normalized_row.clone());

        // Update indexes
        self.update_indexes_for_insert(&normalized_row, row_index);

        Ok(())
    }

    /// Normalize row values according to schema
    /// - CHAR: pad with spaces or truncate to fixed length
    /// - Other types: pass through unchanged
    fn normalize_row(&self, mut row: Row) -> Row {
        for (i, column) in self.schema.columns.iter().enumerate() {
            if let Some(value) = row.values.get_mut(i) {
                // Normalize CHAR values
                if let types::DataType::Character { length } = column.data_type {
                    if let types::SqlValue::Character(s) = value {
                        *s = Self::normalize_char_value(s, length);
                    }
                }
            }
        }
        row
    }

    /// Normalize a CHAR value to fixed length
    /// - Pad with spaces if too short
    /// - Truncate if too long
    fn normalize_char_value(value: &str, length: usize) -> String {
        use std::cmp::Ordering;
        match value.len().cmp(&length) {
            Ordering::Less => {
                // Pad with spaces to the right
                format!("{:width$}", value, width = length)
            }
            Ordering::Greater => {
                // Truncate to fixed length
                value[..length].to_string()
            }
            Ordering::Equal => {
                // Exact length - no change needed
                value.to_string()
            }
        }
    }

    /// Get all rows (for scanning)
    pub fn scan(&self) -> &[Row] {
        &self.rows
    }

    /// Get number of rows
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Clear all rows
    pub fn clear(&mut self) {
        self.rows.clear();
        // Clear indexes
        if let Some(ref mut pk_index) = self.primary_key_index {
            pk_index.clear();
        }
        for unique_index in &mut self.unique_indexes {
            unique_index.clear();
        }
    }

    /// Update a row at the specified index
    pub fn update_row(&mut self, index: usize, row: Row) -> Result<(), StorageError> {
        if index >= self.rows.len() {
            return Err(StorageError::ColumnIndexOutOfBounds { index });
        }

        // Validate row has correct number of columns
        if row.len() != self.schema.column_count() {
            return Err(StorageError::ColumnCountMismatch {
                expected: self.schema.column_count(),
                actual: row.len(),
            });
        }

        // Normalize row values (e.g., CHAR padding/truncation)
        let normalized_row = self.normalize_row(row);

        // Get old row for index updates (clone to avoid borrow issues)
        let old_row = self.rows[index].clone();

        // Update the row
        self.rows[index] = normalized_row.clone();

        // Update indexes
        self.update_indexes_for_update(&old_row, &normalized_row, index);

        Ok(())
    }

    /// Delete rows matching a predicate
    /// Returns number of rows deleted
    pub fn delete_where<F>(&mut self, mut predicate: F) -> usize
    where
        F: FnMut(&Row) -> bool,
    {
        // Collect indices and rows to delete (only call predicate once per row)
        let mut indices_and_rows_to_delete: Vec<(usize, Row)> = Vec::new();
        for (index, row) in self.rows.iter().enumerate() {
            if predicate(row) {
                indices_and_rows_to_delete.push((index, row.clone()));
            }
        }

        // Delete rows in reverse order to maintain correct indices
        for (index, _) in indices_and_rows_to_delete.iter().rev() {
            self.rows.remove(*index);
        }

        // Update indexes for deleted rows
        for (_, deleted_row) in &indices_and_rows_to_delete {
            self.update_indexes_for_delete(deleted_row, 0); // row_index not used in delete
        }

        // Since rows shifted, we need to rebuild indexes to maintain correct indices
        self.rebuild_indexes();

        indices_and_rows_to_delete.len()
    }

    /// Remove a specific row (used for transaction undo)
    /// Returns error if row not found
    pub fn remove_row(&mut self, target_row: &Row) -> Result<(), StorageError> {
        // Find and remove the first matching row
        if let Some(pos) = self.rows.iter().position(|row| row == target_row) {
            // Update indexes before removing
            self.update_indexes_for_delete(target_row, pos);
            self.rows.remove(pos);
            // Rebuild indexes since row indices changed
            self.rebuild_indexes();
            Ok(())
        } else {
            Err(StorageError::RowNotFound)
        }
    }

    /// Get mutable reference to rows
    pub fn rows_mut(&mut self) -> &mut Vec<Row> {
        &mut self.rows
    }

    /// Get mutable reference to schema
    pub fn schema_mut(&mut self) -> &mut catalog::TableSchema {
        &mut self.schema
    }

    /// Get reference to primary key index
    pub fn primary_key_index(&self) -> Option<&HashMap<Vec<SqlValue>, usize>> {
        self.primary_key_index.as_ref()
    }

    /// Get reference to unique constraint indexes
    pub fn unique_indexes(&self) -> &[HashMap<Vec<SqlValue>, usize>] {
        &self.unique_indexes
    }

    /// Update hash indexes when inserting a row
    fn update_indexes_for_insert(&mut self, row: &Row, row_index: usize) {
        // Update primary key index
        if let Some(ref mut pk_index) = self.primary_key_index {
            if let Some(pk_indices) = self.schema.get_primary_key_indices() {
                let pk_values: Vec<SqlValue> =
                    pk_indices.iter().map(|&idx| row.values[idx].clone()).collect();
                pk_index.insert(pk_values, row_index);
            }
        }

        // Update unique constraint indexes
        let unique_constraint_indices = self.schema.get_unique_constraint_indices();
        for (constraint_idx, unique_indices) in unique_constraint_indices.iter().enumerate() {
            let unique_values: Vec<SqlValue> =
                unique_indices.iter().map(|&idx| row.values[idx].clone()).collect();

            // Skip if any value in the unique constraint is NULL
            if unique_values.iter().any(|v| *v == SqlValue::Null) {
                continue;
            }

            if let Some(unique_index) = self.unique_indexes.get_mut(constraint_idx) {
                unique_index.insert(unique_values, row_index);
            }
        }
    }

    /// Update hash indexes when updating a row
    fn update_indexes_for_update(&mut self, old_row: &Row, new_row: &Row, row_index: usize) {
        // Update primary key index
        if let Some(ref mut pk_index) = self.primary_key_index {
            if let Some(pk_indices) = self.schema.get_primary_key_indices() {
                let old_pk_values: Vec<SqlValue> =
                    pk_indices.iter().map(|&idx| old_row.values[idx].clone()).collect();
                let new_pk_values: Vec<SqlValue> =
                    pk_indices.iter().map(|&idx| new_row.values[idx].clone()).collect();

                // Remove old key if different from new key
                if old_pk_values != new_pk_values {
                    pk_index.remove(&old_pk_values);
                    pk_index.insert(new_pk_values, row_index);
                }
            }
        }

        // Update unique constraint indexes
        let unique_constraint_indices = self.schema.get_unique_constraint_indices();
        for (constraint_idx, unique_indices) in unique_constraint_indices.iter().enumerate() {
            let old_unique_values: Vec<SqlValue> =
                unique_indices.iter().map(|&idx| old_row.values[idx].clone()).collect();
            let new_unique_values: Vec<SqlValue> =
                unique_indices.iter().map(|&idx| new_row.values[idx].clone()).collect();

            if let Some(unique_index) = self.unique_indexes.get_mut(constraint_idx) {
                // Remove old key if it's different and not NULL
                if old_unique_values != new_unique_values
                    && !old_unique_values.iter().any(|v| *v == SqlValue::Null)
                {
                    unique_index.remove(&old_unique_values);
                }

                // Insert new key if not NULL
                if !new_unique_values.iter().any(|v| *v == SqlValue::Null) {
                    unique_index.insert(new_unique_values, row_index);
                }
            }
        }
    }

    /// Update hash indexes when deleting rows
    fn update_indexes_for_delete(&mut self, row: &Row, _row_index: usize) {
        // Update primary key index
        if let Some(ref mut pk_index) = self.primary_key_index {
            if let Some(pk_indices) = self.schema.get_primary_key_indices() {
                let pk_values: Vec<SqlValue> =
                    pk_indices.iter().map(|&idx| row.values[idx].clone()).collect();
                pk_index.remove(&pk_values);
            }
        }

        // Update unique constraint indexes
        let unique_constraint_indices = self.schema.get_unique_constraint_indices();
        for (constraint_idx, unique_indices) in unique_constraint_indices.iter().enumerate() {
            let unique_values: Vec<SqlValue> =
                unique_indices.iter().map(|&idx| row.values[idx].clone()).collect();

            // Skip if any value in the unique constraint is NULL
            if unique_values.iter().any(|v| *v == SqlValue::Null) {
                continue;
            }

            if let Some(unique_index) = self.unique_indexes.get_mut(constraint_idx) {
                unique_index.remove(&unique_values);
            }
        }
    }

    /// Rebuild all hash indexes from scratch (used after bulk operations that change row indices)
    fn rebuild_indexes(&mut self) {
        // Clear existing indexes
        if let Some(ref mut pk_index) = self.primary_key_index {
            pk_index.clear();
        }
        for unique_index in &mut self.unique_indexes {
            unique_index.clear();
        }

        // Rebuild from current rows (collect to avoid borrow issues)
        let rows_with_indices: Vec<(Row, usize)> =
            self.rows.iter().enumerate().map(|(idx, row)| (row.clone(), idx)).collect();

        for (row, row_index) in rows_with_indices {
            self.update_indexes_for_insert(&row, row_index);
        }
    }
}
