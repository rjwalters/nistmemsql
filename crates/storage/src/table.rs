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

    // Append mode optimization tracking
    last_pk_value: Option<Vec<SqlValue>>,  // Last inserted primary key value
    append_mode: bool,                     // True when detecting sequential inserts
    append_streak: usize,                  // Count of consecutive sequential inserts
}

impl Table {
    /// Create a new empty table with given schema
    pub fn new(schema: catalog::TableSchema) -> Self {
        let primary_key_index =
            if schema.primary_key.is_some() { Some(HashMap::new()) } else { None };

        let unique_indexes = (0..schema.unique_constraints.len()).map(|_| HashMap::new()).collect();

        Table {
            schema,
            rows: Vec::new(),
            primary_key_index,
            unique_indexes,
            last_pk_value: None,
            append_mode: false,
            append_streak: 0,
        }
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

        // Detect sequential append pattern before inserting
        if let Some(pk_indices) = self.schema.get_primary_key_indices() {
            let pk_values: Vec<SqlValue> =
                pk_indices.iter().map(|&idx| normalized_row.values[idx].clone()).collect();
            self.update_append_mode(&pk_values);
        }

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

    /// Update append mode tracking based on current primary key value
    /// Detects sequential inserts and enables append mode after a threshold
    fn update_append_mode(&mut self, pk_values: &[SqlValue]) {
        if let Some(last_pk) = &self.last_pk_value {
            // Check if current PK is greater than last PK (sequential)
            if pk_values > last_pk.as_slice() {
                self.append_streak += 1;
                // Enable append mode after 3 consecutive sequential inserts
                if self.append_streak >= 3 {
                    self.append_mode = true;
                }
            } else {
                // Non-sequential insert - reset append mode
                self.append_mode = false;
                self.append_streak = 0;
            }
        }
        // Update last PK value for next comparison
        self.last_pk_value = Some(pk_values.to_vec());
    }

    /// Get all rows (for scanning)
    pub fn scan(&self) -> &[Row] {
        &self.rows
    }

    /// Get number of rows
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Check if table is in append mode (sequential inserts detected)
    /// When true, constraint checks can skip duplicate lookups for optimization
    pub fn is_in_append_mode(&self) -> bool {
        self.append_mode
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
        // Reset append mode tracking
        self.last_pk_value = None;
        self.append_mode = false;
        self.append_streak = 0;
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

#[cfg(test)]
mod tests {
    use super::*;
    use catalog::{ColumnSchema, TableSchema};
    use types::{DataType, SqlValue};

    fn create_test_table() -> Table {
        let columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(50) }, true),
        ];
        let schema = TableSchema::with_primary_key(
            "test_table".to_string(),
            columns,
            vec!["id".to_string()],
        );
        Table::new(schema)
    }

    fn create_row(id: i64, name: &str) -> Row {
        Row {
            values: vec![SqlValue::Integer(id), SqlValue::Varchar(name.to_string())],
        }
    }

    #[test]
    fn test_append_mode_detection() {
        let mut table = create_test_table();

        // Insert sequential values
        table.insert(create_row(1, "Alice")).unwrap();
        assert_eq!(table.append_streak, 0); // First insert, no streak yet
        assert!(!table.append_mode);

        table.insert(create_row(2, "Bob")).unwrap();
        assert_eq!(table.append_streak, 1); // Second sequential
        assert!(!table.append_mode); // Not yet at threshold

        table.insert(create_row(3, "Charlie")).unwrap();
        assert_eq!(table.append_streak, 2); // Third sequential
        assert!(!table.append_mode); // Not yet at threshold

        table.insert(create_row(4, "David")).unwrap();
        assert_eq!(table.append_streak, 3); // Fourth sequential
        assert!(table.append_mode); // NOW append mode is active (threshold = 3)
    }

    #[test]
    fn test_append_mode_reset_on_non_sequential() {
        let mut table = create_test_table();

        // Build up append mode
        table.insert(create_row(1, "Alice")).unwrap();
        table.insert(create_row(2, "Bob")).unwrap();
        table.insert(create_row(3, "Charlie")).unwrap();
        table.insert(create_row(4, "David")).unwrap();
        assert!(table.append_mode);
        assert_eq!(table.append_streak, 3);

        // Insert non-sequential value (goes backward)
        table.insert(create_row(2, "Eve")).unwrap(); // 2 < 4, non-sequential
        assert!(!table.append_mode); // Append mode reset
        assert_eq!(table.append_streak, 0); // Streak reset
    }

    #[test]
    fn test_append_mode_still_indexes_rows() {
        let mut table = create_test_table();

        // Enter append mode
        table.insert(create_row(1, "Alice")).unwrap();
        table.insert(create_row(2, "Bob")).unwrap();
        table.insert(create_row(3, "Charlie")).unwrap();
        table.insert(create_row(4, "David")).unwrap();
        assert!(table.append_mode);

        // Verify all rows are indexed
        let pk_index = table.primary_key_index().unwrap();
        assert!(pk_index.contains_key(&vec![SqlValue::Integer(1)]));
        assert!(pk_index.contains_key(&vec![SqlValue::Integer(2)]));
        assert!(pk_index.contains_key(&vec![SqlValue::Integer(3)]));
        assert!(pk_index.contains_key(&vec![SqlValue::Integer(4)]));
    }

    #[test]
    fn test_append_mode_clear_resets() {
        let mut table = create_test_table();

        // Enter append mode
        table.insert(create_row(1, "Alice")).unwrap();
        table.insert(create_row(2, "Bob")).unwrap();
        table.insert(create_row(3, "Charlie")).unwrap();
        table.insert(create_row(4, "David")).unwrap();
        assert!(table.append_mode);
        assert_eq!(table.append_streak, 3);

        // Clear table
        table.clear();

        // Verify append mode tracking is reset
        assert!(!table.append_mode);
        assert_eq!(table.append_streak, 0);
        assert_eq!(table.last_pk_value, None);
    }

    #[test]
    fn test_duplicate_caught_after_append_mode() {
        let mut table = create_test_table();

        // Enter append mode
        table.insert(create_row(1, "Alice")).unwrap();
        table.insert(create_row(2, "Bob")).unwrap();
        table.insert(create_row(3, "Charlie")).unwrap();
        table.insert(create_row(4, "David")).unwrap();
        assert!(table.append_mode);

        // Continue in append mode
        table.insert(create_row(100, "Eve")).unwrap();
        assert!(table.append_mode); // Still in append mode

        // Try to insert duplicate (non-sequential, will exit append mode)
        table.insert(create_row(4, "Frank")).unwrap(); // Inserts successfully (no constraint check in Table)
        assert!(!table.append_mode); // Exited append mode (4 < 100)

        // Note: Duplicate detection happens at executor level, not table level
        // This test verifies that append mode resets on non-sequential inserts
    }

    #[test]
    fn test_empty_table_first_insert() {
        let mut table = create_test_table();

        // First insert should not trigger append mode
        table.insert(create_row(5, "Alice")).unwrap();
        assert_eq!(table.append_streak, 0); // No comparison possible
        assert!(!table.append_mode);
        assert_eq!(table.last_pk_value, Some(vec![SqlValue::Integer(5)]));
    }

    #[test]
    fn test_is_in_append_mode_accessor() {
        let mut table = create_test_table();

        assert!(!table.is_in_append_mode());

        // Enter append mode
        table.insert(create_row(1, "Alice")).unwrap();
        table.insert(create_row(2, "Bob")).unwrap();
        table.insert(create_row(3, "Charlie")).unwrap();
        table.insert(create_row(4, "David")).unwrap();

        assert!(table.is_in_append_mode());
    }
}
