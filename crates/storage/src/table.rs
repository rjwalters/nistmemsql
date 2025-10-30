// ============================================================================
// Table
// ============================================================================

use crate::{Row, StorageError};

/// In-memory table - stores rows
#[derive(Debug, Clone)]
pub struct Table {
    pub schema: catalog::TableSchema,
    rows: Vec<Row>,
}

impl Table {
    /// Create a new empty table with given schema
    pub fn new(schema: catalog::TableSchema) -> Self {
        Table { schema, rows: Vec::new() }
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

        self.rows.push(normalized_row);
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
<<<<<<< HEAD
        let current_len = value.len();
        match current_len.cmp(&length) {
=======
        match value.len().cmp(&length) {
>>>>>>> origin/main
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

        self.rows[index] = normalized_row;
        Ok(())
    }

    /// Delete rows matching a predicate
    /// Returns number of rows deleted
    pub fn delete_where<F>(&mut self, predicate: F) -> usize
    where
        F: Fn(&Row) -> bool,
    {
        let initial_count = self.rows.len();
        self.rows.retain(|row| !predicate(row));
        initial_count - self.rows.len()
    }

    /// Remove a specific row (used for transaction undo)
    /// Returns error if row not found
    pub fn remove_row(&mut self, target_row: &Row) -> Result<(), StorageError> {
        // Find and remove the first matching row
        if let Some(pos) = self.rows.iter().position(|row| row == target_row) {
            self.rows.remove(pos);
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
}
