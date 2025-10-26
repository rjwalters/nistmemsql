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

        self.rows.push(row);
        Ok(())
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
}
