use crate::column::ColumnSchema;

/// Table schema definition.
#[derive(Debug, Clone, PartialEq)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
    /// Primary key column names (None if no primary key, Some(vec) for single or composite key)
    pub primary_key: Option<Vec<String>>,
}

impl TableSchema {
    pub fn new(name: String, columns: Vec<ColumnSchema>) -> Self {
        TableSchema {
            name,
            columns,
            primary_key: None,
        }
    }

    /// Create a table schema with a primary key
    pub fn with_primary_key(name: String, columns: Vec<ColumnSchema>, primary_key: Vec<String>) -> Self {
        TableSchema {
            name,
            columns,
            primary_key: Some(primary_key),
        }
    }

    /// Get column by name.
    pub fn get_column(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|col| col.name == name)
    }

    /// Get column index by name.
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|col| col.name == name)
    }

    /// Get number of columns.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Get the indices of primary key columns
    pub fn get_primary_key_indices(&self) -> Option<Vec<usize>> {
        self.primary_key.as_ref().map(|pk_cols| {
            pk_cols
                .iter()
                .filter_map(|col_name| self.get_column_index(col_name))
                .collect()
        })
    }
}
