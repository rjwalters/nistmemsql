use crate::column::ColumnSchema;

/// Table schema definition.
#[derive(Debug, Clone, PartialEq)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
}

impl TableSchema {
    pub fn new(name: String, columns: Vec<ColumnSchema>) -> Self {
        TableSchema { name, columns }
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
}
