use crate::column::ColumnSchema;

/// Table schema definition.
#[derive(Debug, Clone, PartialEq)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
    /// Primary key column names (None if no primary key, Some(vec) for single or composite key)
    pub primary_key: Option<Vec<String>>,
    /// Unique constraints - each inner vec represents a unique constraint (can be single or composite)
    pub unique_constraints: Vec<Vec<String>>,
    /// Check constraints - each tuple is (constraint_name, check_expression)
    pub check_constraints: Vec<(String, ast::Expression)>,
}

impl TableSchema {
    pub fn new(name: String, columns: Vec<ColumnSchema>) -> Self {
        TableSchema {
            name,
            columns,
            primary_key: None,
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
        }
    }

    /// Create a table schema with a primary key
    pub fn with_primary_key(name: String, columns: Vec<ColumnSchema>, primary_key: Vec<String>) -> Self {
        TableSchema {
            name,
            columns,
            primary_key: Some(primary_key),
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
        }
    }

    /// Create a table schema with unique constraints
    pub fn with_unique_constraints(
        name: String,
        columns: Vec<ColumnSchema>,
        unique_constraints: Vec<Vec<String>>,
    ) -> Self {
        TableSchema {
            name,
            columns,
            primary_key: None,
            unique_constraints,
            check_constraints: Vec::new(),
        }
    }

    /// Create a table schema with both primary key and unique constraints
    pub fn with_all_constraints(
        name: String,
        columns: Vec<ColumnSchema>,
        primary_key: Option<Vec<String>>,
        unique_constraints: Vec<Vec<String>>,
    ) -> Self {
        TableSchema {
            name,
            columns,
            primary_key,
            unique_constraints,
            check_constraints: Vec::new(),
        }
    }

    /// Create a table schema with all constraint types
    pub fn with_all_constraint_types(
        name: String,
        columns: Vec<ColumnSchema>,
        primary_key: Option<Vec<String>>,
        unique_constraints: Vec<Vec<String>>,
        check_constraints: Vec<(String, ast::Expression)>,
    ) -> Self {
        TableSchema {
            name,
            columns,
            primary_key,
            unique_constraints,
            check_constraints,
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

    /// Get the indices for all unique constraints
    /// Returns a vector where each element is a vector of column indices for one unique constraint
    pub fn get_unique_constraint_indices(&self) -> Vec<Vec<usize>> {
        self.unique_constraints
            .iter()
            .map(|constraint_cols| {
                constraint_cols
                    .iter()
                    .filter_map(|col_name| self.get_column_index(col_name))
                    .collect()
            })
            .collect()
    }
}
