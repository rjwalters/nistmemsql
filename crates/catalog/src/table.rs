use crate::column::ColumnSchema;
use crate::foreign_key::ForeignKeyConstraint;
use std::collections::HashMap;

/// Table schema definition.
#[derive(Debug, Clone, PartialEq)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
    /// Cache for O(1) column name to index lookup
    column_index_cache: HashMap<String, usize>,
    /// Primary key column names (None if no primary key, Some(vec) for single or composite key)
    pub primary_key: Option<Vec<String>>,
    /// Unique constraints - each inner vec represents a unique constraint (can be single or composite)
    pub unique_constraints: Vec<Vec<String>>,
    /// Check constraints - each tuple is (constraint_name, check_expression)
    pub check_constraints: Vec<(String, ast::Expression)>,
    /// Foreign key constraints
    pub foreign_keys: Vec<ForeignKeyConstraint>,
}

impl TableSchema {
    pub fn new(name: String, columns: Vec<ColumnSchema>) -> Self {
        let column_index_cache: HashMap<String, usize> =
            columns.iter().enumerate().map(|(idx, col)| (col.name.clone(), idx)).collect();

        TableSchema {
            name,
            columns,
            column_index_cache,
            primary_key: None,
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
        }
    }

    /// Create a table schema with a primary key
    pub fn with_primary_key(
        name: String,
        columns: Vec<ColumnSchema>,
        primary_key: Vec<String>,
    ) -> Self {
        let column_index_cache: HashMap<String, usize> =
            columns.iter().enumerate().map(|(idx, col)| (col.name.clone(), idx)).collect();

        TableSchema {
            name,
            columns,
            column_index_cache,
            primary_key: Some(primary_key),
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
        }
    }

    /// Create a table schema with unique constraints
    pub fn with_unique_constraints(
        name: String,
        columns: Vec<ColumnSchema>,
        unique_constraints: Vec<Vec<String>>,
    ) -> Self {
        let column_index_cache: HashMap<String, usize> =
            columns.iter().enumerate().map(|(idx, col)| (col.name.clone(), idx)).collect();

        TableSchema {
            name,
            columns,
            column_index_cache,
            primary_key: None,
            unique_constraints,
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
        }
    }

    /// Create a table schema with foreign key constraints
    pub fn with_foreign_keys(
        name: String,
        columns: Vec<ColumnSchema>,
        foreign_keys: Vec<ForeignKeyConstraint>,
    ) -> Self {
        let column_index_cache: HashMap<String, usize> =
            columns.iter().enumerate().map(|(idx, col)| (col.name.clone(), idx)).collect();

        TableSchema {
            name,
            columns,
            column_index_cache,
            primary_key: None,
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
            foreign_keys,
        }
    }

    /// Create a table schema with both primary key and unique constraints
    pub fn with_all_constraints(
        name: String,
        columns: Vec<ColumnSchema>,
        primary_key: Option<Vec<String>>,
        unique_constraints: Vec<Vec<String>>,
    ) -> Self {
        let column_index_cache: HashMap<String, usize> =
            columns.iter().enumerate().map(|(idx, col)| (col.name.clone(), idx)).collect();

        TableSchema {
            name,
            columns,
            column_index_cache,
            primary_key,
            unique_constraints,
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
        }
    }

    /// Create a table schema with all constraint types
    pub fn with_all_constraint_types(
        name: String,
        columns: Vec<ColumnSchema>,
        primary_key: Option<Vec<String>>,
        unique_constraints: Vec<Vec<String>>,
        check_constraints: Vec<(String, ast::Expression)>,
        foreign_keys: Vec<ForeignKeyConstraint>,
    ) -> Self {
        let column_index_cache: HashMap<String, usize> =
            columns.iter().enumerate().map(|(idx, col)| (col.name.clone(), idx)).collect();

        TableSchema {
            name,
            columns,
            column_index_cache,
            primary_key,
            unique_constraints,
            check_constraints,
            foreign_keys,
        }
    }

    /// Get column by name.
    pub fn get_column(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|col| col.name == name)
    }

    /// Get column index by name.
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.column_index_cache.get(name).copied()
    }

    /// Get number of columns.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Get the indices of primary key columns
    pub fn get_primary_key_indices(&self) -> Option<Vec<usize>> {
        self.primary_key.as_ref().map(|pk_cols| {
            pk_cols.iter().filter_map(|col_name| self.get_column_index(col_name)).collect()
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

    /// Add a column to the table schema
    pub fn add_column(&mut self, column: ColumnSchema) -> Result<(), crate::CatalogError> {
        if self.get_column(&column.name).is_some() {
            return Err(crate::CatalogError::ColumnAlreadyExists(column.name));
        }
        let index = self.columns.len();
        self.columns.push(column.clone());
        self.column_index_cache.insert(column.name, index);
        Ok(())
    }

    /// Remove a column from the table schema by index
    pub fn remove_column(&mut self, index: usize) -> Result<(), crate::CatalogError> {
        if index >= self.columns.len() {
            return Err(crate::CatalogError::ColumnNotFound("index out of bounds".to_string()));
        }
        let removed_column = self.columns.remove(index);

        // Rebuild the column index cache since indices have shifted
        self.column_index_cache.clear();
        for (idx, col) in self.columns.iter().enumerate() {
            self.column_index_cache.insert(col.name.clone(), idx);
        }

        // Remove from primary key if present
        if let Some(ref mut pk) = self.primary_key {
            pk.retain(|col_name| col_name != &removed_column.name);
            if pk.is_empty() {
                self.primary_key = None;
            }
        }

        // Remove from unique constraints
        self.unique_constraints = self
            .unique_constraints
            .iter()
            .filter_map(|constraint| {
                let filtered: Vec<String> = constraint
                    .iter()
                    .filter(|col_name| *col_name != &removed_column.name)
                    .cloned()
                    .collect();
                if filtered.is_empty() {
                    None
                } else {
                    Some(filtered)
                }
            })
            .collect();

        // TODO: Handle foreign keys and check constraints

        Ok(())
    }

    /// Check if a column exists
    pub fn has_column(&self, name: &str) -> bool {
        self.get_column(name).is_some()
    }

    /// Check if a column is part of the primary key
    pub fn is_column_in_primary_key(&self, column_name: &str) -> bool {
        self.primary_key.as_ref().is_some_and(|pk| pk.contains(&column_name.to_string()))
    }

    /// Set nullable property for a column by index
    pub fn set_column_nullable(
        &mut self,
        index: usize,
        nullable: bool,
    ) -> Result<(), crate::CatalogError> {
        if index >= self.columns.len() {
            return Err(crate::CatalogError::ColumnNotFound("index out of bounds".to_string()));
        }
        self.columns[index].set_nullable(nullable);
        Ok(())
    }
}
