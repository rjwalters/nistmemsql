//! Schema - Named collection of database objects

use crate::errors::CatalogError;
use crate::table::TableSchema;
use std::collections::HashMap;

/// A schema - named collection of database objects (tables, views, etc.)
#[derive(Debug, Clone)]
pub struct Schema {
    pub name: String,
    tables: HashMap<String, TableSchema>,
    // Future: views, functions, sequences, etc.
}

impl Schema {
    /// Create a new empty schema
    pub fn new(name: String) -> Self {
        Schema { name, tables: HashMap::new() }
    }

    /// Create a table in this schema
    pub fn create_table(&mut self, schema: TableSchema) -> Result<(), CatalogError> {
        let table_name = schema.name.clone();
        if self.tables.contains_key(&table_name) {
            return Err(CatalogError::TableAlreadyExists(table_name));
        }
        self.tables.insert(table_name, schema);
        Ok(())
    }

    /// Get a table schema by name
    pub fn get_table(&self, name: &str) -> Option<&TableSchema> {
        self.tables.get(name)
    }

    /// Drop a table from this schema
    pub fn drop_table(&mut self, name: &str) -> Result<(), CatalogError> {
        if self.tables.remove(name).is_some() {
            Ok(())
        } else {
            Err(CatalogError::TableNotFound(name.to_string()))
        }
    }

    /// List all table names in this schema
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// Check if table exists in this schema
    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// Check if schema is empty (no tables)
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Get the number of tables in this schema
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }
}
