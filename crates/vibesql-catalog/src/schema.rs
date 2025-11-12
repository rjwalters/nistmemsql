//! Schema - Named collection of database objects

use std::collections::HashMap;

use crate::{errors::CatalogError, table::TableSchema};

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

    /// Get a table schema by name with optional case-insensitive lookup
    pub fn get_table(&self, name: &str, case_sensitive: bool) -> Option<&TableSchema> {
        if case_sensitive {
            self.tables.get(name)
        } else {
            // Case-insensitive lookup: normalize both stored and search keys
            let name_upper = name.to_uppercase();
            self.tables
                .values()
                .find(|table| table.name.to_uppercase() == name_upper)
        }
    }

    /// Get a table schema by name (legacy, uses case-sensitive lookup)
    #[deprecated(since = "0.2.0", note = "Use get_table with case_sensitive parameter")]
    pub fn get_table_deprecated(&self, name: &str) -> Option<&TableSchema> {
        self.tables.get(name)
    }

    /// Drop a table from this schema with optional case-insensitive lookup
    ///
    /// When case_sensitive is true, requires exact name match.
    /// When case_sensitive is false, performs case-insensitive lookup.
    /// Note: The input `name` parameter is the search key (already normalized by caller),
    /// not the original user input.
    pub fn drop_table(&mut self, name: &str, case_sensitive: bool) -> Result<(), CatalogError> {
        if case_sensitive {
            // Case-sensitive: exact match required
            if self.tables.remove(name).is_some() {
                Ok(())
            } else {
                Err(CatalogError::TableNotFound(name.to_string()))
            }
        } else {
            // Case-insensitive: find the actual name first
            let name_upper = name.to_uppercase();
            let actual_name = self.tables
                .keys()
                .find(|k| k.to_uppercase() == name_upper)
                .cloned();

            if let Some(actual_name) = actual_name {
                self.tables.remove(&actual_name);
                Ok(())
            } else {
                Err(CatalogError::TableNotFound(name.to_string()))
            }
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
