use std::collections::HashMap;

use crate::errors::CatalogError;
use crate::table::TableSchema;

/// Database catalog - manages all table schemas.
#[derive(Debug, Clone)]
pub struct Catalog {
    tables: HashMap<String, TableSchema>,
}

impl Catalog {
    /// Create a new empty catalog.
    pub fn new() -> Self {
        Catalog { tables: HashMap::new() }
    }

    /// Create a table schema.
    pub fn create_table(&mut self, schema: TableSchema) -> Result<(), CatalogError> {
        let table_name = schema.name.clone();
        if self.tables.contains_key(&table_name) {
            return Err(CatalogError::TableAlreadyExists(table_name));
        }
        self.tables.insert(table_name, schema);
        Ok(())
    }

    /// Get a table schema by name.
    pub fn get_table(&self, name: &str) -> Option<&TableSchema> {
        self.tables.get(name)
    }

    /// Drop a table schema.
    pub fn drop_table(&mut self, name: &str) -> Result<(), CatalogError> {
        if self.tables.remove(name).is_some() {
            Ok(())
        } else {
            Err(CatalogError::TableNotFound(name.to_string()))
        }
    }

    /// List all table names.
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// Check if table exists.
    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}
