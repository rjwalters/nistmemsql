// ============================================================================
// Database
// ============================================================================

use crate::{Row, StorageError, Table};
use std::collections::HashMap;

/// In-memory database - manages catalog and tables
#[derive(Debug, Clone)]
pub struct Database {
    pub catalog: catalog::Catalog,
    tables: HashMap<String, Table>,
}

impl Database {
    /// Create a new empty database
    pub fn new() -> Self {
        Database { catalog: catalog::Catalog::new(), tables: HashMap::new() }
    }

    /// Create a table
    pub fn create_table(&mut self, schema: catalog::TableSchema) -> Result<(), StorageError> {
        let table_name = schema.name.clone();

        // Add to catalog
        self.catalog
            .create_table(schema.clone())
            .map_err(|e| StorageError::CatalogError(e.to_string()))?;

        // Create empty table
        let table = Table::new(schema);
        self.tables.insert(table_name, table);

        Ok(())
    }

    /// Get a table for reading
    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    /// Get a table for writing
    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.tables.get_mut(name)
    }

    /// Drop a table
    pub fn drop_table(&mut self, name: &str) -> Result<(), StorageError> {
        // Remove from catalog
        self.catalog.drop_table(name).map_err(|e| StorageError::CatalogError(e.to_string()))?;

        // Remove table data
        self.tables.remove(name);

        Ok(())
    }

    /// Insert a row into a table
    pub fn insert_row(&mut self, table_name: &str, row: Row) -> Result<(), StorageError> {
        let table = self
            .get_table_mut(table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;

        table.insert(row)
    }

    /// List all table names
    pub fn list_tables(&self) -> Vec<String> {
        self.catalog.list_tables()
    }

    /// Get debug information about database state
    pub fn debug_info(&self) -> String {
        let mut output = String::new();
        output.push_str("=== Database Debug Info ===\n");
        output.push_str(&format!("Tables: {}\n", self.list_tables().len()));
        for table_name in self.list_tables() {
            if let Some(table) = self.get_table(&table_name) {
                output.push_str(&format!(
                    "  - {} ({} rows, {} columns)\n",
                    table_name,
                    table.row_count(),
                    table.schema.column_count()
                ));
            }
        }
        output
    }

    /// Dump all table contents in readable format
    pub fn dump_tables(&self) -> String {
        let mut output = String::new();
        for table_name in self.list_tables() {
            if let Ok(dump) = self.dump_table(&table_name) {
                output.push_str(&dump);
                output.push('\n');
            }
        }
        output
    }

    /// Dump a specific table's contents
    pub fn dump_table(&self, name: &str) -> Result<String, StorageError> {
        let table =
            self.get_table(name).ok_or_else(|| StorageError::TableNotFound(name.to_string()))?;

        let mut output = String::new();
        output.push_str(&format!("=== Table: {} ===\n", name));

        // Column headers
        let col_names: Vec<String> = table.schema.columns.iter().map(|c| c.name.clone()).collect();
        output.push_str(&format!("{}\n", col_names.join(" | ")));
        output.push_str(&format!("{}\n", "-".repeat(col_names.join(" | ").len())));

        // Rows
        for row in table.scan() {
            let values: Vec<String> = row.values.iter().map(|v| format!("{}", v)).collect();
            output.push_str(&format!("{}\n", values.join(" | ")));
        }

        output.push_str(&format!("({} rows)\n", table.row_count()));
        Ok(output)
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}
