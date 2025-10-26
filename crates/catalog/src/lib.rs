//! Catalog - Schema Metadata Storage
//!
//! This crate manages database schema metadata including table definitions,
//! column schemas, and constraints.

use std::collections::HashMap;

// ============================================================================
// Column Schema
// ============================================================================

/// Column definition in a table schema
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: types::DataType,
    pub nullable: bool,
}

impl ColumnSchema {
    pub fn new(name: String, data_type: types::DataType, nullable: bool) -> Self {
        ColumnSchema { name, data_type, nullable }
    }
}

// ============================================================================
// Table Schema
// ============================================================================

/// Table schema definition
#[derive(Debug, Clone, PartialEq)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
}

impl TableSchema {
    pub fn new(name: String, columns: Vec<ColumnSchema>) -> Self {
        TableSchema { name, columns }
    }

    /// Get column by name
    pub fn get_column(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|col| col.name == name)
    }

    /// Get column index by name
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|col| col.name == name)
    }

    /// Get number of columns
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}

// ============================================================================
// Catalog
// ============================================================================

/// Database catalog - manages all table schemas
#[derive(Debug, Clone)]
pub struct Catalog {
    tables: HashMap<String, TableSchema>,
}

impl Catalog {
    /// Create a new empty catalog
    pub fn new() -> Self {
        Catalog { tables: HashMap::new() }
    }

    /// Create a table schema
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

    /// Drop a table schema
    pub fn drop_table(&mut self, name: &str) -> Result<(), CatalogError> {
        if self.tables.remove(name).is_some() {
            Ok(())
        } else {
            Err(CatalogError::TableNotFound(name.to_string()))
        }
    }

    /// List all table names
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// Check if table exists
    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum CatalogError {
    TableAlreadyExists(String),
    TableNotFound(String),
}

impl std::fmt::Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogError::TableAlreadyExists(name) => {
                write!(f, "Table '{}' already exists", name)
            }
            CatalogError::TableNotFound(name) => write!(f, "Table '{}' not found", name),
        }
    }
}

impl std::error::Error for CatalogError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_schema_creation() {
        let col = ColumnSchema::new(
            "id".to_string(),
            types::DataType::Integer,
            false, // not nullable
        );
        assert_eq!(col.name, "id");
        assert_eq!(col.data_type, types::DataType::Integer);
        assert!(!col.nullable);
    }

    #[test]
    fn test_table_schema_creation() {
        let columns = vec![
            ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: 100 },
                true,
            ),
        ];
        let schema = TableSchema::new("users".to_string(), columns);
        assert_eq!(schema.name, "users");
        assert_eq!(schema.column_count(), 2);
    }

    #[test]
    fn test_table_schema_get_column() {
        let columns = vec![
            ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: 100 },
                true,
            ),
        ];
        let schema = TableSchema::new("users".to_string(), columns);

        let id_col = schema.get_column("id");
        assert!(id_col.is_some());
        assert_eq!(id_col.unwrap().data_type, types::DataType::Integer);

        let missing_col = schema.get_column("missing");
        assert!(missing_col.is_none());
    }

    #[test]
    fn test_table_schema_get_column_index() {
        let columns = vec![
            ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: 100 },
                true,
            ),
        ];
        let schema = TableSchema::new("users".to_string(), columns);

        assert_eq!(schema.get_column_index("id"), Some(0));
        assert_eq!(schema.get_column_index("name"), Some(1));
        assert_eq!(schema.get_column_index("missing"), None);
    }

    #[test]
    fn test_catalog_create_table() {
        let mut catalog = Catalog::new();
        let columns = vec![ColumnSchema::new("id".to_string(), types::DataType::Integer, false)];
        let schema = TableSchema::new("users".to_string(), columns);

        let result = catalog.create_table(schema);
        assert!(result.is_ok());
        assert!(catalog.table_exists("users"));
    }

    #[test]
    fn test_catalog_create_duplicate_table() {
        let mut catalog = Catalog::new();
        let columns = vec![ColumnSchema::new("id".to_string(), types::DataType::Integer, false)];
        let schema1 = TableSchema::new("users".to_string(), columns.clone());
        let schema2 = TableSchema::new("users".to_string(), columns);

        catalog.create_table(schema1).unwrap();
        let result = catalog.create_table(schema2);

        assert!(result.is_err());
        match result.unwrap_err() {
            CatalogError::TableAlreadyExists(name) => assert_eq!(name, "users"),
            _ => panic!("Expected TableAlreadyExists error"),
        }
    }

    #[test]
    fn test_catalog_get_table() {
        let mut catalog = Catalog::new();
        let columns = vec![ColumnSchema::new("id".to_string(), types::DataType::Integer, false)];
        let schema = TableSchema::new("users".to_string(), columns);

        catalog.create_table(schema).unwrap();

        let retrieved = catalog.get_table("users");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name, "users");

        let missing = catalog.get_table("missing");
        assert!(missing.is_none());
    }

    #[test]
    fn test_catalog_drop_table() {
        let mut catalog = Catalog::new();
        let columns = vec![ColumnSchema::new("id".to_string(), types::DataType::Integer, false)];
        let schema = TableSchema::new("users".to_string(), columns);

        catalog.create_table(schema).unwrap();
        assert!(catalog.table_exists("users"));

        let result = catalog.drop_table("users");
        assert!(result.is_ok());
        assert!(!catalog.table_exists("users"));
    }

    #[test]
    fn test_catalog_drop_nonexistent_table() {
        let mut catalog = Catalog::new();
        let result = catalog.drop_table("missing");

        assert!(result.is_err());
        match result.unwrap_err() {
            CatalogError::TableNotFound(name) => assert_eq!(name, "missing"),
            _ => panic!("Expected TableNotFound error"),
        }
    }

    #[test]
    fn test_catalog_list_tables() {
        let mut catalog = Catalog::new();

        let schema1 = TableSchema::new(
            "users".to_string(),
            vec![ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
        );
        let schema2 = TableSchema::new(
            "orders".to_string(),
            vec![ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
        );

        catalog.create_table(schema1).unwrap();
        catalog.create_table(schema2).unwrap();

        let tables = catalog.list_tables();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"users".to_string()));
        assert!(tables.contains(&"orders".to_string()));
    }
}
