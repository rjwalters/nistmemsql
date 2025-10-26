//! Storage - In-Memory Data Storage
//!
//! This crate provides in-memory storage for database tables and rows.

use std::collections::HashMap;

// ============================================================================
// Row
// ============================================================================

/// A single row of data - vector of SqlValues
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub values: Vec<types::SqlValue>,
}

impl Row {
    /// Create a new row from values
    pub fn new(values: Vec<types::SqlValue>) -> Self {
        Row { values }
    }

    /// Get value at column index
    pub fn get(&self, index: usize) -> Option<&types::SqlValue> {
        self.values.get(index)
    }

    /// Get number of columns in this row
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if row is empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

// ============================================================================
// Table
// ============================================================================

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

// ============================================================================
// Database
// ============================================================================

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

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum StorageError {
    TableNotFound(String),
    ColumnCountMismatch { expected: usize, actual: usize },
    CatalogError(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::TableNotFound(name) => write!(f, "Table '{}' not found", name),
            StorageError::ColumnCountMismatch { expected, actual } => {
                write!(f, "Column count mismatch: expected {}, got {}", expected, actual)
            }
            StorageError::CatalogError(msg) => write!(f, "Catalog error: {}", msg),
        }
    }
}

impl std::error::Error for StorageError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_creation() {
        let row = Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("Alice".to_string()),
        ]);
        assert_eq!(row.len(), 2);
        assert_eq!(row.get(0), Some(&types::SqlValue::Integer(1)));
    }

    #[test]
    fn test_table_creation() {
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "name".to_string(),
                    types::DataType::Varchar { max_length: 100 },
                    true,
                ),
            ],
        );
        let table = Table::new(schema);
        assert_eq!(table.row_count(), 0);
        assert_eq!(table.schema.name, "users");
    }

    #[test]
    fn test_table_insert() {
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "name".to_string(),
                    types::DataType::Varchar { max_length: 100 },
                    true,
                ),
            ],
        );
        let mut table = Table::new(schema);

        let row = Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("Alice".to_string()),
        ]);

        let result = table.insert(row);
        assert!(result.is_ok());
        assert_eq!(table.row_count(), 1);
    }

    #[test]
    fn test_table_insert_wrong_column_count() {
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "name".to_string(),
                    types::DataType::Varchar { max_length: 100 },
                    true,
                ),
            ],
        );
        let mut table = Table::new(schema);

        // Only 1 value when table expects 2
        let row = Row::new(vec![types::SqlValue::Integer(1)]);

        let result = table.insert(row);
        assert!(result.is_err());
        match result.unwrap_err() {
            StorageError::ColumnCountMismatch { expected, actual } => {
                assert_eq!(expected, 2);
                assert_eq!(actual, 1);
            }
            _ => panic!("Expected ColumnCountMismatch error"),
        }
    }

    #[test]
    fn test_table_scan() {
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
        );
        let mut table = Table::new(schema);

        table.insert(Row::new(vec![types::SqlValue::Integer(1)])).unwrap();
        table.insert(Row::new(vec![types::SqlValue::Integer(2)])).unwrap();
        table.insert(Row::new(vec![types::SqlValue::Integer(3)])).unwrap();

        let rows = table.scan();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get(0), Some(&types::SqlValue::Integer(1)));
        assert_eq!(rows[1].get(0), Some(&types::SqlValue::Integer(2)));
        assert_eq!(rows[2].get(0), Some(&types::SqlValue::Integer(3)));
    }

    #[test]
    fn test_database_create_table() {
        let mut db = Database::new();
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
        );

        let result = db.create_table(schema);
        assert!(result.is_ok());
        assert!(db.catalog.table_exists("users"));
        assert!(db.get_table("users").is_some());
    }

    #[test]
    fn test_database_insert_row() {
        let mut db = Database::new();
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
        );
        db.create_table(schema).unwrap();

        let row = Row::new(vec![types::SqlValue::Integer(1)]);
        let result = db.insert_row("users", row);
        assert!(result.is_ok());

        let table = db.get_table("users").unwrap();
        assert_eq!(table.row_count(), 1);
    }

    #[test]
    fn test_database_insert_into_nonexistent_table() {
        let mut db = Database::new();
        let row = Row::new(vec![types::SqlValue::Integer(1)]);
        let result = db.insert_row("missing", row);

        assert!(result.is_err());
        match result.unwrap_err() {
            StorageError::TableNotFound(name) => assert_eq!(name, "missing"),
            _ => panic!("Expected TableNotFound error"),
        }
    }

    #[test]
    fn test_database_drop_table() {
        let mut db = Database::new();
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
        );
        db.create_table(schema).unwrap();

        assert!(db.catalog.table_exists("users"));

        let result = db.drop_table("users");
        assert!(result.is_ok());
        assert!(!db.catalog.table_exists("users"));
        assert!(db.get_table("users").is_none());
    }

    #[test]
    fn test_database_multiple_tables() {
        let mut db = Database::new();

        // Create users table
        let users_schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
        );
        db.create_table(users_schema).unwrap();

        // Create orders table
        let orders_schema = catalog::TableSchema::new(
            "orders".to_string(),
            vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
        );
        db.create_table(orders_schema).unwrap();

        // Insert into both
        db.insert_row("users", Row::new(vec![types::SqlValue::Integer(1)])).unwrap();
        db.insert_row("orders", Row::new(vec![types::SqlValue::Integer(100)])).unwrap();

        assert_eq!(db.get_table("users").unwrap().row_count(), 1);
        assert_eq!(db.get_table("orders").unwrap().row_count(), 1);

        let tables = db.list_tables();
        assert_eq!(tables.len(), 2);
    }

    // ========================================================================
    // Diagnostic Tools Tests
    // ========================================================================

    #[test]
    fn test_debug_info() {
        let mut db = Database::new();
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "name".to_string(),
                    types::DataType::Varchar { max_length: 100 },
                    true,
                ),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "users",
            Row::new(vec![
                types::SqlValue::Integer(1),
                types::SqlValue::Varchar("Alice".to_string()),
            ]),
        )
        .unwrap();

        let debug = db.debug_info();
        assert!(debug.contains("Database Debug Info"));
        assert!(debug.contains("Tables: 1"));
        assert!(debug.contains("users"));
        assert!(debug.contains("1 rows"));
        assert!(debug.contains("2 columns"));
    }

    #[test]
    fn test_dump_table() {
        let mut db = Database::new();
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "name".to_string(),
                    types::DataType::Varchar { max_length: 100 },
                    true,
                ),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "users",
            Row::new(vec![
                types::SqlValue::Integer(1),
                types::SqlValue::Varchar("Alice".to_string()),
            ]),
        )
        .unwrap();
        db.insert_row(
            "users",
            Row::new(vec![
                types::SqlValue::Integer(2),
                types::SqlValue::Varchar("Bob".to_string()),
            ]),
        )
        .unwrap();

        let dump = db.dump_table("users").unwrap();
        assert!(dump.contains("Table: users"));
        assert!(dump.contains("id | name"));
        assert!(dump.contains("1 | Alice"));
        assert!(dump.contains("2 | Bob"));
        assert!(dump.contains("(2 rows)"));
    }

    #[test]
    fn test_dump_table_not_found() {
        let db = Database::new();
        let result = db.dump_table("missing");
        assert!(result.is_err());
        match result.unwrap_err() {
            StorageError::TableNotFound(name) => assert_eq!(name, "missing"),
            _ => panic!("Expected TableNotFound error"),
        }
    }

    #[test]
    fn test_dump_tables() {
        let mut db = Database::new();

        // Create and populate users table
        let users_schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
        );
        db.create_table(users_schema).unwrap();
        db.insert_row("users", Row::new(vec![types::SqlValue::Integer(1)])).unwrap();

        // Create and populate orders table
        let orders_schema = catalog::TableSchema::new(
            "orders".to_string(),
            vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
        );
        db.create_table(orders_schema).unwrap();
        db.insert_row("orders", Row::new(vec![types::SqlValue::Integer(100)])).unwrap();

        let dump = db.dump_tables();
        assert!(dump.contains("Table: users") || dump.contains("Table: orders"));
        assert!(dump.contains("1") || dump.contains("100"));
    }
}
