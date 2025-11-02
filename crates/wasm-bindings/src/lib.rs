use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsError;

// Module declarations
pub mod examples;
pub mod execute;
pub mod query;
pub mod schema;
#[cfg(test)]
mod tests;

/// Initializes the WASM module and sets up panic hooks
#[wasm_bindgen(start)]
pub fn init_wasm() {
    // Better error messages in browser console
    console_error_panic_hook::set_once();
}

/// Result of a query execution
#[derive(Serialize, Deserialize)]
#[wasm_bindgen(getter_with_clone)]
pub struct QueryResult {
    /// Column names
    pub columns: Vec<String>,
    /// Row data as JSON strings
    pub rows: Vec<String>,
    /// Number of rows
    pub row_count: usize,
}

/// Result of an execute (DDL/DML) operation
#[derive(Serialize, Deserialize)]
#[wasm_bindgen(getter_with_clone)]
pub struct ExecuteResult {
    /// Number of rows affected (for INSERT, UPDATE, DELETE)
    pub rows_affected: usize,
    /// Success message
    pub message: String,
}

/// Table column metadata
#[derive(Clone, Serialize, Deserialize)]
#[wasm_bindgen(getter_with_clone)]
pub struct ColumnInfo {
    /// Column name
    pub name: String,
    /// Data type (as string)
    pub data_type: String,
    /// Whether column can be NULL
    pub nullable: bool,
}

/// Table schema information
#[derive(Clone, Serialize, Deserialize)]
#[wasm_bindgen(getter_with_clone)]
pub struct TableSchema {
    /// Table name
    pub name: String,
    /// Column definitions
    pub columns: Vec<ColumnInfo>,
}

/// In-memory SQL database with WASM bindings
#[wasm_bindgen]
pub struct Database {
    db: storage::Database,
}

#[wasm_bindgen]
impl Database {
    /// Creates a new empty database instance
    #[wasm_bindgen(constructor)]
    pub fn new() -> Database {
        Database { db: storage::Database::new() }
    }

    /// Returns the version string
    pub fn version(&self) -> String {
        "nistmemsql-wasm 0.1.0".to_string()
    }

    /// Lists all table names in the database
    pub fn list_tables(&self) -> Vec<String> {
        self.db.list_tables()
    }

    /// Executes a SELECT query and returns results as JSON
    pub fn query(&self, sql: String) -> Result<JsValue, JsError> {
        query::execute_query(self, &sql)
            .map_err(|e| JsError::new(&format!("{:?}", e)))
    }

    /// Gets the schema for a specific table
    pub fn describe_table(&self, table_name: String) -> Result<JsValue, JsError> {
        schema::describe_table_impl(self, &table_name)
            .map_err(|e| JsError::new(&format!("{:?}", e)))
    }

    /// Executes a DDL or DML statement (CREATE TABLE, INSERT, UPDATE, DELETE)
    /// Returns a JSON string with the result
    pub fn execute(&mut self, sql: String) -> Result<JsValue, JsError> {
        execute::execute_statement(self, &sql)
            .map_err(|e| JsError::new(&format!("{:?}", e)))
    }

    /// Load the Employees example database
    pub fn load_employees(&mut self) -> Result<JsValue, JsError> {
        examples::load_employees_impl(self)
            .map_err(|e| JsError::new(&format!("{:?}", e)))
    }

    /// Load the Northwind example database
    pub fn load_northwind(&mut self) -> Result<JsValue, JsError> {
        examples::load_northwind_impl(self)
            .map_err(|e| JsError::new(&format!("{:?}", e)))
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}
