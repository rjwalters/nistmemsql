use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

// Module declarations
mod query;
mod execute;
mod schema;
mod examples;
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
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}
