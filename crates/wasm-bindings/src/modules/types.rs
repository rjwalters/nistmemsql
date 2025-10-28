//! Type definitions for WASM bindings

use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

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
