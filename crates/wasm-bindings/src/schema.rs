//! Schema introspection for WASM bindings
//!
//! This module provides methods to inspect database schema:
//! listing tables and describing table structures.

use wasm_bindgen::prelude::*;
use crate::{Database, TableSchema, ColumnInfo};

impl Database {
    /// Lists all table names in the database
    pub fn list_tables(&self) -> Result<JsValue, JsValue> {
        let table_names: Vec<String> = self.db.list_tables();

        serde_wasm_bindgen::to_value(&table_names)
            .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
    }

    /// Gets the schema for a specific table
    pub fn describe_table(&self, table_name: &str) -> Result<JsValue, JsValue> {
        let table = self
            .db
            .get_table(table_name)
            .ok_or_else(|| JsValue::from_str(&format!("Table '{}' not found", table_name)))?;

        let schema = &table.schema;
        let columns: Vec<ColumnInfo> = schema
            .columns
            .iter()
            .map(|col| ColumnInfo {
                name: col.name.clone(),
                data_type: format!("{:?}", col.data_type),
                nullable: col.nullable,
            })
            .collect();

        let table_schema = TableSchema { name: table_name.to_string(), columns };

        serde_wasm_bindgen::to_value(&table_schema)
            .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
    }
}
