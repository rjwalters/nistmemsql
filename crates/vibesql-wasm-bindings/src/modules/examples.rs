//! Example database loaders

use super::database::Database;
use super::types::ExecuteResult;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
impl Database {
    /// Loads the Employees example database (hierarchical org structure)
    /// Demonstrates recursive queries with WITH RECURSIVE
    pub fn load_employees(&mut self) -> Result<JsValue, JsValue> {
        let sql = include_str!("../../../../web-demo/examples/employees.sql");

        // Execute the SQL file (contains CREATE TABLE and INSERT statements)
        // Split by semicolons and execute each statement
        for statement_sql in sql.split(';') {
            let trimmed = statement_sql.trim();
            if trimmed.is_empty() || trimmed.starts_with("--") {
                continue; // Skip empty lines and comments
            }

            self.execute(trimmed)?;
        }

        let result = ExecuteResult {
            rows_affected: 35, // 35 employees inserted
            message: "Employees database loaded successfully (35 employees)".to_string(),
        };

        serde_wasm_bindgen::to_value(&result)
            .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
    }

    /// Loads the Northwind example database (classic sales/orders database)
    /// Demonstrates JOINs, aggregates, and relational database concepts
    pub fn load_northwind(&mut self) -> Result<JsValue, JsValue> {
        let sql = include_str!("../../../../web-demo/examples/northwind.sql");

        // Execute the SQL file (contains CREATE TABLE and INSERT statements)
        // Split by semicolons and execute each statement
        for statement_sql in sql.split(';') {
            let trimmed = statement_sql.trim();
            if trimmed.is_empty() || trimmed.starts_with("--") {
                continue; // Skip empty lines and comments
            }

            self.execute(trimmed)?;
        }

        let result = ExecuteResult {
            rows_affected: 143, // Total rows across all tables
            message: "Northwind database loaded successfully (5 tables, 143 rows)".to_string(),
        };

        serde_wasm_bindgen::to_value(&result)
            .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
    }
}
