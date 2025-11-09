//! Query execution for WASM bindings
//!
//! This module handles SELECT query execution and result serialization.

use wasm_bindgen::prelude::*;

use crate::{Database, QueryResult};

/// Executes a SELECT query and returns results as JSON
pub fn execute_query(db: &Database, sql: &str) -> Result<JsValue, JsValue> {
    // Parse the SQL
    let stmt = parser::Parser::parse_sql(sql)
        .map_err(|e| JsValue::from_str(&format!("Parse error: {:?}", e)))?;

    // Ensure it's a SELECT statement
    let select_stmt = match stmt {
        ast::Statement::Select(s) => s,
        _ => return Err(JsValue::from_str("query() method requires a SELECT statement")),
    };

    // Execute the query with column metadata
    let select_executor = executor::SelectExecutor::new(&db.db);
    let result = select_executor
        .execute_with_columns(&select_stmt)
        .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

    let columns = result.columns;
    let rows = result.rows;

    // Convert rows to JSON strings
    let row_strings: Vec<String> = rows
        .iter()
        .map(|row| {
            // Convert each SqlValue to a JSON-compatible representation
            let json_values: Vec<serde_json::Value> = row
                .values
                .iter()
                .map(|v| match v {
                    types::SqlValue::Integer(i) => serde_json::Value::Number((*i).into()),
                    types::SqlValue::Smallint(i) => serde_json::Value::Number((*i).into()),
                    types::SqlValue::Bigint(i) => serde_json::Value::Number((*i).into()),
                    types::SqlValue::Unsigned(u) => serde_json::Value::Number((*u).into()),
                    types::SqlValue::Float(f) => serde_json::Number::from_f64(*f as f64)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                    types::SqlValue::Real(f) => serde_json::Number::from_f64(*f as f64)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                    types::SqlValue::Double(f) => serde_json::Number::from_f64(*f)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                    types::SqlValue::Varchar(s) | types::SqlValue::Character(s) => {
                        serde_json::Value::String(s.clone())
                    }
                    types::SqlValue::Boolean(b) => serde_json::Value::Bool(*b),
                    types::SqlValue::Numeric(n) => serde_json::Number::from_f64(*n)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                    types::SqlValue::Date(s)
                    | types::SqlValue::Time(s)
                    | types::SqlValue::Timestamp(s)
                    | types::SqlValue::Interval(s) => serde_json::Value::String(s.clone()),
                    types::SqlValue::Null => serde_json::Value::Null,
                })
                .collect();

            serde_json::to_string(&json_values).unwrap_or_else(|_| "[]".to_string())
        })
        .collect();

    let result = QueryResult { columns, rows: row_strings, row_count: rows.len() };

    serde_wasm_bindgen::to_value(&result)
        .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
}
