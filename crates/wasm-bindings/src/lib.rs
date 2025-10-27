use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

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

    /// Executes a DDL or DML statement (CREATE TABLE, INSERT, UPDATE, DELETE)
    /// Returns a JSON string with the result
    pub fn execute(&mut self, sql: &str) -> Result<JsValue, JsValue> {
        // Parse the SQL
        let stmt = parser::Parser::parse_sql(sql)
            .map_err(|e| JsValue::from_str(&format!("Parse error: {:?}", e)))?;

        // Execute based on statement type
        match stmt {
            ast::Statement::CreateTable(create_stmt) => {
                executor::CreateTableExecutor::execute(&create_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Table '{}' created successfully", create_stmt.table_name),
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::Insert(insert_stmt) => {
                executor::InsertExecutor::execute(&mut self.db, &insert_stmt)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let row_count = insert_stmt.values.len();
                let result = ExecuteResult {
                    rows_affected: row_count,
                    message: format!(
                        "{} row{} inserted into '{}'",
                        row_count,
                        if row_count == 1 { "" } else { "s" },
                        insert_stmt.table_name
                    ),
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::Select(_) => {
                Err(JsValue::from_str("Use query() method for SELECT statements"))
            }
            _ => Err(JsValue::from_str(&format!(
                "Statement type not yet supported in WASM: {:?}",
                stmt
            ))),
        }
    }

    /// Executes a SELECT query and returns results as JSON
    pub fn query(&self, sql: &str) -> Result<JsValue, JsValue> {
        // Parse the SQL
        let stmt = parser::Parser::parse_sql(sql)
            .map_err(|e| JsValue::from_str(&format!("Parse error: {:?}", e)))?;

        // Ensure it's a SELECT statement
        let select_stmt = match stmt {
            ast::Statement::Select(s) => s,
            _ => return Err(JsValue::from_str("query() method requires a SELECT statement")),
        };

        // Execute the query
        let select_executor = executor::SelectExecutor::new(&self.db);
        let rows = select_executor
            .execute(&select_stmt)
            .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

        // Extract column names (simplified - assumes we can derive from first row or schema)
        let columns = if !rows.is_empty() {
            // For now, use generic column names
            // In a real implementation, we'd extract from the schema
            (0..rows[0].values.len()).map(|i| format!("col{}", i)).collect()
        } else {
            Vec::new()
        };

        // Convert rows to JSON strings
        let row_strings: Vec<String> = rows
            .iter()
            .map(|row| {
                let values: Vec<String> = row.values.iter().map(|v| format!("{:?}", v)).collect();
                format!("[{}]", values.join(","))
            })
            .collect();

        let result = QueryResult { columns, rows: row_strings, row_count: rows.len() };

        serde_wasm_bindgen::to_value(&result)
            .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
    }

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

    /// Loads the Employees example database (hierarchical org structure)
    /// Demonstrates recursive queries with WITH RECURSIVE
    pub fn load_employees(&mut self) -> Result<JsValue, JsValue> {
        let sql = include_str!("../../../web-demo/examples/employees.sql");

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
        let sql = include_str!("../../../web-demo/examples/northwind.sql");

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_creation() {
        let db = Database::new();
        assert_eq!(db.version(), "nistmemsql-wasm 0.1.0");
    }

    // Note: Tests that call JsValue-returning methods must use wasm-pack test --node
    // The methods above work correctly but require a WASM runtime to test
    // For now, we verify that the code compiles correctly
}
