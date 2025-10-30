//! DDL/DML execution methods

use super::database::Database;
use super::types::ExecuteResult;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
impl Database {
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
            ast::Statement::DropTable(drop_stmt) => {
                let message = executor::DropTableExecutor::execute(&drop_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message,
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
            ast::Statement::BeginTransaction(begin_stmt) => {
                let message = executor::BeginTransactionExecutor::execute(&begin_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message,
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::Commit(commit_stmt) => {
                let message = executor::CommitExecutor::execute(&commit_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message,
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::Rollback(rollback_stmt) => {
                let message = executor::RollbackExecutor::execute(&rollback_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message,
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::AlterTable(alter_stmt) => {
                let message = executor::AlterTableExecutor::execute(&alter_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message,
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::CreateSchema(create_stmt) => {
                let message = executor::SchemaExecutor::execute_create_schema(&create_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message,
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::DropSchema(drop_stmt) => {
                let message = executor::SchemaExecutor::execute_drop_schema(&drop_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message,
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::SetSchema(set_stmt) => {
                let message = executor::SchemaExecutor::execute_set_schema(&set_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message,
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::Grant(grant_stmt) => {
                let message = executor::GrantExecutor::execute_grant(&grant_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message,
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            _ => Err(JsValue::from_str(&format!(
                "Statement type not yet supported in WASM: {:?}",
                stmt
            ))),
        }
    }
}
