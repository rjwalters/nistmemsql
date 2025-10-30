//! DML and transaction execution for WASM bindings
//!
//! This module handles data manipulation (INSERT, UPDATE, DELETE) and
//! transaction control (BEGIN, COMMIT, ROLLBACK, SAVEPOINT) operations.

use crate::{Database, ExecuteResult};
use wasm_bindgen::prelude::*;

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

                let result = ExecuteResult { rows_affected: 0, message };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::Insert(insert_stmt) => {
                let row_count = executor::InsertExecutor::execute(&mut self.db, &insert_stmt)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

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
                let message =
                    executor::BeginTransactionExecutor::execute(&begin_stmt, &mut self.db)
                        .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult { rows_affected: 0, message };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::Commit(commit_stmt) => {
                let message = executor::CommitExecutor::execute(&commit_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult { rows_affected: 0, message };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::Rollback(rollback_stmt) => {
                let message = executor::RollbackExecutor::execute(&rollback_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult { rows_affected: 0, message };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::Savepoint(savepoint_stmt) => {
                let message =
                    executor::SavepointExecutor::execute(&savepoint_stmt, &mut self.db)
                        .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult { rows_affected: 0, message };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::RollbackToSavepoint(rollback_to_stmt) => {
                let message =
                    executor::RollbackToSavepointExecutor::execute(&rollback_to_stmt, &mut self.db)
                        .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult { rows_affected: 0, message };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::ReleaseSavepoint(release_stmt) => {
                let message =
                    executor::ReleaseSavepointExecutor::execute(&release_stmt, &mut self.db)
                        .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult { rows_affected: 0, message };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::CreateRole(create_role_stmt) => {
                let message =
                    executor::RoleExecutor::execute_create_role(&create_role_stmt, &mut self.db)
                        .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult { rows_affected: 0, message };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::DropRole(drop_role_stmt) => {
                let message =
                    executor::RoleExecutor::execute_drop_role(&drop_role_stmt, &mut self.db)
                        .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult { rows_affected: 0, message };

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
