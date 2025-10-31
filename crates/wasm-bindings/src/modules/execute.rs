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
            ast::Statement::SetCatalog(set_stmt) => {
                let message = executor::SchemaExecutor::execute_set_catalog(&set_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message,
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::SetNames(set_stmt) => {
                let message = executor::SchemaExecutor::execute_set_names(&set_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message,
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::SetTimeZone(set_stmt) => {
                let message = executor::SchemaExecutor::execute_set_time_zone(&set_stmt, &mut self.db)
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
            ast::Statement::Revoke(revoke_stmt) => {
                let message = executor::RevokeExecutor::execute_revoke(&revoke_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message,
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            // Advanced SQL:1999 objects
            ast::Statement::CreateDomain(stmt) => {
                executor::advanced_objects::execute_create_domain(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Domain '{}' created successfully", stmt.domain_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::DropDomain(stmt) => {
                executor::advanced_objects::execute_drop_domain(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Domain '{}' dropped successfully", stmt.domain_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::CreateSequence(stmt) => {
                executor::advanced_objects::execute_create_sequence(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Sequence '{}' created successfully", stmt.sequence_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::DropSequence(stmt) => {
                executor::advanced_objects::execute_drop_sequence(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Sequence '{}' dropped successfully", stmt.sequence_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::CreateType(stmt) => {
                executor::advanced_objects::execute_create_type(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Type '{}' created successfully", stmt.type_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::DropType(stmt) => {
                executor::advanced_objects::execute_drop_type(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Type '{}' dropped successfully", stmt.type_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::CreateCollation(stmt) => {
                executor::advanced_objects::execute_create_collation(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Collation '{}' created successfully", stmt.collation_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::DropCollation(stmt) => {
                executor::advanced_objects::execute_drop_collation(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Collation '{}' dropped successfully", stmt.collation_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::CreateCharacterSet(stmt) => {
                executor::advanced_objects::execute_create_character_set(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Character set '{}' created successfully", stmt.charset_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::DropCharacterSet(stmt) => {
                executor::advanced_objects::execute_drop_character_set(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Character set '{}' dropped successfully", stmt.charset_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::CreateTranslation(stmt) => {
                executor::advanced_objects::execute_create_translation(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Translation '{}' created successfully", stmt.translation_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::DropTranslation(stmt) => {
                executor::advanced_objects::execute_drop_translation(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Translation '{}' dropped successfully", stmt.translation_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::CreateView(stmt) => {
                executor::advanced_objects::execute_create_view(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("View '{}' created successfully", stmt.view_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            ast::Statement::DropView(stmt) => {
                executor::advanced_objects::execute_drop_view(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("View '{}' dropped successfully", stmt.view_name),
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
