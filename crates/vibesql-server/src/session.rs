use anyhow::Result;
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Session state for a database connection
pub struct Session {
    /// Database name
    pub database: String,
    /// User name
    pub user: String,
    /// Database instance
    db: Database,
    /// Transaction state
    pub in_transaction: bool,
}

/// Simplified execution result for wire protocol
#[derive(Debug)]
pub enum ExecutionResult {
    Select {
        rows: Vec<Row>,
        columns: Vec<Column>,
    },
    Insert {
        rows_affected: usize,
    },
    Update {
        rows_affected: usize,
    },
    Delete {
        rows_affected: usize,
    },
    CreateTable,
    CreateIndex,
    CreateView,
    DropTable,
    DropIndex,
    DropView,
    Other {
        message: String,
    },
}

impl ExecutionResult {
    /// Get the statement type as a string for metrics
    pub fn statement_type(&self) -> &str {
        match self {
            ExecutionResult::Select { .. } => "SELECT",
            ExecutionResult::Insert { .. } => "INSERT",
            ExecutionResult::Update { .. } => "UPDATE",
            ExecutionResult::Delete { .. } => "DELETE",
            ExecutionResult::CreateTable => "CREATE_TABLE",
            ExecutionResult::CreateIndex => "CREATE_INDEX",
            ExecutionResult::CreateView => "CREATE_VIEW",
            ExecutionResult::DropTable => "DROP_TABLE",
            ExecutionResult::DropIndex => "DROP_INDEX",
            ExecutionResult::DropView => "DROP_VIEW",
            ExecutionResult::Other { .. } => "OTHER",
        }
    }

    /// Get the number of rows affected
    pub fn rows_affected(&self) -> u64 {
        match self {
            ExecutionResult::Select { rows, .. } => rows.len() as u64,
            ExecutionResult::Insert { rows_affected } => *rows_affected as u64,
            ExecutionResult::Update { rows_affected } => *rows_affected as u64,
            ExecutionResult::Delete { rows_affected } => *rows_affected as u64,
            _ => 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<vibesql_types::SqlValue>,
}

impl Session {
    /// Create a new session
    pub fn new(database: String, user: String) -> Result<Self> {
        let db = Database::new();

        Ok(Self {
            database,
            user,
            db,
            in_transaction: false,
        })
    }

    /// Execute a SQL query
    pub fn execute(&mut self, sql: &str) -> Result<ExecutionResult> {
        // Parse SQL
        let statement = Parser::parse_sql(sql).map_err(|e| anyhow::anyhow!("{}", e))?;

        // Execute based on statement type
        match statement {
            vibesql_ast::Statement::Select(select_stmt) => {
                let executor = vibesql_executor::SelectExecutor::new(&self.db);
                let rows = executor.execute(&select_stmt)?;

                // Convert to our result format
                let result_rows: Vec<Row> = rows.iter().map(|r| Row { values: r.values.clone() }).collect();

                // TODO: Get actual column names from select statement
                let columns = if !rows.is_empty() {
                    (0..rows[0].values.len())
                        .map(|i| Column { name: format!("col{}", i) })
                        .collect()
                } else {
                    Vec::new()
                };

                Ok(ExecutionResult::Select {
                    rows: result_rows,
                    columns,
                })
            }

            vibesql_ast::Statement::Insert(insert_stmt) => {
                let affected = vibesql_executor::InsertExecutor::execute(&mut self.db, &insert_stmt)?;
                Ok(ExecutionResult::Insert { rows_affected: affected })
            }

            vibesql_ast::Statement::Update(update_stmt) => {
                let affected = vibesql_executor::UpdateExecutor::execute(&update_stmt, &mut self.db)?;
                Ok(ExecutionResult::Update { rows_affected: affected })
            }

            vibesql_ast::Statement::Delete(delete_stmt) => {
                let affected = vibesql_executor::DeleteExecutor::execute(&delete_stmt, &mut self.db)?;
                Ok(ExecutionResult::Delete { rows_affected: affected })
            }

            vibesql_ast::Statement::CreateTable(create_stmt) => {
                vibesql_executor::CreateTableExecutor::execute(&create_stmt, &mut self.db)?;
                Ok(ExecutionResult::CreateTable)
            }

            vibesql_ast::Statement::CreateIndex(index_stmt) => {
                vibesql_executor::CreateIndexExecutor::execute(&index_stmt, &mut self.db)?;
                Ok(ExecutionResult::CreateIndex)
            }

            vibesql_ast::Statement::CreateView(view_stmt) => {
                vibesql_executor::advanced_objects::execute_create_view(&view_stmt, &mut self.db)?;
                Ok(ExecutionResult::CreateView)
            }

            vibesql_ast::Statement::DropTable(drop_stmt) => {
                vibesql_executor::DropTableExecutor::execute(&drop_stmt, &mut self.db)?;
                Ok(ExecutionResult::DropTable)
            }

            vibesql_ast::Statement::DropIndex(drop_stmt) => {
                vibesql_executor::DropIndexExecutor::execute(&drop_stmt, &mut self.db)?;
                Ok(ExecutionResult::DropIndex)
            }

            vibesql_ast::Statement::DropView(drop_stmt) => {
                vibesql_executor::advanced_objects::execute_drop_view(&drop_stmt, &mut self.db)?;
                Ok(ExecutionResult::DropView)
            }

            _ => {
                // For now, return a generic success for other statements
                Ok(ExecutionResult::Other {
                    message: "Command completed successfully".to_string(),
                })
            }
        }
    }

    /// Begin a transaction
    pub fn begin_transaction(&mut self) -> Result<()> {
        if self.in_transaction {
            return Err(anyhow::anyhow!("Already in transaction"));
        }
        self.in_transaction = true;
        Ok(())
    }

    /// Commit the current transaction
    pub fn commit(&mut self) -> Result<()> {
        if !self.in_transaction {
            return Err(anyhow::anyhow!("No transaction in progress"));
        }
        self.in_transaction = false;
        Ok(())
    }

    /// Rollback the current transaction
    pub fn rollback(&mut self) -> Result<()> {
        if !self.in_transaction {
            return Err(anyhow::anyhow!("No transaction in progress"));
        }
        self.in_transaction = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_creation() {
        let session = Session::new("testdb".to_string(), "testuser".to_string());
        assert!(session.is_ok());
        let session = session.unwrap();
        assert_eq!(session.database, "testdb");
        assert_eq!(session.user, "testuser");
        assert!(!session.in_transaction);
    }

    #[test]
    fn test_transaction_state() {
        let mut session = Session::new("testdb".to_string(), "testuser".to_string()).unwrap();

        // Not in transaction initially
        assert!(!session.in_transaction);

        // Begin transaction
        assert!(session.begin_transaction().is_ok());
        assert!(session.in_transaction);

        // Can't begin twice
        assert!(session.begin_transaction().is_err());

        // Commit
        assert!(session.commit().is_ok());
        assert!(!session.in_transaction);

        // Can't commit when not in transaction
        assert!(session.commit().is_err());
    }
}
