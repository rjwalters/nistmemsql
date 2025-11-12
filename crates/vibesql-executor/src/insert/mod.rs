mod bulk_transfer;
mod constraints;
mod defaults;
mod execution;
mod foreign_keys;
mod replace;
mod row_validator;
mod validation;

use crate::errors::ExecutorError;

/// Executor for INSERT statements
pub struct InsertExecutor;

impl InsertExecutor {
    /// Execute an INSERT statement
    /// Returns number of rows inserted
    pub fn execute(
        db: &mut vibesql_storage::Database,
        stmt: &vibesql_ast::InsertStmt,
    ) -> Result<usize, ExecutorError> {
        execution::execute_insert(db, stmt)
    }
}
