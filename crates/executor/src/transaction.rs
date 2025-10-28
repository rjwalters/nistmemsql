//! Transaction control statement execution (BEGIN, COMMIT, ROLLBACK)

use ast::{BeginStmt, CommitStmt, RollbackStmt};
use storage::Database;

use crate::errors::ExecutorError;

/// Executor for BEGIN TRANSACTION statements
pub struct BeginTransactionExecutor;

impl BeginTransactionExecutor {
    /// Execute a BEGIN TRANSACTION statement
    pub fn execute(_stmt: &BeginStmt, db: &mut Database) -> Result<String, ExecutorError> {
        db.begin_transaction()
            .map_err(|e| ExecutorError::StorageError(format!("Failed to begin transaction: {}", e)))?;

        Ok(format!("Transaction started"))
    }
}

/// Executor for COMMIT statements
pub struct CommitExecutor;

impl CommitExecutor {
    /// Execute a COMMIT statement
    pub fn execute(_stmt: &CommitStmt, db: &mut Database) -> Result<String, ExecutorError> {
        db.commit_transaction()
            .map_err(|e| ExecutorError::StorageError(format!("Failed to commit transaction: {}", e)))?;

        Ok(format!("Transaction committed"))
    }
}

/// Executor for ROLLBACK statements
pub struct RollbackExecutor;

impl RollbackExecutor {
    /// Execute a ROLLBACK statement
    pub fn execute(_stmt: &RollbackStmt, db: &mut Database) -> Result<String, ExecutorError> {
        db.rollback_transaction()
            .map_err(|e| ExecutorError::StorageError(format!("Failed to rollback transaction: {}", e)))?;

        Ok(format!("Transaction rolled back"))
    }
}
