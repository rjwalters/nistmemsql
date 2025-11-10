//! Transaction control statement execution (BEGIN, COMMIT, ROLLBACK)

use vibesql_ast::{
    BeginStmt, CommitStmt, ReleaseSavepointStmt, RollbackStmt, RollbackToSavepointStmt,
    SavepointStmt,
};
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Executor for BEGIN TRANSACTION statements
pub struct BeginTransactionExecutor;

impl BeginTransactionExecutor {
    /// Execute a BEGIN TRANSACTION statement
    pub fn execute(_stmt: &BeginStmt, db: &mut Database) -> Result<String, ExecutorError> {
        db.begin_transaction().map_err(|e| {
            ExecutorError::StorageError(format!("Failed to begin transaction: {}", e))
        })?;

        Ok("Transaction started".to_string())
    }
}

/// Executor for COMMIT statements
pub struct CommitExecutor;

impl CommitExecutor {
    /// Execute a COMMIT statement
    pub fn execute(_stmt: &CommitStmt, db: &mut Database) -> Result<String, ExecutorError> {
        db.commit_transaction().map_err(|e| {
            ExecutorError::StorageError(format!("Failed to commit transaction: {}", e))
        })?;

        Ok("Transaction committed".to_string())
    }
}

/// Executor for ROLLBACK statements
pub struct RollbackExecutor;

impl RollbackExecutor {
    /// Execute a ROLLBACK statement
    pub fn execute(_stmt: &RollbackStmt, db: &mut Database) -> Result<String, ExecutorError> {
        db.rollback_transaction().map_err(|e| {
            ExecutorError::StorageError(format!("Failed to rollback transaction: {}", e))
        })?;

        Ok("Transaction rolled back".to_string())
    }
}

/// Executor for SAVEPOINT statements
pub struct SavepointExecutor;

impl SavepointExecutor {
    /// Execute a SAVEPOINT statement
    pub fn execute(stmt: &SavepointStmt, db: &mut Database) -> Result<String, ExecutorError> {
        db.create_savepoint(stmt.name.clone()).map_err(|e| {
            ExecutorError::StorageError(format!("Failed to create savepoint: {}", e))
        })?;

        Ok(format!("Savepoint '{}' created", stmt.name))
    }
}

/// Executor for ROLLBACK TO SAVEPOINT statements
pub struct RollbackToSavepointExecutor;

impl RollbackToSavepointExecutor {
    /// Execute a ROLLBACK TO SAVEPOINT statement
    pub fn execute(
        stmt: &RollbackToSavepointStmt,
        db: &mut Database,
    ) -> Result<String, ExecutorError> {
        db.rollback_to_savepoint(stmt.name.clone()).map_err(|e| {
            ExecutorError::StorageError(format!("Failed to rollback to savepoint: {}", e))
        })?;

        Ok(format!("Rolled back to savepoint '{}'", stmt.name))
    }
}

/// Executor for RELEASE SAVEPOINT statements
pub struct ReleaseSavepointExecutor;

impl ReleaseSavepointExecutor {
    /// Execute a RELEASE SAVEPOINT statement
    pub fn execute(
        stmt: &ReleaseSavepointStmt,
        db: &mut Database,
    ) -> Result<String, ExecutorError> {
        db.release_savepoint(stmt.name.clone()).map_err(|e| {
            ExecutorError::StorageError(format!("Failed to release savepoint: {}", e))
        })?;

        Ok(format!("Savepoint '{}' released", stmt.name))
    }
}
