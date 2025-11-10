//! Transaction control DDL operations
//!
//! This module contains AST nodes for transaction control statements:
//! - BEGIN TRANSACTION
//! - COMMIT
//! - ROLLBACK
//! - SAVEPOINT
//! - SET TRANSACTION

/// BEGIN TRANSACTION statement
#[derive(Debug, Clone, PartialEq)]
pub struct BeginStmt;

/// COMMIT statement
#[derive(Debug, Clone, PartialEq)]
pub struct CommitStmt;

/// ROLLBACK statement
#[derive(Debug, Clone, PartialEq)]
pub struct RollbackStmt;

/// SAVEPOINT statement
#[derive(Debug, Clone, PartialEq)]
pub struct SavepointStmt {
    pub name: String,
}

/// ROLLBACK TO SAVEPOINT statement
#[derive(Debug, Clone, PartialEq)]
pub struct RollbackToSavepointStmt {
    pub name: String,
}

/// RELEASE SAVEPOINT statement
#[derive(Debug, Clone, PartialEq)]
pub struct ReleaseSavepointStmt {
    pub name: String,
}

/// Transaction isolation level
#[derive(Debug, Clone, PartialEq)]
pub enum IsolationLevel {
    Serializable,
}

/// Transaction access mode
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionAccessMode {
    ReadOnly,
    ReadWrite,
}

/// SET TRANSACTION statement (SQL:1999 Feature E152)
#[derive(Debug, Clone, PartialEq)]
pub struct SetTransactionStmt {
    pub local: bool, // true for SET LOCAL TRANSACTION, false for SET TRANSACTION
    pub isolation_level: Option<IsolationLevel>,
    pub access_mode: Option<TransactionAccessMode>,
}
