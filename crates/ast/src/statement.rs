//! Top-level SQL statement types
//!
//! This module defines the Statement enum that represents all possible SQL statements.

use crate::{
    AlterTableStmt, BeginStmt, CommitStmt, CreateRoleStmt, CreateSchemaStmt, CreateTableStmt,
    DeleteStmt, DropRoleStmt, DropSchemaStmt, DropTableStmt, GrantStmt, InsertStmt,
    ReleaseSavepointStmt, RollbackStmt, RollbackToSavepointStmt, SavepointStmt, SelectStmt,
    SetSchemaStmt, UpdateStmt,
};

// ============================================================================
// Top-level SQL Statements
// ============================================================================

/// A complete SQL statement
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    AlterTable(AlterTableStmt),
    CreateSchema(CreateSchemaStmt),
    DropSchema(DropSchemaStmt),
    SetSchema(SetSchemaStmt),
    CreateRole(CreateRoleStmt),
    DropRole(DropRoleStmt),
    BeginTransaction(BeginStmt),
    Commit(CommitStmt),
    Rollback(RollbackStmt),
    Savepoint(SavepointStmt),
    RollbackToSavepoint(RollbackToSavepointStmt),
    ReleaseSavepoint(ReleaseSavepointStmt),
    Grant(GrantStmt),
    // TODO: Add more statement types (ALTER, etc.)
}
