//! Top-level SQL statement types
//!
//! This module defines the Statement enum that represents all possible SQL statements.

use crate::{BeginStmt, CommitStmt, CreateSchemaStmt, CreateTableStmt, DeleteStmt, DropSchemaStmt, DropTableStmt, InsertStmt, RollbackStmt, SelectStmt, SetSchemaStmt, UpdateStmt};

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
    CreateSchema(CreateSchemaStmt),
    DropSchema(DropSchemaStmt),
    SetSchema(SetSchemaStmt),
    BeginTransaction(BeginStmt),
    Commit(CommitStmt),
    Rollback(RollbackStmt),
    // TODO: Add more statement types (ALTER, etc.)
}
