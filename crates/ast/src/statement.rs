//! Top-level SQL statement types
//!
//! This module defines the Statement enum that represents all possible SQL statements.

use crate::{
    AlterSequenceStmt, AlterTableStmt, BeginStmt, CommitStmt, CreateCharacterSetStmt,
    CreateCollationStmt, CreateDomainStmt, CreateRoleStmt, CreateSchemaStmt, CreateSequenceStmt,
    CreateTableStmt, CreateTranslationStmt, CreateTriggerStmt, CreateTypeStmt, CreateViewStmt,
    DeleteStmt, DropCharacterSetStmt, DropCollationStmt, DropDomainStmt, DropRoleStmt,
    DropSchemaStmt, DropSequenceStmt, DropTableStmt, DropTranslationStmt, DropTriggerStmt,
    DropTypeStmt, DropViewStmt, GrantStmt, InsertStmt, ReleaseSavepointStmt, RevokeStmt,
    RollbackStmt, RollbackToSavepointStmt, SavepointStmt, SelectStmt, SetCatalogStmt, SetNamesStmt,
    SetSchemaStmt, SetTimeZoneStmt, UpdateStmt,
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
    SetCatalog(SetCatalogStmt),
    SetNames(SetNamesStmt),
    SetTimeZone(SetTimeZoneStmt),
    CreateRole(CreateRoleStmt),
    DropRole(DropRoleStmt),
    BeginTransaction(BeginStmt),
    Commit(CommitStmt),
    Rollback(RollbackStmt),
    Savepoint(SavepointStmt),
    RollbackToSavepoint(RollbackToSavepointStmt),
    ReleaseSavepoint(ReleaseSavepointStmt),
    Grant(GrantStmt),
    Revoke(RevokeStmt),
    // Advanced SQL object statements (SQL:1999)
    CreateDomain(CreateDomainStmt),
    DropDomain(DropDomainStmt),
    CreateSequence(CreateSequenceStmt),
    AlterSequence(AlterSequenceStmt),
    DropSequence(DropSequenceStmt),
    CreateType(CreateTypeStmt),
    DropType(DropTypeStmt),
    CreateCollation(CreateCollationStmt),
    DropCollation(DropCollationStmt),
    CreateCharacterSet(CreateCharacterSetStmt),
    DropCharacterSet(DropCharacterSetStmt),
    CreateTranslation(CreateTranslationStmt),
    DropTranslation(DropTranslationStmt),
    CreateView(CreateViewStmt),
    DropView(DropViewStmt),
    CreateTrigger(CreateTriggerStmt),
    DropTrigger(DropTriggerStmt),
    // TODO: Add more statement types (ALTER, etc.)
}
