//! Top-level SQL statement types
//!
//! This module defines the Statement enum that represents all possible SQL statements.

use crate::{
    AlterSequenceStmt, AlterTableStmt, BeginStmt, CallStmt, CloseCursorStmt, CommitStmt,
    CreateAssertionStmt, CreateCharacterSetStmt, CreateCollationStmt, CreateDomainStmt,
    CreateFunctionStmt, CreateIndexStmt, CreateProcedureStmt, CreateRoleStmt, CreateSchemaStmt,
    CreateSequenceStmt, CreateTableStmt, CreateTranslationStmt, CreateTriggerStmt, CreateTypeStmt,
    CreateViewStmt, DeclareCursorStmt, DeleteStmt, DropAssertionStmt, DropCharacterSetStmt,
    DropCollationStmt, DropDomainStmt, DropFunctionStmt, DropIndexStmt, DropProcedureStmt,
    DropRoleStmt, DropSchemaStmt, DropSequenceStmt, DropTableStmt, DropTranslationStmt,
    DropTriggerStmt, DropTypeStmt, DropViewStmt, FetchStmt, GrantStmt, InsertStmt, OpenCursorStmt,
    ReleaseSavepointStmt, RevokeStmt, RollbackStmt, RollbackToSavepointStmt, SavepointStmt,
    SelectStmt, SetCatalogStmt, SetNamesStmt, SetSchemaStmt, SetTimeZoneStmt, SetTransactionStmt,
    UpdateStmt,
};

// ============================================================================
// Top-level SQL Statements
// ============================================================================

/// A complete SQL statement
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(Box<SelectStmt>),
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
    SetTransaction(SetTransactionStmt),
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
    CreateIndex(CreateIndexStmt),
    DropIndex(DropIndexStmt),
    CreateAssertion(CreateAssertionStmt),
    DropAssertion(DropAssertionStmt),
    // Cursor operations (SQL:1999 Feature E121)
    DeclareCursor(DeclareCursorStmt),
    OpenCursor(OpenCursorStmt),
    Fetch(FetchStmt),
    CloseCursor(CloseCursorStmt),
    // Stored procedures and functions (SQL:1999 Feature P001)
    CreateProcedure(CreateProcedureStmt),
    DropProcedure(DropProcedureStmt),
    CreateFunction(CreateFunctionStmt),
    DropFunction(DropFunctionStmt),
    Call(CallStmt),
}
