//! Abstract Syntax Tree (AST) for SQL:1999
//!
//! This crate defines the structure of SQL statements and expressions
//! as parsed from SQL text. The AST is a tree representation that
//! preserves the semantic structure of SQL queries.

mod ddl;
mod dml;
mod expression;
mod grant;
mod operators;
mod revoke;
mod select;
mod statement;

pub use ddl::{
    AddColumnStmt, AddConstraintStmt, AlterColumnStmt, AlterSequenceStmt, AlterTableStmt,
    BeginStmt, CloseCursorStmt, ColumnConstraint, ColumnConstraintKind, ColumnDef, CommitStmt,
    CreateAssertionStmt, CreateCharacterSetStmt, CreateCollationStmt, CreateDomainStmt,
    CreateIndexStmt, CreateRoleStmt, CreateSchemaStmt, CreateSequenceStmt, CreateTableStmt,
    CreateTranslationStmt, CreateTriggerStmt, CreateTypeStmt, CreateViewStmt, CursorUpdatability,
    DeclareCursorStmt, DomainConstraint, DropAssertionStmt, DropBehavior, DropCharacterSetStmt,
    DropCollationStmt, DropColumnStmt, DropConstraintStmt, DropDomainStmt, DropIndexStmt,
    DropRoleStmt, DropSchemaStmt, DropSequenceStmt, DropTableStmt, DropTranslationStmt,
    DropTriggerStmt, DropTypeStmt, DropViewStmt, FetchOrientation, FetchStmt, IndexColumn, IsolationLevel,
    OpenCursorStmt, ReferentialAction, ReleaseSavepointStmt, RollbackStmt,
    RollbackToSavepointStmt, SavepointStmt, SchemaElement, SetCatalogStmt, SetNamesStmt,
    SetSchemaStmt, SetTimeZoneStmt, SetTransactionStmt, TableConstraint, TableConstraintKind,
    TimeZoneSpec, TransactionAccessMode, TriggerAction, TriggerEvent, TriggerGranularity,
    TriggerTiming, TypeAttribute, TypeDefinition,
};
pub use dml::{Assignment, DeleteStmt, InsertSource, InsertStmt, UpdateStmt, WhereClause};
pub use expression::{
    CaseWhen, CharacterUnit, Expression, FrameBound, FrameUnit, Quantifier, TrimPosition,
    WindowFrame, WindowFunctionSpec, WindowSpec,
};
pub use grant::{GrantStmt, ObjectType, PrivilegeType};
pub use operators::{BinaryOperator, UnaryOperator};
pub use revoke::{CascadeOption, RevokeStmt};
pub use select::{
    CommonTableExpr, FromClause, JoinType, OrderByItem, OrderDirection, SelectItem, SelectStmt,
    SetOperation, SetOperator,
};
pub use statement::Statement;
