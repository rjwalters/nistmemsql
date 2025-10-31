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
    BeginStmt, ColumnConstraint, ColumnConstraintKind, ColumnDef, CommitStmt,
    CreateCharacterSetStmt, CreateCollationStmt, CreateDomainStmt, CreateRoleStmt,
    CreateSchemaStmt, CreateSequenceStmt, CreateTableStmt, CreateTranslationStmt, CreateTypeStmt,
    CreateViewStmt, DomainConstraint, DropBehavior, DropCharacterSetStmt, DropCollationStmt,
    DropColumnStmt, DropConstraintStmt, DropDomainStmt, DropRoleStmt, DropSchemaStmt,
    DropSequenceStmt, DropTableStmt, DropTranslationStmt, DropTypeStmt, DropViewStmt,
    ReferentialAction, ReleaseSavepointStmt, RollbackStmt, RollbackToSavepointStmt, SavepointStmt,
    SchemaElement, SetCatalogStmt, SetNamesStmt, SetSchemaStmt, SetTimeZoneStmt, TableConstraint,
    TableConstraintKind, TimeZoneSpec, TypeAttribute, TypeDefinition,
};
pub use dml::{Assignment, DeleteStmt, InsertSource, InsertStmt, UpdateStmt};
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
