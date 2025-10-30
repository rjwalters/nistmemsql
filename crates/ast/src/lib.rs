//! Abstract Syntax Tree (AST) for SQL:1999
//!
//! This crate defines the structure of SQL statements and expressions
//! as parsed from SQL text. The AST is a tree representation that
//! preserves the semantic structure of SQL queries.

mod ddl;
mod dml;
mod expression;
mod operators;
mod select;
mod statement;

pub use ddl::{
    AddColumnStmt, AddConstraintStmt, AlterColumnStmt, AlterTableStmt, BeginStmt, ColumnConstraint,
    ColumnConstraintKind, ColumnDef, CommitStmt, CreateSchemaStmt, CreateTableStmt, DropColumnStmt,
    DropConstraintStmt, DropSchemaStmt, DropTableStmt, ReleaseSavepointStmt, RollbackStmt,
    RollbackToSavepointStmt, SavepointStmt, SetSchemaStmt, TableConstraint, TableConstraintKind,
};
pub use dml::{Assignment, DeleteStmt, InsertSource, InsertStmt, UpdateStmt};
pub use expression::{
    CaseWhen, Expression, FrameBound, FrameUnit, Quantifier, TrimPosition, WindowFrame,
    WindowFunctionSpec, WindowSpec,
};
pub use operators::{BinaryOperator, UnaryOperator};
pub use select::{
    CommonTableExpr, FromClause, JoinType, OrderByItem, OrderDirection, SelectItem, SelectStmt,
    SetOperation, SetOperator,
};
pub use statement::Statement;
