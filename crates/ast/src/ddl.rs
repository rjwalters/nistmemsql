//! Data Definition Language (DDL) AST nodes

use crate::Expression;
use types::DataType;

/// CREATE TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStmt {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
    pub table_constraints: Vec<TableConstraint>,
}

/// Column definition
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub constraints: Vec<ColumnConstraint>,
}

/// Column-level constraint
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnConstraint {
    pub name: Option<String>,
    pub kind: ColumnConstraintKind,
}

/// Column constraint types
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraintKind {
    NotNull,
    PrimaryKey,
    Unique,
    Check(Box<Expression>),
    References { table: String, column: String },
}

/// Table-level constraint
#[derive(Debug, Clone, PartialEq)]
pub struct TableConstraint {
    pub name: Option<String>,
    pub kind: TableConstraintKind,
}

/// Table constraint types
#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraintKind {
    PrimaryKey { columns: Vec<String> },
    ForeignKey { columns: Vec<String>, references_table: String, references_columns: Vec<String> },
    Unique { columns: Vec<String> },
    Check { expr: Box<Expression> },
}

/// DROP TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStmt {
    pub table_name: String,
    pub if_exists: bool,
}

/// BEGIN TRANSACTION statement
#[derive(Debug, Clone, PartialEq)]
pub struct BeginStmt;

/// COMMIT statement
#[derive(Debug, Clone, PartialEq)]
pub struct CommitStmt;

/// ROLLBACK statement
#[derive(Debug, Clone, PartialEq)]
pub struct RollbackStmt;

/// ALTER TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableStmt {
    AddColumn(AddColumnStmt),
    DropColumn(DropColumnStmt),
    AlterColumn(AlterColumnStmt),
    AddConstraint(AddConstraintStmt),
    DropConstraint(DropConstraintStmt),
}

/// ADD COLUMN operation
#[derive(Debug, Clone, PartialEq)]
pub struct AddColumnStmt {
    pub table_name: String,
    pub column_def: ColumnDef,
}

/// DROP COLUMN operation
#[derive(Debug, Clone, PartialEq)]
pub struct DropColumnStmt {
    pub table_name: String,
    pub column_name: String,
    pub if_exists: bool,
}

/// ALTER COLUMN operation
#[derive(Debug, Clone, PartialEq)]
pub enum AlterColumnStmt {
    SetDefault { table_name: String, column_name: String, default: Expression },
    DropDefault { table_name: String, column_name: String },
    SetNotNull { table_name: String, column_name: String },
    DropNotNull { table_name: String, column_name: String },
}

/// ADD CONSTRAINT operation
#[derive(Debug, Clone, PartialEq)]
pub struct AddConstraintStmt {
    pub table_name: String,
    pub constraint: TableConstraint,
}

/// DROP CONSTRAINT operation
#[derive(Debug, Clone, PartialEq)]
pub struct DropConstraintStmt {
    pub table_name: String,
    pub constraint_name: String,
}

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

/// CREATE SCHEMA statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateSchemaStmt {
    pub schema_name: String,
    pub if_not_exists: bool,
}

/// DROP SCHEMA statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropSchemaStmt {
    pub schema_name: String,
    pub if_exists: bool,
    pub cascade: bool,
}

/// SET SCHEMA statement
#[derive(Debug, Clone, PartialEq)]
pub struct SetSchemaStmt {
    pub schema_name: String,
}
