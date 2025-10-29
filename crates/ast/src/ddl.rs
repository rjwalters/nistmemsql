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
pub enum ColumnConstraint {
    PrimaryKey,
    Unique,
    Check(Box<Expression>),
    References {
        table: String,
        column: String,
    },
}

/// Table-level constraint
#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
    PrimaryKey {
        columns: Vec<String>,
    },
    ForeignKey {
        columns: Vec<String>,
        references_table: String,
        references_columns: Vec<String>,
    },
    Unique {
        columns: Vec<String>,
    },
    Check {
        expr: Box<Expression>,
    },
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
