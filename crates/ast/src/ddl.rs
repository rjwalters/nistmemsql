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
    pub default_value: Option<Box<Expression>>,
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
    pub schema_elements: Vec<SchemaElement>,
}

/// Schema element that can be included in CREATE SCHEMA
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaElement {
    CreateTable(CreateTableStmt),
    // Future: CreateView, Grant, etc.
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

/// CREATE ROLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateRoleStmt {
    pub role_name: String,
}

/// DROP ROLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropRoleStmt {
    pub role_name: String,
}

// ============================================================================
// Advanced SQL Object Statements (SQL:1999)
// ============================================================================

/// CREATE DOMAIN statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateDomainStmt {
    pub domain_name: String,
    pub data_type: DataType,
    pub default: Option<Box<Expression>>,
    pub constraints: Vec<DomainConstraint>,
}

/// Domain constraint (CHECK constraint on domain values)
#[derive(Debug, Clone, PartialEq)]
pub struct DomainConstraint {
    pub name: Option<String>,
    pub check: Box<Expression>,
}

/// DROP DOMAIN statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropDomainStmt {
    pub domain_name: String,
    pub cascade: bool, // true for CASCADE, false for RESTRICT
}

/// CREATE SEQUENCE statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateSequenceStmt {
    pub sequence_name: String,
    pub start_with: Option<i64>,
    pub increment_by: i64, // default: 1
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub cycle: bool, // default: false
}

/// DROP SEQUENCE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropSequenceStmt {
    pub sequence_name: String,
    pub cascade: bool, // true for CASCADE, false for RESTRICT
}

/// ALTER SEQUENCE statement
#[derive(Debug, Clone, PartialEq)]
pub struct AlterSequenceStmt {
    pub sequence_name: String,
    pub restart_with: Option<i64>,
    pub increment_by: Option<i64>,
    pub min_value: Option<Option<i64>>, // None = no change, Some(None) = NO MINVALUE, Some(Some(n)) = MINVALUE n
    pub max_value: Option<Option<i64>>, // None = no change, Some(None) = NO MAXVALUE, Some(Some(n)) = MAXVALUE n
    pub cycle: Option<bool>,
}

/// CREATE TYPE statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTypeStmt {
    pub type_name: String,
}

/// DROP TYPE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropTypeStmt {
    pub type_name: String,
}

/// CREATE COLLATION statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateCollationStmt {
    pub collation_name: String,
}

/// DROP COLLATION statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropCollationStmt {
    pub collation_name: String,
}

/// CREATE CHARACTER SET statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateCharacterSetStmt {
    pub charset_name: String,
}

/// DROP CHARACTER SET statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropCharacterSetStmt {
    pub charset_name: String,
}

/// CREATE TRANSLATION statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTranslationStmt {
    pub translation_name: String,
}

/// DROP TRANSLATION statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropTranslationStmt {
    pub translation_name: String,
}
