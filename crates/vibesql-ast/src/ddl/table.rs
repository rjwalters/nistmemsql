//! Table DDL operations
//!
//! This module contains AST nodes for table-related DDL operations:
//! - CREATE TABLE
//! - DROP TABLE
//! - ALTER TABLE (add/drop column, add/drop constraint, etc.)

use vibesql_types::DataType;

use crate::Expression;

/// Referential action for foreign key constraints
#[derive(Debug, Clone, PartialEq)]
pub enum ReferentialAction {
    NoAction,
    Cascade,
    SetNull,
    SetDefault,
}

/// MySQL table options for CREATE TABLE
#[derive(Debug, Clone, PartialEq)]
pub enum TableOption {
    /// KEY_BLOCK_SIZE [=] value
    KeyBlockSize(Option<i64>),
    /// CONNECTION [=] 'string'
    Connection(Option<String>),
    /// INSERT_METHOD = {FIRST | LAST | NO}
    InsertMethod(InsertMethod),
    /// UNION [=] (col1, col2, ...)
    Union(Option<Vec<String>>),
    /// ROW_FORMAT [=] {DEFAULT | DYNAMIC | FIXED | COMPRESSED | REDUNDANT | COMPACT}
    RowFormat(Option<RowFormat>),
    /// DELAY_KEY_WRITE [=] value
    DelayKeyWrite(Option<i64>),
    /// TABLE_CHECKSUM [=] value | CHECKSUM [=] value
    TableChecksum(Option<i64>),
    /// STATS_SAMPLE_PAGES [=] value
    StatsSamplePages(Option<i64>),
    /// PASSWORD [=] 'string'
    Password(Option<String>),
    /// AVG_ROW_LENGTH [=] value
    AvgRowLength(Option<i64>),
    /// MIN_ROWS [=] value
    MinRows(Option<i64>),
    /// MAX_ROWS [=] value
    MaxRows(Option<i64>),
    /// SECONDARY_ENGINE [=] identifier | NULL
    SecondaryEngine(Option<String>),
    /// COLLATE [=] collation_name
    Collate(Option<String>),
    /// COMMENT [=] 'string'
    Comment(Option<String>),
}

/// MySQL INSERT_METHOD values
#[derive(Debug, Clone, PartialEq)]
pub enum InsertMethod {
    First,
    Last,
    No,
}

/// MySQL ROW_FORMAT values
#[derive(Debug, Clone, PartialEq)]
pub enum RowFormat {
    Default,
    Dynamic,
    Fixed,
    Compressed,
    Redundant,
    Compact,
}

/// CREATE TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStmt {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
    pub table_constraints: Vec<TableConstraint>,
    pub table_options: Vec<TableOption>,
}

/// Column definition
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub constraints: Vec<ColumnConstraint>,
    pub default_value: Option<Box<Expression>>,
    pub comment: Option<String>,
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
    References {
        table: String,
        column: String,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
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
    PrimaryKey {
        columns: Vec<String>,
    },
    ForeignKey {
        columns: Vec<String>,
        references_table: String,
        references_columns: Vec<String>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
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
