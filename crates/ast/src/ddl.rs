//! Data Definition Language (DDL) AST nodes

use crate::Expression;
use types::DataType;

/// Referential action for foreign key constraints
#[derive(Debug, Clone, PartialEq)]
pub enum ReferentialAction {
    NoAction,
    Cascade,
    SetNull,
    SetDefault,
}

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

/// SET CATALOG statement
#[derive(Debug, Clone, PartialEq)]
pub struct SetCatalogStmt {
    pub catalog_name: String,
}

/// SET NAMES statement
#[derive(Debug, Clone, PartialEq)]
pub struct SetNamesStmt {
    pub charset_name: String,
    pub collation: Option<String>,
}

/// SET TIME ZONE statement
#[derive(Debug, Clone, PartialEq)]
pub struct SetTimeZoneStmt {
    pub zone: TimeZoneSpec,
}

/// Time zone specification for SET TIME ZONE
#[derive(Debug, Clone, PartialEq)]
pub enum TimeZoneSpec {
    Local,
    Interval(String), // e.g., "+05:00"
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
    pub definition: TypeDefinition,
}

/// Type definition (distinct or structured)
#[derive(Debug, Clone, PartialEq)]
pub enum TypeDefinition {
    Distinct { base_type: DataType },
    Structured { attributes: Vec<TypeAttribute> },
}

/// Attribute in a structured type
#[derive(Debug, Clone, PartialEq)]
pub struct TypeAttribute {
    pub name: String,
    pub data_type: DataType,
}

/// DROP TYPE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropTypeStmt {
    pub type_name: String,
    pub behavior: DropBehavior,
}

/// Drop behavior for CASCADE/RESTRICT
#[derive(Debug, Clone, PartialEq)]
pub enum DropBehavior {
    Cascade,
    Restrict,
}

/// CREATE COLLATION statement
///
/// SQL:1999 Syntax:
///   CREATE COLLATION collation_name
///     [FOR character_set]
///     [FROM source_collation]
///     [PAD SPACE | NO PAD]
#[derive(Debug, Clone, PartialEq)]
pub struct CreateCollationStmt {
    pub collation_name: String,
    pub character_set: Option<String>,    // FOR character_set
    pub source_collation: Option<String>, // FROM source_collation
    pub pad_space: Option<bool>,          // PAD SPACE (true) | NO PAD (false)
}

/// DROP COLLATION statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropCollationStmt {
    pub collation_name: String,
}

/// CREATE CHARACTER SET statement
///
/// SQL:1999 Syntax:
///   CREATE CHARACTER SET charset_name [AS]
///     [GET source]
///     [COLLATE FROM collation]
#[derive(Debug, Clone, PartialEq)]
pub struct CreateCharacterSetStmt {
    pub charset_name: String,
    pub source: Option<String>,    // GET source
    pub collation: Option<String>, // COLLATE FROM collation
}

/// DROP CHARACTER SET statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropCharacterSetStmt {
    pub charset_name: String,
}

/// CREATE TRANSLATION statement
///
/// SQL:1999 Syntax:
///   CREATE TRANSLATION translation_name
///     [FOR source_charset TO target_charset]
///     [FROM translation_source]
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTranslationStmt {
    pub translation_name: String,
    pub source_charset: Option<String>,     // FOR source_charset
    pub target_charset: Option<String>,     // TO target_charset
    pub translation_source: Option<String>, // FROM translation_source
}

/// DROP TRANSLATION statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropTranslationStmt {
    pub translation_name: String,
}

/// CREATE VIEW statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateViewStmt {
    pub view_name: String,
    pub columns: Option<Vec<String>>,
    pub query: Box<crate::SelectStmt>,
    pub with_check_option: bool,
}

/// DROP VIEW statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropViewStmt {
    pub view_name: String,
    pub if_exists: bool,
    pub cascade: bool,
}

/// CREATE TRIGGER statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTriggerStmt {
    pub trigger_name: String,
    pub timing: TriggerTiming,
    pub event: TriggerEvent,
    pub table_name: String,
    pub granularity: TriggerGranularity,
    pub when_condition: Option<Box<Expression>>,
    pub triggered_action: TriggerAction,
}

/// Trigger timing: BEFORE | AFTER | INSTEAD OF
#[derive(Debug, Clone, PartialEq)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

/// Trigger event: INSERT | UPDATE | DELETE
#[derive(Debug, Clone, PartialEq)]
pub enum TriggerEvent {
    Insert,
    Update(Option<Vec<String>>), // Optional column list for UPDATE OF
    Delete,
}

/// Trigger granularity: FOR EACH ROW | FOR EACH STATEMENT
#[derive(Debug, Clone, PartialEq)]
pub enum TriggerGranularity {
    Row,       // FOR EACH ROW
    Statement, // FOR EACH STATEMENT (default in SQL:1999)
}

/// Triggered action (procedural SQL)
#[derive(Debug, Clone, PartialEq)]
pub enum TriggerAction {
    /// For initial implementation, store raw SQL
    RawSql(String),
    // Future: Add procedural SQL statement support
    // Statements(Vec<Statement>),
}

/// DROP TRIGGER statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropTriggerStmt {
    pub trigger_name: String,
    pub cascade: bool,
}

/// CREATE INDEX statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStmt {
    pub index_name: String,
    pub table_name: String,
    pub unique: bool,
    pub columns: Vec<String>,
}

/// DROP INDEX statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropIndexStmt {
    pub index_name: String,
}

/// DECLARE CURSOR statement (SQL:1999 Feature E121)
#[derive(Debug, Clone, PartialEq)]
pub struct DeclareCursorStmt {
    pub cursor_name: String,
    pub insensitive: bool,
    pub scroll: bool,
    pub hold: Option<bool>, // Some(true) = WITH HOLD, Some(false) = WITHOUT HOLD, None = not specified
    pub query: Box<crate::SelectStmt>,
    pub updatability: CursorUpdatability,
}

/// Cursor updatability specification
#[derive(Debug, Clone, PartialEq)]
pub enum CursorUpdatability {
    ReadOnly,
    Update { columns: Option<Vec<String>> }, // Some(cols) = UPDATE OF cols, None = UPDATE (all columns)
    Unspecified,
}

/// OPEN CURSOR statement (SQL:1999 Feature E121)
#[derive(Debug, Clone, PartialEq)]
pub struct OpenCursorStmt {
    pub cursor_name: String,
}

/// FETCH statement (SQL:1999 Feature E121)
#[derive(Debug, Clone, PartialEq)]
pub struct FetchStmt {
    pub cursor_name: String,
    pub orientation: FetchOrientation,
    pub into_variables: Option<Vec<String>>, // INTO variable_list
}

/// Fetch orientation specification
#[derive(Debug, Clone, PartialEq)]
pub enum FetchOrientation {
    Next,
    Prior,
    First,
    Last,
    Absolute(i64), // ABSOLUTE n
    Relative(i64), // RELATIVE n
}

/// CLOSE CURSOR statement (SQL:1999 Feature E121)
#[derive(Debug, Clone, PartialEq)]
pub struct CloseCursorStmt {
    pub cursor_name: String,
}

/// Transaction isolation level
#[derive(Debug, Clone, PartialEq)]
pub enum IsolationLevel {
    Serializable,
}

/// Transaction access mode
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionAccessMode {
    ReadOnly,
    ReadWrite,
}

/// SET TRANSACTION statement (SQL:1999 Feature E152)
#[derive(Debug, Clone, PartialEq)]
pub struct SetTransactionStmt {
    pub local: bool, // true for SET LOCAL TRANSACTION, false for SET TRANSACTION
    pub isolation_level: Option<IsolationLevel>,
    pub access_mode: Option<TransactionAccessMode>,
}

/// CREATE ASSERTION statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateAssertionStmt {
    pub assertion_name: String,
    pub check_condition: Box<Expression>,
}

/// DROP ASSERTION statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropAssertionStmt {
    pub assertion_name: String,
    pub cascade: bool, // true for CASCADE, false for RESTRICT
}
