//! Schema, catalog, and database object DDL operations
//!
//! This module contains AST nodes for:
//! - CREATE/DROP SCHEMA
//! - CREATE/DROP VIEW
//! - CREATE/DROP INDEX
//! - CREATE/DROP ROLE
//! - SET SCHEMA/CATALOG/NAMES/TIME ZONE
//! - CREATE/DROP TRIGGER

use crate::Expression;

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
    CreateTable(super::table::CreateTableStmt),
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
    pub if_not_exists: bool,
    pub index_name: String,
    pub table_name: String,
    pub unique: bool,
    pub columns: Vec<IndexColumn>,
}

/// Index column specification
#[derive(Debug, Clone, PartialEq)]
pub struct IndexColumn {
    pub column_name: String,
    pub direction: crate::select::OrderDirection,
}

/// DROP INDEX statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropIndexStmt {
    pub if_exists: bool,
    pub index_name: String,
}
