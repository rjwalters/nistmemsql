//! Modular organization of WASM bindings
//!
//! This module contains the WASM bindings organized into:
//! - `types` - Result types and schemas
//! - `database` - Database struct and constructor
//! - `execute` - DDL/DML execution
//! - `query` - SELECT query execution
//! - `schema` - Schema introspection (list_tables, describe_table)
//! - `examples` - Example database loaders (load_employees, load_northwind)

pub mod database;
pub mod examples;
pub mod execute;
pub mod query;
pub mod schema;
pub mod types;

pub use database::Database;
pub use vibesql_types::{ColumnInfo, ExecuteResult, QueryResult, TableSchema};

#[cfg(test)]
mod tests;
