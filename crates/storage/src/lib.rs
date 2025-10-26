//! Storage - In-Memory Data Storage
//!
//! This crate provides in-memory storage for database tables and rows.

pub mod database;
pub mod error;
pub mod row;
pub mod table;

pub use database::Database;
pub use error::StorageError;
pub use row::Row;
pub use table::Table;
