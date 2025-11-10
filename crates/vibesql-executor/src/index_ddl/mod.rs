//! CREATE INDEX and DROP INDEX statement execution
//!
//! This module provides executors for index DDL operations.
//!
//! # Structure
//!
//! - `create_index.rs` - CREATE INDEX executor
//! - `drop_index.rs` - DROP INDEX executor

pub mod create_index;
pub mod drop_index;

use vibesql_ast::{CreateIndexStmt, DropIndexStmt};
pub use create_index::CreateIndexExecutor;
pub use drop_index::DropIndexExecutor;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Unified executor for index operations (CREATE and DROP INDEX)
///
/// This struct provides backward compatibility with the original API,
/// delegating to specialized executors for each operation.
pub struct IndexExecutor;

impl IndexExecutor {
    /// Execute a CREATE INDEX statement (delegates to CreateIndexExecutor)
    pub fn execute(
        stmt: &CreateIndexStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        CreateIndexExecutor::execute(stmt, database)
    }

    /// Execute a DROP INDEX statement (delegates to DropIndexExecutor)
    pub fn execute_drop(
        stmt: &DropIndexStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        DropIndexExecutor::execute(stmt, database)
    }
}
