//! Executor - SQL Query Execution Engine
//!
//! This crate provides query execution functionality for SQL statements.

mod alter;
mod create_table;
mod delete;
mod drop_table;
pub mod errors;
pub mod evaluator;
mod insert;
mod schema;
mod schema_ddl;
mod select;
mod transaction;
mod update;

pub use alter::AlterTableExecutor;
pub use create_table::CreateTableExecutor;
pub use delete::DeleteExecutor;
pub use drop_table::DropTableExecutor;
pub use schema_ddl::SchemaExecutor;
pub use errors::ExecutorError;
pub use evaluator::ExpressionEvaluator;
pub use insert::InsertExecutor;
pub use select::{SelectExecutor, SelectResult};
pub use transaction::{BeginTransactionExecutor, CommitExecutor, ReleaseSavepointExecutor, RollbackExecutor, RollbackToSavepointExecutor, SavepointExecutor};
pub use update::UpdateExecutor;

#[cfg(test)]
mod tests;
