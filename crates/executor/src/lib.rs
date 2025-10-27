//! Executor - SQL Query Execution Engine
//!
//! This crate provides query execution functionality for SQL statements.

mod create_table;
mod delete;
pub mod errors;
mod evaluator;
mod insert;
mod schema;
mod select;
mod update;

pub use create_table::CreateTableExecutor;
pub use delete::DeleteExecutor;
pub use errors::ExecutorError;
pub use evaluator::ExpressionEvaluator;
pub use insert::InsertExecutor;
pub use select::{SelectExecutor, SelectResult};
pub use update::UpdateExecutor;

#[cfg(test)]
mod tests;
