//! Executor - SQL Query Execution Engine
//!
//! This crate provides query execution functionality for SQL statements.

mod create_table;
pub mod errors;
mod evaluator;
mod schema;
mod select;

pub use create_table::CreateTableExecutor;
pub use errors::ExecutorError;
pub use evaluator::ExpressionEvaluator;
pub use select::SelectExecutor;

#[cfg(test)]
mod tests;
