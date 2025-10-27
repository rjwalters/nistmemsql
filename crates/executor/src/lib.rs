//! Executor - SQL Query Execution Engine
//!
//! This crate provides query execution functionality for SQL statements.

pub mod errors;
mod evaluator;
mod schema;
mod select;
mod update;

pub use errors::ExecutorError;
pub use evaluator::ExpressionEvaluator;
pub use select::SelectExecutor;
pub use update::UpdateExecutor;

#[cfg(test)]
mod tests;
