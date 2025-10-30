//! Executor - SQL Query Execution Engine
//!
//! This crate provides query execution functionality for SQL statements.

mod advanced_objects;
mod alter;
mod create_table;
mod delete;
mod domain_ddl;
mod drop_table;
pub mod errors;
pub mod evaluator;
mod grant;
mod insert;
mod privilege_checker;
mod revoke;
mod role_ddl;
mod schema;
mod schema_ddl;
mod select;
mod transaction;
mod update;

pub use alter::AlterTableExecutor;
pub use create_table::CreateTableExecutor;
pub use delete::DeleteExecutor;
pub use domain_ddl::DomainExecutor;
pub use drop_table::DropTableExecutor;
pub use errors::ExecutorError;
pub use evaluator::ExpressionEvaluator;
pub use grant::GrantExecutor;
pub use insert::InsertExecutor;
pub use privilege_checker::PrivilegeChecker;
pub use revoke::RevokeExecutor;
pub use role_ddl::RoleExecutor;
pub use schema_ddl::SchemaExecutor;
pub use select::{SelectExecutor, SelectResult};
pub use transaction::{
    BeginTransactionExecutor, CommitExecutor, ReleaseSavepointExecutor, RollbackExecutor,
    RollbackToSavepointExecutor, SavepointExecutor,
};
pub use update::UpdateExecutor;

#[cfg(test)]
mod tests;
