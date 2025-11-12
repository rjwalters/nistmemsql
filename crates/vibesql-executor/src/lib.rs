//! Executor - SQL Query Execution Engine
//!
//! This crate provides query execution functionality for SQL statements.

pub mod advanced_objects;
mod alter;
pub mod cache;
mod constraint_validator;
mod create_table;
mod delete;
mod domain_ddl;
mod drop_table;
mod truncate_table;
pub mod errors;
pub mod evaluator;
mod grant;
pub mod index_ddl;
mod insert;
pub mod limits;
mod optimizer;
pub mod persistence;
mod privilege_checker;
pub mod procedural;
mod revoke;
mod role_ddl;
mod schema;
mod schema_ddl;
pub mod select;
mod select_into;
mod transaction;
mod trigger_ddl;
mod trigger_execution;
mod type_ddl;
mod update;

pub use alter::AlterTableExecutor;
pub use cache::{CacheManager, CacheStats, CachedQueryContext, QueryPlanCache, QuerySignature};
pub use constraint_validator::ConstraintValidator;
pub use create_table::CreateTableExecutor;
pub use delete::DeleteExecutor;
pub use domain_ddl::DomainExecutor;
pub use drop_table::DropTableExecutor;
pub use truncate_table::TruncateTableExecutor;
pub use errors::ExecutorError;
pub use evaluator::ExpressionEvaluator;
pub use grant::GrantExecutor;
pub use index_ddl::{CreateIndexExecutor, DropIndexExecutor, IndexExecutor, ReindexExecutor};
pub use insert::InsertExecutor;
pub use persistence::load_sql_dump;
pub use privilege_checker::PrivilegeChecker;
pub use revoke::RevokeExecutor;
pub use trigger_execution::TriggerFirer;
pub use role_ddl::RoleExecutor;
pub use schema_ddl::SchemaExecutor;
pub use select::{SelectExecutor, SelectResult};
pub use select_into::SelectIntoExecutor;
pub use transaction::{
    BeginTransactionExecutor, CommitExecutor, ReleaseSavepointExecutor, RollbackExecutor,
    RollbackToSavepointExecutor, SavepointExecutor,
};
pub use trigger_ddl::TriggerExecutor;
pub use type_ddl::TypeExecutor;
pub use update::UpdateExecutor;

#[cfg(test)]
mod tests;
