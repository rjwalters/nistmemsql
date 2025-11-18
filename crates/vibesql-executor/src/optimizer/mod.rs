//! Expression optimization module for query planning
//!
//! This module implements:
//! - Constant folding and dead code elimination for expressions
//! - WHERE clause predicate pushdown for efficient join evaluation
//! - Centralized index planning and strategy selection
//! - Unified predicate classification and index strategy selection

mod expressions;
pub mod index_planner;
pub mod index_strategy;
pub mod predicate;
pub mod predicate_plan;
#[cfg(test)]
mod tests;
pub mod where_pushdown;

pub use expressions::*;
pub use index_planner::{IndexPlan, IndexPlanner};
pub use index_strategy::{IndexMetadata, IndexStrategy};
pub use predicate::{ColumnRef, Predicate, TableId};
pub use predicate_plan::PredicatePlan;
pub use where_pushdown::{combine_with_and, decompose_where_clause, PredicateDecomposition};
