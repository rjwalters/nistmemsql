//! Expression optimization module for query planning
//!
//! This module implements:
//! - Constant folding and dead code elimination for expressions
//! - WHERE clause predicate pushdown for efficient join evaluation
//! - Centralized index planning and strategy selection

mod expressions;
pub mod index_planner;
#[cfg(test)]
mod tests;
pub mod where_pushdown;

pub use expressions::*;
pub use index_planner::{IndexPlan, IndexPlanner};
pub use where_pushdown::{combine_with_and, decompose_where_clause, PredicateDecomposition};
