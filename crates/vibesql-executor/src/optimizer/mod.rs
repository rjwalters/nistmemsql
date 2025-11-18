//! Expression optimization module for query planning
//!
//! This module implements:
//! - Constant folding and dead code elimination for expressions
//! - WHERE clause predicate pushdown for efficient join evaluation
//! - Cost-based predicate ordering for optimal filter performance

mod expressions;
mod predicate_plan;
pub mod selectivity;
#[cfg(test)]
mod tests;
pub mod where_pushdown;

pub use expressions::*;
pub use predicate_plan::PredicatePlan;
pub use selectivity::{estimate_selectivity, order_predicates_by_selectivity};
pub use where_pushdown::{combine_with_and, decompose_where_clause, PredicateDecomposition};
