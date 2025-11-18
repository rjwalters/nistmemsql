//! Expression optimization module for query planning
//!
//! This module implements:
//! - Constant folding and dead code elimination for expressions
//! - WHERE clause predicate pushdown for efficient join evaluation

mod expressions;
pub mod predicate_plan;
#[cfg(test)]
mod tests;
pub mod where_pushdown;

pub use expressions::*;
pub use predicate_plan::PredicatePlan;
pub use where_pushdown::{combine_with_and, decompose_where_clause, PredicateDecomposition};
