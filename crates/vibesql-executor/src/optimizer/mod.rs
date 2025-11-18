//! Expression optimization module for query planning
//!
//! This module implements:
//! - Constant folding and dead code elimination for expressions
//! - WHERE clause predicate pushdown for efficient join evaluation
//! - Cost-based predicate ordering for optimal filter performance
//! - Centralized index planning and strategy selection
//! - Unified predicate classification and index strategy selection

mod expressions;
pub mod index_planner;
pub mod index_strategy;
pub mod predicate;
mod predicate_plan;
pub mod selectivity;
#[cfg(test)]
mod tests;
pub mod where_pushdown;

pub use expressions::*;
pub use predicate_plan::PredicatePlan;
pub use where_pushdown::{combine_with_and, PredicateDecomposition};
