//! Expression optimization module for query planning
//!
//! This module implements:
//! - Constant folding and dead code elimination for expressions
//! - WHERE clause predicate pushdown for efficient join evaluation

mod expressions;
#[cfg(test)]
mod tests;
pub mod where_pushdown;

pub use expressions::*;
pub use where_pushdown::{combine_with_and, PredicateDecomposition};
