//! Expression optimization module for query planning
//!
//! This module implements:
//! - Constant folding and dead code elimination for expressions
//! - WHERE clause predicate pushdown for efficient join evaluation

mod expressions;
pub mod where_pushdown;
#[cfg(test)]
mod tests;

pub use expressions::*;
pub use where_pushdown::{
    decompose_where_predicates, get_table_local_predicates,
    get_equijoin_predicates, combine_with_and, get_predicates_for_tables,
};
// Also export branch-specific types for join reordering
pub use where_pushdown::{decompose_where_clause, PredicateDecomposition};
