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
pub use where_pushdown::{
    combine_with_and, decompose_where_predicates, get_equijoin_predicates,
    get_predicates_for_tables, get_table_local_predicates,
};
// Also export branch-specific types for join reordering
pub use where_pushdown::{decompose_where_clause, PredicateDecomposition};
