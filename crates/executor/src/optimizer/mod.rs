//! Expression optimization module for query planning
//!
//! This module implements constant folding, dead code elimination, and predicate pushdown
//! to optimize expression evaluation during query execution.

mod expressions;
mod where_pushdown;
#[cfg(test)]
mod tests;

pub use expressions::*;
pub use where_pushdown::{
    decompose_where_predicates, get_table_local_predicates,
    get_equijoin_predicates, combine_with_and, get_predicates_for_tables,
};
