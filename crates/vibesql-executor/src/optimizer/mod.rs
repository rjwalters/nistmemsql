//! Expression optimization module for query planning
//!
//! This module implements:
//! - Constant folding and dead code elimination for expressions
//! - WHERE clause predicate pushdown for efficient join evaluation
//! - Unified predicate classification and index strategy selection

mod expressions;
pub mod index_strategy;
pub mod predicate;
#[cfg(test)]
mod tests;
pub mod where_pushdown;

pub use expressions::*;
pub use index_strategy::{IndexMetadata, IndexStrategy};
pub use predicate::{ColumnRef, Predicate, TableId};
pub use where_pushdown::{combine_with_and, decompose_where_clause, PredicateDecomposition};
