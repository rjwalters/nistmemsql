//! Expression optimization module for query planning
//!
//! This module implements constant folding and dead code elimination to optimize
//! expression evaluation during query execution.

mod expressions;
#[cfg(test)]
mod tests;

pub use expressions::*;
