//! Expression evaluation
//!
//! This module implements expression evaluation organized into:
//! - `eval` - Main evaluation entry point and column references
//! - `predicates` - BETWEEN, LIKE, IN list, POSITION, CAST
//! - `subqueries` - Subquery evaluation (scalar, IN, EXISTS, quantified)
//! - `special` - CASE expressions and function calls
//! - `operators` - Operator evaluation (unary +/-)

mod eval;
mod predicates;
mod special;
mod subqueries;
pub(crate) mod operators;
