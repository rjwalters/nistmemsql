// Module declarations
pub(crate) mod casting;
mod combined;
mod core;
pub mod date_format;
mod expression_hash;
mod expressions;
mod functions;
pub(crate) mod operators;
pub(crate) mod pattern;
pub mod window;

#[cfg(test)]
mod tests;

// Re-export public API
pub use core::{CombinedExpressionEvaluator, ExpressionEvaluator};
// Re-export eval_unary_op for use in other modules
pub(crate) use expressions::operators::eval_unary_op;
