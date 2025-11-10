// Module declarations
pub(crate) mod casting;
mod combined;
mod core;
pub mod date_format;
mod expressions;
mod functions;
pub(crate) mod operators;
mod pattern;
pub mod window;

#[cfg(test)]
mod tests;

// Re-export public API
pub use core::{CombinedExpressionEvaluator, ExpressionEvaluator};
