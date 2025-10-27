// Module declarations
mod casting;
mod combined;
mod core;
mod expressions;
mod pattern;

#[cfg(test)]
mod tests;

// Re-export public API
pub use core::{CombinedExpressionEvaluator, ExpressionEvaluator};
