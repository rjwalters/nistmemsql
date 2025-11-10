//! SelectExecutor implementation split across modules
//!
//! The SelectExecutor is organized into separate implementation files:
//! - `builder` - Struct definition and constructor methods
//! - `execute` - Main execution entry points
//! - `columns` - Column name derivation
//! - `aggregation` - Aggregation and GROUP BY execution
//! - `nonagg` - Non-aggregation execution path
//! - `utils` - Utility methods for expression analysis
//! - `index_optimization` - Index-based optimizations for WHERE and ORDER BY

mod aggregation;
mod builder;
mod columns;
mod execute;
mod index_optimization;
mod nonagg;
mod utils;

#[cfg(test)]
mod memory_tests;

pub use builder::SelectExecutor;
