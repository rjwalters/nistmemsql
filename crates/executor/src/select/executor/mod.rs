//! SelectExecutor implementation split across modules
//!
//! The SelectExecutor is organized into separate implementation files:
//! - `builder` - Struct definition and constructor methods
//! - `execute` - Main execution entry points
//! - `columns` - Column name derivation
//! - `aggregation` - Aggregation and GROUP BY execution
//! - `nonagg` - Non-aggregation execution path
//! - `utils` - Utility methods for expression analysis

mod aggregation;
mod builder;
mod columns;
mod execute;
mod nonagg;
mod utils;

#[cfg(test)]
mod memory_tests;

pub use builder::SelectExecutor;
