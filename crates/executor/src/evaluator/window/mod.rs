//! Window Function Evaluator
//!
//! This module implements the core window function evaluation engine that:
//! - Partitions rows by PARTITION BY expressions
//! - Sorts partitions by ORDER BY clauses
//! - Calculates frame boundaries (ROWS mode)
//! - Evaluates window functions over frames
//!
//! # Module Organization
//!
//! The window function evaluator is split into logical modules:
//! - `partitioning` - Partition management and row grouping
//! - `sorting` - Partition sorting and value comparison
//! - `frames` - Frame boundary calculation (ROWS mode)
//! - `ranking` - Ranking functions (ROW_NUMBER, RANK, DENSE_RANK, NTILE)
//! - `aggregates` - Aggregate window functions (COUNT, SUM, AVG, MIN, MAX)
//! - `value` - Value access functions (LAG, LEAD)
//! - `utils` - Shared utility functions

mod aggregates;
mod frames;
mod partitioning;
mod ranking;
mod sorting;
mod utils;
mod value;

// Re-export public API
pub use partitioning::{Partition, partition_rows};
pub use sorting::{sort_partition, compare_values};
pub use frames::calculate_frame;
pub use ranking::{evaluate_row_number, evaluate_rank, evaluate_dense_rank, evaluate_ntile};
pub use aggregates::{
    evaluate_count_window, evaluate_sum_window, evaluate_avg_window,
    evaluate_min_window, evaluate_max_window,
};
pub use value::{evaluate_lag, evaluate_lead};

#[cfg(test)]
mod tests;
