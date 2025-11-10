//! SQLLogicTest suite infrastructure modules.
//!
//! This module contains the refactored components of the SQLLogicTest suite:
//! - `db_adapter`: Database adapter implementing AsyncDB trait
//! - `execution`: Test file execution with timeout handling
//! - `formatting`: SQL value formatting utilities
//! - `preprocessing`: MySQL-specific directive handling
//! - `scheduler`: Test prioritization and worker partitioning
//! - `stats`: Test statistics and failure tracking
//! - `work_queue`: Work queue for dynamic file allocation across workers
//! - `metrics`: Benchmark metrics collection and aggregation
//! - `report`: Comparison report generator with multiple output formats

pub mod db_adapter;
pub mod execution;
pub mod formatting;
pub mod metrics;
pub mod preprocessing;
pub mod report;
pub mod scheduler;
pub mod stats;
pub mod work_queue;
