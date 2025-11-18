//! Vectorized/batch execution for improved cache locality and SIMD potential
//!
//! This module provides chunk-based predicate evaluation to improve:
//! - Instruction cache locality (batch operations reduce function call overhead)
//! - Data cache locality (better memory access patterns)
//! - Branch prediction (more predictable patterns in batch operations)
//! - Foundation for future SIMD optimizations
//!
//! The vectorized approach processes rows in chunks (default 256 rows),
//! evaluating predicates on the entire chunk and using bitmaps for filtering.

pub mod bitmap;
pub mod predicate;

pub use predicate::apply_where_filter_vectorized;

/// Default chunk size for vectorized operations
/// Tuned for L1 cache utilization (typical 32-64KB)
pub const DEFAULT_CHUNK_SIZE: usize = 256;

/// Threshold for using vectorized evaluation
/// Below this, row-by-row is more efficient due to lower setup overhead
pub const VECTORIZE_THRESHOLD: usize = 100;
