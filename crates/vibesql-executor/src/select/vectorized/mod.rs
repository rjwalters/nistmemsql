//! Vectorized execution module
//!
//! This module provides chunk-based predicate evaluation for cache optimization.
//!
//! ## Phase 3 - SIMD Vectorization (In Progress)
//!
//! Apache Arrow integration for true SIMD vectorization is planned in Phase 3.
//! See `PHASE3_VECTORIZATION.md` in the crate root for the design document.
//!
//! Key planned features:
//! - RecordBatch-based columnar processing
//! - SIMD filter and aggregation kernels
//! - 5-10x performance improvement target
//!
//! Current status: Arrow dependencies added, foundation in place.

pub mod bitmap;  // Kept for potential future SIMD use
pub mod predicate;

pub use predicate::apply_where_filter_vectorized;

/// Default chunk size for vectorized operations
/// Tuned for L1 cache utilization (typical 32-64KB)
pub const DEFAULT_CHUNK_SIZE: usize = 256;

/// Threshold for using vectorized evaluation
/// Below this, row-by-row is more efficient due to lower setup overhead
pub const VECTORIZE_THRESHOLD: usize = 100;
