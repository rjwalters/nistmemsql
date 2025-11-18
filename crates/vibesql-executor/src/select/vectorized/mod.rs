//! Chunk-based execution for improved instruction cache locality
//!
//! This module provides chunk-based predicate evaluation to improve:
//! - Instruction cache locality: Keeps predicate evaluation function hot in cache
//! - Code locality: Tight inner loops enable better CPU pipeline efficiency
//! - Single-pass processing: Avoids allocation overhead of intermediate data structures
//!
//! The chunk-based approach processes rows in chunks (default 256 rows) with
//! single-pass evaluation and filtering. This is NOT true SIMD/vectorization -
//! it's a cache optimization technique.

pub mod bitmap;  // Kept for potential future SIMD use
pub mod predicate;

pub use predicate::apply_where_filter_vectorized;

/// Default chunk size for vectorized operations
/// Tuned for L1 cache utilization (typical 32-64KB)
pub const DEFAULT_CHUNK_SIZE: usize = 256;

/// Threshold for using vectorized evaluation
/// Below this, row-by-row is more efficient due to lower setup overhead
pub const VECTORIZE_THRESHOLD: usize = 100;
