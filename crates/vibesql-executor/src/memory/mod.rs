//! Memory management utilities for efficient query execution
//!
//! This module provides custom memory allocators designed to minimize
//! heap allocation overhead during query execution.

mod arena;

pub use arena::QueryArena;
