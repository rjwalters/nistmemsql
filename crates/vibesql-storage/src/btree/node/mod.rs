//! B+ Tree Node Structures and Operations
//!
//! This module is organized into focused submodules:
//! - `structure`: Core node data structures (InternalNode, LeafNode, Key, RowId)
//! - `operations`: Basic node operations (insert, delete, search, traversal)
//! - `split_merge`: Node restructuring (splitting and merging)
//! - `datatype_serialization`: DataType persistence utilities
//! - `btree_index`: Main B+ tree index implementation
//!
//! ## Public API
//!
//! This module re-exports the main types and structures needed by the rest of the codebase:
//! - `BTreeIndex`: Main B+ tree index structure
//! - `InternalNode`, `LeafNode`: Node structures
//! - `Key`, `RowId`: Type aliases for keys and row identifiers

// Submodules
mod structure;
mod operations;
mod split_merge;
mod datatype_serialization;
mod btree_index;

// Re-export public types and structures
pub use structure::{InternalNode, Key, LeafNode, RowId};
pub use btree_index::BTreeIndex;

// Tests module
#[cfg(test)]
mod tests;
