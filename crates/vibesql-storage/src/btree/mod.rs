//! B+ Tree Index Implementation
//!
//! This module provides a disk-backed B+ tree index implementation using the page
//! management infrastructure. B+ trees are optimized for range queries and sequential
//! access patterns common in database workloads.
//!
//! # Architecture
//!
//! - **Internal Nodes**: Store keys and page pointers to child nodes
//! - **Leaf Nodes**: Store key-value pairs and pointers to next leaf (for range scans)
//! - **Page-based Storage**: All nodes are serialized to fixed-size pages (4KB)
//! - **Multi-column Keys**: Support composite keys via Vec<SqlValue>
//!
//! # Design Decisions
//!
//! - ADR-001: Reuse SqlValue serialization from persistence/binary/value.rs
//! - ADR-002: Page 0 reserved for B+ tree metadata (root_page_id, degree, height)
//! - ADR-003: Multi-column keys as Vec<SqlValue> for API compatibility
//! - ADR-004: Pure disk-backed implementation (buffer pool deferred to Part 3)

mod node;
mod serialize;

pub use node::{BTreeIndex, Key, RowId};

use crate::page::{PageId, PAGE_SIZE};
use vibesql_types::DataType;

// Page type identifiers
const PAGE_TYPE_METADATA: u8 = 0;
const PAGE_TYPE_INTERNAL: u8 = 1;
const PAGE_TYPE_LEAF: u8 = 2;

// Null page reference
const NULL_PAGE_ID: PageId = 0;

/// Calculate the maximum degree for a B+ tree based on key schema
///
/// The degree determines how many children an internal node can have
/// and how many entries a leaf node can store.
///
/// # Arguments
/// * `key_schema` - The data types of the key columns
///
/// # Returns
/// The maximum degree (minimum is enforced as 5)
fn calculate_degree(key_schema: &[DataType]) -> usize {
    let key_size = estimate_max_key_size(key_schema);

    // Internal node format: [header: 3 bytes][keys: N * key_size][children: (N+1) * 8 bytes]
    // Leaf node format: [header: 3 bytes][entries: N * (key_size + 8)][next_leaf: 8 bytes]

    // Use leaf node calculation as it's more restrictive
    let header_size = 3; // page_type(1) + num_entries(2)
    let next_leaf_size = 8;
    let entry_size = key_size + 8; // key + row_id

    let available = PAGE_SIZE - header_size - next_leaf_size;
    let degree = available / entry_size;

    // Enforce minimum degree of 5 to maintain tree properties
    degree.max(5)
}

/// Estimate the maximum size of a serialized key
///
/// # Arguments
/// * `key_schema` - The data types of the key columns
///
/// # Returns
/// Estimated maximum size in bytes
fn estimate_max_key_size(key_schema: &[DataType]) -> usize {
    let mut total_size = 0;

    for data_type in key_schema {
        let size = match data_type {
            DataType::Smallint => 1 + 2,  // tag + i16
            DataType::Integer | DataType::Bigint => 1 + 8,  // tag + i64
            DataType::Unsigned => 1 + 8,  // tag + u64
            DataType::Float { .. } | DataType::Real => 1 + 4,  // tag + f32
            DataType::Numeric { .. } | DataType::Decimal { .. } | DataType::DoublePrecision => 1 + 8,  // tag + f64
            DataType::Boolean => 1 + 1,  // tag + bool
            DataType::Character { length } => {
                // tag + length(8) + max_chars
                // Conservative estimate: assume 4 bytes per UTF-8 char
                1 + 8 + (length * 4)
            }
            DataType::Varchar { max_length } => {
                // tag + length(8) + max_chars
                // Conservative estimate: assume 4 bytes per UTF-8 char
                let max_len = max_length.unwrap_or(255);
                1 + 8 + (max_len * 4)
            }
            DataType::CharacterLargeObject | DataType::Name => {
                // tag + length(8) + max_chars for NAME/CLOB
                // Conservative estimate: assume 4 bytes per UTF-8 char
                1 + 8 + (255 * 4)
            }
            DataType::Date => {
                // Serialized as strings, conservative estimate
                1 + 8 + 32
            }
            DataType::Time { .. } | DataType::Timestamp { .. } => {
                // Serialized as strings, conservative estimate
                1 + 8 + 32
            }
            DataType::Interval { .. } => {
                // Serialized as string, conservative estimate
                1 + 8 + 64
            }
            DataType::BinaryLargeObject => {
                // Conservative estimate for BLOB
                1 + 8 + 1024
            }
            DataType::UserDefined { .. } => {
                // Unknown type, use conservative estimate
                1 + 8 + 256
            }
            DataType::Null => {
                // Just a tag
                1
            }
        };
        total_size += size;
    }

    // Add overhead for Vec serialization (count field)
    total_size + 8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_degree_integer_key() {
        let key_schema = vec![DataType::Integer];
        let degree = calculate_degree(&key_schema);

        // For integer keys: 1 (tag) + 8 (i64) = 9 bytes per key
        // Entry size: 9 + 8 (row_id) = 17 bytes
        // Available: 4096 - 3 - 8 = 4085 bytes
        // Degree: 4085 / 17 = 240
        assert!(degree >= 200, "Expected degree >= 200, got {}", degree);
    }

    #[test]
    fn test_calculate_degree_varchar_key() {
        let key_schema = vec![DataType::Varchar { max_length: Some(50) }];
        let degree = calculate_degree(&key_schema);

        // Should be significantly less than integer keys due to varchar size
        assert!(degree >= 5, "Expected degree >= 5, got {}", degree);
        assert!(degree < 200, "Expected degree < 200 for varchar(50), got {}", degree);
    }

    #[test]
    fn test_calculate_degree_multi_column() {
        let key_schema = vec![
            DataType::Integer,
            DataType::Varchar { max_length: Some(20) },
        ];
        let degree = calculate_degree(&key_schema);

        // Multi-column keys should have lower degree
        assert!(degree >= 5, "Expected degree >= 5, got {}", degree);
    }

    #[test]
    fn test_calculate_degree_minimum_enforced() {
        // Even with very large keys, minimum degree should be 5
        let key_schema = vec![DataType::Varchar { max_length: Some(10000) }];
        let degree = calculate_degree(&key_schema);

        assert_eq!(degree, 5, "Minimum degree of 5 should be enforced");
    }
}
