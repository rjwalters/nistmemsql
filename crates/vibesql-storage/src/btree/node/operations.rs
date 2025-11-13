//! B+ Tree Node Operations
//!
//! This module implements basic node operations:
//! - Key search and traversal
//! - Insertion and deletion
//! - Key/value manipulation

use crate::page::PageId;

use super::structure::{InternalNode, Key, LeafNode, RowId};

impl InternalNode {
    /// Find the child index for a given key
    ///
    /// Returns the index of the child that should contain the key
    pub fn find_child_index(&self, key: &Key) -> usize {
        // Binary search to find the appropriate child
        match self.keys.binary_search(key) {
            Ok(idx) => idx + 1,  // Key found, go to right child
            Err(idx) => idx,     // Key not found, idx is the insertion point
        }
    }

    /// Insert a key and child into this internal node
    ///
    /// Assumes the node is not full (caller should check)
    pub fn insert_child(&mut self, key: Key, child_page_id: PageId) {
        // Find insertion point
        let idx = match self.keys.binary_search(&key) {
            Ok(idx) | Err(idx) => idx,
        };

        // Insert key and child
        self.keys.insert(idx, key);
        self.children.insert(idx + 1, child_page_id);
    }
}

impl LeafNode {
    /// Insert a key-value pair into this leaf node
    ///
    /// Returns true if inserted, false if key already exists
    pub fn insert(&mut self, key: Key, row_id: RowId) -> bool {
        match self.entries.binary_search_by_key(&&key, |(k, _)| k) {
            Ok(_) => false,  // Key already exists
            Err(idx) => {
                self.entries.insert(idx, (key, row_id));
                true
            }
        }
    }

    /// Search for a key in this leaf node
    ///
    /// Returns the row_id if found
    #[allow(dead_code)]
    pub fn search(&self, key: &Key) -> Option<RowId> {
        self.entries
            .binary_search_by_key(&key, |(k, _)| k)
            .ok()
            .map(|idx| self.entries[idx].1)
    }

    /// Delete a key from this leaf node
    ///
    /// Returns true if deleted, false if key not found
    pub fn delete(&mut self, key: &Key) -> bool {
        match self.entries.binary_search_by_key(&key, |(k, _)| k) {
            Ok(idx) => {
                self.entries.remove(idx);
                true
            }
            Err(_) => false,
        }
    }
}
