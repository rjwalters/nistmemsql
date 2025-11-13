//! Tree traversal helpers for BTreeIndex
//!
//! This module contains helper functions for navigating the B+ tree structure.

use std::sync::Arc;

use crate::page::{PageId, PageManager};
use crate::StorageError;

use super::structure::{Key, LeafNode};
use super::btree_index::BTreeIndex;

impl BTreeIndex {
    /// Navigate to leaf node that should contain the key, returning the path from root
    ///
    /// Returns (leaf_node, path) where path is Vec<(PageId, child_index)>
    /// The path tracks which child was taken at each internal node level
    pub(crate) fn find_leaf_path(&self, key: &Key) -> Result<(LeafNode, Vec<(PageId, usize)>), StorageError> {
        let mut path = Vec::new();
        let mut current_page_id = self.root_page_id();

        // Navigate down the tree
        for _ in 0..self.height() - 1 {
            // Read internal node
            let internal = self.read_internal_node(current_page_id)?;

            // Find which child to follow
            let child_idx = internal.find_child_index(key);
            path.push((current_page_id, child_idx));

            // Move to child
            current_page_id = internal.children[child_idx];
        }

        // Read leaf node
        let leaf = self.read_leaf_node(current_page_id)?;

        Ok((leaf, path))
    }

    /// Find the leftmost (first) leaf node in the tree
    ///
    /// # Returns
    /// The leftmost leaf node
    pub(crate) fn find_leftmost_leaf(&self) -> Result<LeafNode, StorageError> {
        let mut current_page_id = self.root_page_id();

        // Navigate down the tree always taking the leftmost child
        for _ in 0..self.height() - 1 {
            let internal = self.read_internal_node(current_page_id)?;
            if internal.children.is_empty() {
                return Err(StorageError::IoError(
                    "Internal node has no children".to_string(),
                ));
            }
            // Always take first child (leftmost)
            current_page_id = internal.children[0];
        }

        // Read the leftmost leaf
        self.read_leaf_node(current_page_id)
    }
}

/// Helper function to read the first key from a page (either internal or leaf)
pub(crate) fn read_first_key_from_page(
    page_manager: &Arc<PageManager>,
    page_id: PageId,
) -> Result<Key, StorageError> {
    let page = page_manager.read_page(page_id)?;

    // Read page type (first byte)
    let page_type = page.data[0];

    if page_type == super::super::PAGE_TYPE_LEAF {
        let leaf = super::super::serialize::read_leaf_node(page_manager, page_id)?;
        if leaf.entries.is_empty() {
            return Err(StorageError::IoError("Leaf node has no entries".to_string()));
        }
        Ok(leaf.entries[0].0.clone())
    } else if page_type == super::super::PAGE_TYPE_INTERNAL {
        let internal = super::super::serialize::read_internal_node(page_manager, page_id)?;
        if internal.keys.is_empty() {
            // Internal node with only one child - need to recurse
            if internal.children.is_empty() {
                return Err(StorageError::IoError("Internal node has no keys or children".to_string()));
            }
            return read_first_key_from_page(page_manager, internal.children[0]);
        }
        Ok(internal.keys[0].clone())
    } else {
        Err(StorageError::IoError(format!("Invalid page type: {}", page_type)))
    }
}
