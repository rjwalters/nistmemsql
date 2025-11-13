//! B+ Tree Node Splitting and Merging
//!
//! This module implements node restructuring operations:
//! - Node splitting (when nodes become too full)
//! - Node merging (for future use in delete operations)
//! - Rebalancing algorithms

use super::structure::{InternalNode, Key, LeafNode};

impl InternalNode {
    /// Split this internal node into two nodes
    ///
    /// Returns (middle_key, new_right_node)
    pub fn split(&mut self, new_page_id: u64) -> (Key, InternalNode) {
        let mid = self.keys.len() / 2;

        // Middle key moves up to parent
        let middle_key = self.keys[mid].clone();

        // Create right node with upper half of keys and children
        let mut right_node = InternalNode::new(new_page_id);
        right_node.keys = self.keys.split_off(mid + 1);
        right_node.children = self.children.split_off(mid + 1);

        // Remove middle key from left node
        self.keys.pop();

        (middle_key, right_node)
    }
}

impl LeafNode {
    /// Split this leaf node into two nodes
    ///
    /// Returns (middle_key, new_right_node)
    pub fn split(&mut self, new_page_id: u64) -> (Key, LeafNode) {
        let mid = self.entries.len() / 2;

        // Create right node with upper half of entries
        let mut right_node = LeafNode::new(new_page_id);
        right_node.entries = self.entries.split_off(mid);

        // Update linked list pointers
        right_node.next_leaf = self.next_leaf;
        self.next_leaf = new_page_id;

        // Middle key is the first key of right node (copy, not move)
        let middle_key = right_node.entries[0].0.clone();

        (middle_key, right_node)
    }
}
