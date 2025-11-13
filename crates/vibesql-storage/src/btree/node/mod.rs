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
mod tests {
    use super::*;
    use vibesql_types::{DataType, SqlValue};
    use std::sync::Arc;
    use crate::page::PageManager;

    #[test]
    fn test_leaf_node_insert() {
        let mut leaf = LeafNode::new(1);

        assert!(leaf.insert(vec![SqlValue::Integer(10)], 0));
        assert!(leaf.insert(vec![SqlValue::Integer(5)], 1));
        assert!(leaf.insert(vec![SqlValue::Integer(15)], 2));

        // Check sorted order
        assert_eq!(leaf.entries.len(), 3);
        assert_eq!(leaf.entries[0].0[0], SqlValue::Integer(5));
        assert_eq!(leaf.entries[1].0[0], SqlValue::Integer(10));
        assert_eq!(leaf.entries[2].0[0], SqlValue::Integer(15));
    }

    #[test]
    fn test_leaf_node_duplicate_insert() {
        let mut leaf = LeafNode::new(1);

        assert!(leaf.insert(vec![SqlValue::Integer(10)], 0));
        assert!(!leaf.insert(vec![SqlValue::Integer(10)], 1));  // Duplicate

        assert_eq!(leaf.entries.len(), 1);
    }

    #[test]
    fn test_leaf_node_search() {
        let mut leaf = LeafNode::new(1);

        leaf.insert(vec![SqlValue::Integer(10)], 100);
        leaf.insert(vec![SqlValue::Integer(20)], 200);

        assert_eq!(leaf.search(&vec![SqlValue::Integer(10)]), Some(100));
        assert_eq!(leaf.search(&vec![SqlValue::Integer(20)]), Some(200));
        assert_eq!(leaf.search(&vec![SqlValue::Integer(15)]), None);
    }

    #[test]
    fn test_leaf_node_delete() {
        let mut leaf = LeafNode::new(1);

        leaf.insert(vec![SqlValue::Integer(10)], 0);
        leaf.insert(vec![SqlValue::Integer(20)], 1);

        assert!(leaf.delete(&vec![SqlValue::Integer(10)]));
        assert!(!leaf.delete(&vec![SqlValue::Integer(10)]));  // Already deleted

        assert_eq!(leaf.entries.len(), 1);
        assert_eq!(leaf.entries[0].0[0], SqlValue::Integer(20));
    }

    #[test]
    fn test_leaf_node_split() {
        let mut leaf = LeafNode::new(1);

        // Insert 6 entries
        for i in 0..6 {
            leaf.insert(vec![SqlValue::Integer(i * 10)], i as usize);
        }

        let (middle_key, right_node) = leaf.split(2);

        // Left node should have first 3 entries
        assert_eq!(leaf.entries.len(), 3);
        assert_eq!(leaf.next_leaf, 2);

        // Right node should have last 3 entries
        assert_eq!(right_node.entries.len(), 3);
        assert_eq!(right_node.page_id, 2);

        // Middle key should be first key of right node
        assert_eq!(middle_key, vec![SqlValue::Integer(30)]);
    }

    #[test]
    fn test_internal_node_find_child() {
        let mut node = InternalNode::new(1);

        node.keys = vec![
            vec![SqlValue::Integer(10)],
            vec![SqlValue::Integer(20)],
            vec![SqlValue::Integer(30)],
        ];
        node.children = vec![2, 3, 4, 5];

        assert_eq!(node.find_child_index(&vec![SqlValue::Integer(5)]), 0);
        assert_eq!(node.find_child_index(&vec![SqlValue::Integer(10)]), 1);
        assert_eq!(node.find_child_index(&vec![SqlValue::Integer(15)]), 1);
        assert_eq!(node.find_child_index(&vec![SqlValue::Integer(25)]), 2);
        assert_eq!(node.find_child_index(&vec![SqlValue::Integer(35)]), 3);
    }

    #[test]
    fn test_internal_node_insert_child() {
        let mut node = InternalNode::new(1);

        node.children.push(2);  // Initial child

        node.insert_child(vec![SqlValue::Integer(10)], 3);
        node.insert_child(vec![SqlValue::Integer(5)], 4);
        node.insert_child(vec![SqlValue::Integer(15)], 5);

        // Check sorted order
        assert_eq!(node.keys.len(), 3);
        assert_eq!(node.children.len(), 4);
        assert_eq!(node.keys[0][0], SqlValue::Integer(5));
        assert_eq!(node.keys[1][0], SqlValue::Integer(10));
        assert_eq!(node.keys[2][0], SqlValue::Integer(15));
    }

    #[test]
    fn test_internal_node_split() {
        let mut node = InternalNode::new(1);

        // Setup node with 5 keys and 6 children
        node.keys = vec![
            vec![SqlValue::Integer(10)],
            vec![SqlValue::Integer(20)],
            vec![SqlValue::Integer(30)],
            vec![SqlValue::Integer(40)],
            vec![SqlValue::Integer(50)],
        ];
        node.children = vec![2, 3, 4, 5, 6, 7];

        let (middle_key, right_node) = node.split(8);

        // Middle key should be 30
        assert_eq!(middle_key, vec![SqlValue::Integer(30)]);

        // Left node should have 2 keys and 3 children
        assert_eq!(node.keys.len(), 2);
        assert_eq!(node.children.len(), 3);
        assert_eq!(node.keys[0][0], SqlValue::Integer(10));
        assert_eq!(node.keys[1][0], SqlValue::Integer(20));

        // Right node should have 2 keys and 3 children
        assert_eq!(right_node.keys.len(), 2);
        assert_eq!(right_node.children.len(), 3);
        assert_eq!(right_node.keys[0][0], SqlValue::Integer(40));
        assert_eq!(right_node.keys[1][0], SqlValue::Integer(50));
    }

    #[test]
    fn test_bulk_load_empty() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.db");
        let page_manager = Arc::new(PageManager::new(&path).unwrap());

        let key_schema = vec![DataType::Integer];
        let sorted_entries = vec![];

        let index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();

        // Empty index should have height 1 (just root leaf)
        assert_eq!(index.height(), 1);
    }

    #[test]
    fn test_bulk_load_small() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.db");
        let page_manager = Arc::new(PageManager::new(&path).unwrap());

        let key_schema = vec![DataType::Integer];
        let sorted_entries = vec![
            (vec![SqlValue::Integer(10)], 0),
            (vec![SqlValue::Integer(20)], 1),
            (vec![SqlValue::Integer(30)], 2),
            (vec![SqlValue::Integer(40)], 3),
            (vec![SqlValue::Integer(50)], 4),
        ];

        let index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();

        // Small index should have height 1 (just root leaf)
        assert_eq!(index.height(), 1);

        // Verify we can read the root leaf
        let root_leaf = index.read_leaf_node(index.root_page_id()).unwrap();
        assert_eq!(root_leaf.entries.len(), 5);
        assert_eq!(root_leaf.entries[0].0, vec![SqlValue::Integer(10)]);
        assert_eq!(root_leaf.entries[4].0, vec![SqlValue::Integer(50)]);
    }

    #[test]
    fn test_bulk_load_multi_column() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.db");
        let page_manager = Arc::new(PageManager::new(&path).unwrap());

        let key_schema = vec![DataType::Integer, DataType::Varchar { max_length: Some(50) }];
        let sorted_entries = vec![
            (vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())], 0),
            (vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())], 1),
            (vec![SqlValue::Integer(3), SqlValue::Varchar("Charlie".to_string())], 2),
        ];

        let index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();

        // Verify we can read the root leaf
        let root_leaf = index.read_leaf_node(index.root_page_id()).unwrap();
        assert_eq!(root_leaf.entries.len(), 3);
        assert_eq!(
            root_leaf.entries[0].0,
            vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())]
        );
    }

    #[test]
    fn test_bulk_load_large() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.db");
        let page_manager = Arc::new(PageManager::new(&path).unwrap());

        let key_schema = vec![DataType::Integer];

        // Create 1000 sorted entries
        let sorted_entries: Vec<(Key, RowId)> = (0..1000)
            .map(|i| (vec![SqlValue::Integer(i * 10)], i as usize))
            .collect();

        let index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager.clone()).unwrap();

        // Large index should have height > 1
        assert!(index.height() > 1, "Index with 1000 entries should have height > 1");

        // Verify we can load the index back from disk
        let loaded_index = BTreeIndex::load(page_manager).unwrap();
        assert_eq!(loaded_index.height(), index.height());
        assert_eq!(loaded_index.degree(), index.degree());
    }
}
