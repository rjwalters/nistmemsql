//! Serialization and Deserialization for B+ Tree Nodes
//!
//! This module handles converting B+ tree nodes to/from disk pages.
//! It reuses the existing SqlValue serialization from persistence/binary/value.rs

use std::io::{Cursor, Write};
use std::sync::Arc;

use crate::page::{Page, PageId, PageManager, PAGE_SIZE};
use crate::persistence::binary::value::{read_sql_value, write_sql_value};
use crate::StorageError;

use super::node::{InternalNode, LeafNode};
use super::{PAGE_TYPE_INTERNAL, PAGE_TYPE_LEAF};

/// Write an internal node to disk
pub fn write_internal_node(
    page_manager: &Arc<PageManager>,
    node: &InternalNode,
    _degree: usize,
) -> Result<(), StorageError> {
    let mut page = Page::new(node.page_id);
    let mut cursor = Cursor::new(&mut page.data[..]);

    // Write page type
    cursor
        .write_all(&[PAGE_TYPE_INTERNAL])
        .map_err(|e| StorageError::IoError(format!("Failed to write page type: {}", e)))?;

    // Write number of keys
    let num_keys = node.keys.len() as u16;
    cursor
        .write_all(&num_keys.to_le_bytes())
        .map_err(|e| StorageError::IoError(format!("Failed to write num_keys: {}", e)))?;

    // Write keys
    for key in &node.keys {
        // Write key length (number of SqlValues in the key)
        let key_len = key.len() as u16;
        cursor
            .write_all(&key_len.to_le_bytes())
            .map_err(|e| StorageError::IoError(format!("Failed to write key_len: {}", e)))?;

        // Write each SqlValue in the key
        for value in key {
            write_sql_value(&mut cursor, value)?;
        }
    }

    // Write children page IDs
    for &child_page_id in &node.children {
        cursor
            .write_all(&child_page_id.to_le_bytes())
            .map_err(|e| StorageError::IoError(format!("Failed to write child_page_id: {}", e)))?;
    }

    // Check if we exceeded page size
    let bytes_written = cursor.position() as usize;
    if bytes_written > PAGE_SIZE {
        return Err(StorageError::IoError(format!(
            "Internal node exceeds page size: {} > {}",
            bytes_written, PAGE_SIZE
        )));
    }

    page.mark_dirty();
    page_manager.write_page(&mut page)?;

    Ok(())
}

/// Read an internal node from disk
pub fn read_internal_node(
    page_manager: &Arc<PageManager>,
    page_id: PageId,
) -> Result<InternalNode, StorageError> {
    let page = page_manager.read_page(page_id)?;
    let mut cursor = Cursor::new(&page.data[..]);

    // Read and verify page type
    let mut page_type = [0u8; 1];
    std::io::Read::read_exact(&mut cursor, &mut page_type)
        .map_err(|e| StorageError::IoError(format!("Failed to read page type: {}", e)))?;

    if page_type[0] != PAGE_TYPE_INTERNAL {
        return Err(StorageError::IoError(format!(
            "Expected internal node, got page type {}",
            page_type[0]
        )));
    }

    // Read number of keys
    let mut num_keys_bytes = [0u8; 2];
    std::io::Read::read_exact(&mut cursor, &mut num_keys_bytes)
        .map_err(|e| StorageError::IoError(format!("Failed to read num_keys: {}", e)))?;
    let num_keys = u16::from_le_bytes(num_keys_bytes) as usize;

    let mut node = InternalNode::new(page_id);

    // Read keys
    for _ in 0..num_keys {
        // Read key length
        let mut key_len_bytes = [0u8; 2];
        std::io::Read::read_exact(&mut cursor, &mut key_len_bytes)
            .map_err(|e| StorageError::IoError(format!("Failed to read key_len: {}", e)))?;
        let key_len = u16::from_le_bytes(key_len_bytes) as usize;

        // Read each SqlValue in the key
        let mut key = Vec::with_capacity(key_len);
        for _ in 0..key_len {
            let value = read_sql_value(&mut cursor)?;
            key.push(value);
        }
        node.keys.push(key);
    }

    // Read children page IDs (num_keys + 1 children)
    for _ in 0..=num_keys {
        let mut page_id_bytes = [0u8; 8];
        std::io::Read::read_exact(&mut cursor, &mut page_id_bytes)
            .map_err(|e| StorageError::IoError(format!("Failed to read child_page_id: {}", e)))?;
        let child_page_id = u64::from_le_bytes(page_id_bytes);
        node.children.push(child_page_id);
    }

    Ok(node)
}

/// Write a leaf node to disk
pub fn write_leaf_node(
    page_manager: &Arc<PageManager>,
    node: &LeafNode,
    _degree: usize,
) -> Result<(), StorageError> {
    let mut page = Page::new(node.page_id);
    let mut cursor = Cursor::new(&mut page.data[..]);

    // Write page type
    cursor
        .write_all(&[PAGE_TYPE_LEAF])
        .map_err(|e| StorageError::IoError(format!("Failed to write page type: {}", e)))?;

    // Write number of entries
    let num_entries = node.entries.len() as u16;
    cursor
        .write_all(&num_entries.to_le_bytes())
        .map_err(|e| StorageError::IoError(format!("Failed to write num_entries: {}", e)))?;

    // Write entries
    for (key, row_id) in &node.entries {
        // Write key length
        let key_len = key.len() as u16;
        cursor
            .write_all(&key_len.to_le_bytes())
            .map_err(|e| StorageError::IoError(format!("Failed to write key_len: {}", e)))?;

        // Write each SqlValue in the key
        for value in key {
            write_sql_value(&mut cursor, value)?;
        }

        // Write row_id
        cursor
            .write_all(&(*row_id as u64).to_le_bytes())
            .map_err(|e| StorageError::IoError(format!("Failed to write row_id: {}", e)))?;
    }

    // Write next_leaf pointer
    cursor
        .write_all(&node.next_leaf.to_le_bytes())
        .map_err(|e| StorageError::IoError(format!("Failed to write next_leaf: {}", e)))?;

    // Check if we exceeded page size
    let bytes_written = cursor.position() as usize;
    if bytes_written > PAGE_SIZE {
        return Err(StorageError::IoError(format!(
            "Leaf node exceeds page size: {} > {}",
            bytes_written, PAGE_SIZE
        )));
    }

    page.mark_dirty();
    page_manager.write_page(&mut page)?;

    Ok(())
}

/// Read a leaf node from disk
pub fn read_leaf_node(
    page_manager: &Arc<PageManager>,
    page_id: PageId,
) -> Result<LeafNode, StorageError> {
    let page = page_manager.read_page(page_id)?;
    let mut cursor = Cursor::new(&page.data[..]);

    // Read and verify page type
    let mut page_type = [0u8; 1];
    std::io::Read::read_exact(&mut cursor, &mut page_type)
        .map_err(|e| StorageError::IoError(format!("Failed to read page type: {}", e)))?;

    if page_type[0] != PAGE_TYPE_LEAF {
        return Err(StorageError::IoError(format!(
            "Expected leaf node, got page type {}",
            page_type[0]
        )));
    }

    // Read number of entries
    let mut num_entries_bytes = [0u8; 2];
    std::io::Read::read_exact(&mut cursor, &mut num_entries_bytes)
        .map_err(|e| StorageError::IoError(format!("Failed to read num_entries: {}", e)))?;
    let num_entries = u16::from_le_bytes(num_entries_bytes) as usize;

    let mut node = LeafNode::new(page_id);

    // Read entries
    for _ in 0..num_entries {
        // Read key length
        let mut key_len_bytes = [0u8; 2];
        std::io::Read::read_exact(&mut cursor, &mut key_len_bytes)
            .map_err(|e| StorageError::IoError(format!("Failed to read key_len: {}", e)))?;
        let key_len = u16::from_le_bytes(key_len_bytes) as usize;

        // Read each SqlValue in the key
        let mut key = Vec::with_capacity(key_len);
        for _ in 0..key_len {
            let value = read_sql_value(&mut cursor)?;
            key.push(value);
        }

        // Read row_id
        let mut row_id_bytes = [0u8; 8];
        std::io::Read::read_exact(&mut cursor, &mut row_id_bytes)
            .map_err(|e| StorageError::IoError(format!("Failed to read row_id: {}", e)))?;
        let row_id = u64::from_le_bytes(row_id_bytes) as usize;

        node.entries.push((key, row_id));
    }

    // Read next_leaf pointer
    let mut next_leaf_bytes = [0u8; 8];
    std::io::Read::read_exact(&mut cursor, &mut next_leaf_bytes)
        .map_err(|e| StorageError::IoError(format!("Failed to read next_leaf: {}", e)))?;
    node.next_leaf = u64::from_le_bytes(next_leaf_bytes);

    Ok(node)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::PageManager;
    use tempfile::TempDir;
    use vibesql_types::SqlValue;

    #[test]
    fn test_serialize_deserialize_internal_node() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.db");
        let page_manager = Arc::new(PageManager::new(&path).unwrap());

        let page_id = page_manager.allocate_page().unwrap();

        // Create internal node
        let mut node = InternalNode::new(page_id);
        node.keys = vec![
            vec![SqlValue::Integer(10)],
            vec![SqlValue::Integer(20)],
            vec![SqlValue::Integer(30)],
        ];
        node.children = vec![2, 3, 4, 5];

        // Write node
        write_internal_node(&page_manager, &node, 100).unwrap();

        // Read node back
        let loaded_node = read_internal_node(&page_manager, page_id).unwrap();

        // Verify
        assert_eq!(loaded_node.page_id, page_id);
        assert_eq!(loaded_node.keys.len(), 3);
        assert_eq!(loaded_node.children.len(), 4);
        assert_eq!(loaded_node.keys[0][0], SqlValue::Integer(10));
        assert_eq!(loaded_node.keys[1][0], SqlValue::Integer(20));
        assert_eq!(loaded_node.keys[2][0], SqlValue::Integer(30));
        assert_eq!(loaded_node.children, vec![2, 3, 4, 5]);
    }

    #[test]
    fn test_serialize_deserialize_leaf_node() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.db");
        let page_manager = Arc::new(PageManager::new(&path).unwrap());

        let page_id = page_manager.allocate_page().unwrap();

        // Create leaf node
        let mut node = LeafNode::new(page_id);
        node.entries = vec![
            (vec![SqlValue::Integer(5)], 100),
            (vec![SqlValue::Integer(10)], 200),
            (vec![SqlValue::Integer(15)], 300),
        ];
        node.next_leaf = 42;

        // Write node
        write_leaf_node(&page_manager, &node, 100).unwrap();

        // Read node back
        let loaded_node = read_leaf_node(&page_manager, page_id).unwrap();

        // Verify
        assert_eq!(loaded_node.page_id, page_id);
        assert_eq!(loaded_node.entries.len(), 3);
        assert_eq!(loaded_node.entries[0], (vec![SqlValue::Integer(5)], 100));
        assert_eq!(loaded_node.entries[1], (vec![SqlValue::Integer(10)], 200));
        assert_eq!(loaded_node.entries[2], (vec![SqlValue::Integer(15)], 300));
        assert_eq!(loaded_node.next_leaf, 42);
    }

    #[test]
    fn test_serialize_multi_column_key() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.db");
        let page_manager = Arc::new(PageManager::new(&path).unwrap());

        let page_id = page_manager.allocate_page().unwrap();

        // Create leaf node with multi-column keys
        let mut node = LeafNode::new(page_id);
        node.entries = vec![
            (
                vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())],
                100,
            ),
            (
                vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())],
                200,
            ),
        ];
        node.next_leaf = 0;

        // Write node
        write_leaf_node(&page_manager, &node, 100).unwrap();

        // Read node back
        let loaded_node = read_leaf_node(&page_manager, page_id).unwrap();

        // Verify
        assert_eq!(loaded_node.entries.len(), 2);
        assert_eq!(
            loaded_node.entries[0].0,
            vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())]
        );
        assert_eq!(
            loaded_node.entries[1].0,
            vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())]
        );
    }
}
