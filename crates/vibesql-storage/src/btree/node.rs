//! B+ Tree Node Structures and Operations
//!
//! This module defines the core node types and the B+ tree index implementation.

use std::sync::Arc;
use vibesql_types::{DataType, IntervalField, SqlValue};

use crate::page::{PageId, PageManager};
use crate::StorageError;

use super::{calculate_degree, NULL_PAGE_ID};

/// Serialize a DataType to bytes
///
/// Returns the number of bytes written
fn serialize_datatype(data_type: &DataType, buffer: &mut [u8]) -> Result<usize, StorageError> {
    let offset = 0;

    let bytes_written = match data_type {
        DataType::Integer => {
            buffer[offset] = 1;
            1
        }
        DataType::Smallint => {
            buffer[offset] = 2;
            1
        }
        DataType::Bigint => {
            buffer[offset] = 3;
            1
        }
        DataType::Unsigned => {
            buffer[offset] = 4;
            1
        }
        DataType::Numeric { precision, scale } => {
            buffer[offset] = 5;
            buffer[offset + 1] = *precision;
            buffer[offset + 2] = *scale;
            3
        }
        DataType::Decimal { precision, scale } => {
            buffer[offset] = 6;
            buffer[offset + 1] = *precision;
            buffer[offset + 2] = *scale;
            3
        }
        DataType::Float { precision } => {
            buffer[offset] = 7;
            buffer[offset + 1] = *precision;
            2
        }
        DataType::Real => {
            buffer[offset] = 8;
            1
        }
        DataType::DoublePrecision => {
            buffer[offset] = 9;
            1
        }
        DataType::Character { length } => {
            buffer[offset] = 10;
            buffer[offset + 1..offset + 3].copy_from_slice(&(*length as u16).to_le_bytes());
            3
        }
        DataType::Varchar { max_length } => {
            buffer[offset] = 11;
            let len = max_length.unwrap_or(255) as u16;
            buffer[offset + 1..offset + 3].copy_from_slice(&len.to_le_bytes());
            3
        }
        DataType::CharacterLargeObject => {
            buffer[offset] = 12;
            1
        }
        DataType::Name => {
            buffer[offset] = 13;
            1
        }
        DataType::Boolean => {
            buffer[offset] = 14;
            1
        }
        DataType::Date => {
            buffer[offset] = 15;
            1
        }
        DataType::Time { with_timezone } => {
            buffer[offset] = 16;
            buffer[offset + 1] = if *with_timezone { 1 } else { 0 };
            2
        }
        DataType::Timestamp { with_timezone } => {
            buffer[offset] = 17;
            buffer[offset + 1] = if *with_timezone { 1 } else { 0 };
            2
        }
        DataType::Interval { start_field, end_field } => {
            buffer[offset] = 18;
            buffer[offset + 1] = interval_field_to_u8(start_field);
            buffer[offset + 2] = end_field.as_ref().map(interval_field_to_u8).unwrap_or(255);
            3
        }
        DataType::BinaryLargeObject => {
            buffer[offset] = 19;
            1
        }
        DataType::UserDefined { type_name } => {
            buffer[offset] = 20;
            let name_bytes = type_name.as_bytes();
            let len = name_bytes.len().min(255);
            buffer[offset + 1] = len as u8;
            buffer[offset + 2..offset + 2 + len].copy_from_slice(&name_bytes[..len]);
            2 + len
        }
        DataType::Null => {
            buffer[offset] = 21;
            1
        }
    };

    Ok(bytes_written)
}

/// Deserialize a DataType from bytes
///
/// Returns (DataType, bytes_read)
fn deserialize_datatype(buffer: &[u8]) -> Result<(DataType, usize), StorageError> {
    let type_tag = buffer[0];
    let mut offset = 1;

    let data_type = match type_tag {
        1 => DataType::Integer,
        2 => DataType::Smallint,
        3 => DataType::Bigint,
        4 => DataType::Unsigned,
        5 => {
            let precision = buffer[offset];
            let scale = buffer[offset + 1];
            offset += 2;
            DataType::Numeric { precision, scale }
        }
        6 => {
            let precision = buffer[offset];
            let scale = buffer[offset + 1];
            offset += 2;
            DataType::Decimal { precision, scale }
        }
        7 => {
            let precision = buffer[offset];
            offset += 1;
            DataType::Float { precision }
        }
        8 => DataType::Real,
        9 => DataType::DoublePrecision,
        10 => {
            let len_bytes: [u8; 2] = buffer[offset..offset + 2].try_into().unwrap();
            let length = u16::from_le_bytes(len_bytes) as usize;
            offset += 2;
            DataType::Character { length }
        }
        11 => {
            let len_bytes: [u8; 2] = buffer[offset..offset + 2].try_into().unwrap();
            let max_length = u16::from_le_bytes(len_bytes);
            offset += 2;
            DataType::Varchar { max_length: Some(max_length as usize) }
        }
        12 => DataType::CharacterLargeObject,
        13 => DataType::Name,
        14 => DataType::Boolean,
        15 => DataType::Date,
        16 => {
            let with_timezone = buffer[offset] != 0;
            offset += 1;
            DataType::Time { with_timezone }
        }
        17 => {
            let with_timezone = buffer[offset] != 0;
            offset += 1;
            DataType::Timestamp { with_timezone }
        }
        18 => {
            let start_field = u8_to_interval_field(buffer[offset]);
            let end_field_u8 = buffer[offset + 1];
            let end_field = if end_field_u8 == 255 { None } else { Some(u8_to_interval_field(end_field_u8)) };
            offset += 2;
            DataType::Interval { start_field, end_field }
        }
        19 => DataType::BinaryLargeObject,
        20 => {
            let len = buffer[offset] as usize;
            offset += 1;
            let type_name = String::from_utf8_lossy(&buffer[offset..offset + len]).to_string();
            offset += len;
            DataType::UserDefined { type_name }
        }
        21 => DataType::Null,
        _ => return Err(StorageError::IoError(format!("Invalid DataType tag: {}", type_tag))),
    };

    Ok((data_type, offset))
}

/// Convert IntervalField to u8 for serialization
fn interval_field_to_u8(field: &IntervalField) -> u8 {
    match field {
        IntervalField::Year => 0,
        IntervalField::Month => 1,
        IntervalField::Day => 2,
        IntervalField::Hour => 3,
        IntervalField::Minute => 4,
        IntervalField::Second => 5,
    }
}

/// Convert u8 to IntervalField for deserialization
fn u8_to_interval_field(value: u8) -> IntervalField {
    match value {
        0 => IntervalField::Year,
        1 => IntervalField::Month,
        2 => IntervalField::Day,
        3 => IntervalField::Hour,
        4 => IntervalField::Minute,
        5 => IntervalField::Second,
        _ => IntervalField::Year, // Default fallback
    }
}

/// Type alias for multi-column keys (compatible with existing IndexData)
pub type Key = Vec<SqlValue>;

/// Type alias for row identifiers (row index in table)
pub type RowId = usize;

/// Internal node in the B+ tree
///
/// Internal nodes store keys and page IDs pointing to child nodes.
/// They do not store actual data values, only routing information.
#[derive(Debug, Clone)]
pub struct InternalNode {
    /// Page ID of this node
    pub page_id: PageId,
    /// Separator keys (length = num_children - 1)
    pub keys: Vec<Key>,
    /// Child page IDs (length = num_children)
    pub children: Vec<PageId>,
}

impl InternalNode {
    /// Create a new internal node
    pub fn new(page_id: PageId) -> Self {
        InternalNode {
            page_id,
            keys: Vec::new(),
            children: Vec::new(),
        }
    }

    /// Check if the node is full (needs splitting)
    ///
    /// This method will be used in Part 2b for tree-level insert operations
    #[allow(dead_code)]
    pub fn is_full(&self, degree: usize) -> bool {
        self.children.len() >= degree
    }

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

    /// Split this internal node into two nodes
    ///
    /// Returns (middle_key, new_right_node)
    pub fn split(&mut self, new_page_id: PageId) -> (Key, InternalNode) {
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

/// Leaf node in the B+ tree
///
/// Leaf nodes store the actual key-value pairs (key -> row_id).
/// They also maintain a linked list structure via next_leaf for range scans.
#[derive(Debug, Clone)]
pub struct LeafNode {
    /// Page ID of this node
    pub page_id: PageId,
    /// Key-value entries (sorted by key)
    pub entries: Vec<(Key, RowId)>,
    /// Page ID of next leaf node (for range scans), or NULL_PAGE_ID
    pub next_leaf: PageId,
}

impl LeafNode {
    /// Create a new leaf node
    pub fn new(page_id: PageId) -> Self {
        LeafNode {
            page_id,
            entries: Vec::new(),
            next_leaf: NULL_PAGE_ID,
        }
    }

    /// Check if the node is full (needs splitting)
    ///
    /// This method will be used in Part 2b for tree-level insert operations
    #[allow(dead_code)]
    pub fn is_full(&self, degree: usize) -> bool {
        self.entries.len() >= degree
    }

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

    /// Split this leaf node into two nodes
    ///
    /// Returns (middle_key, new_right_node)
    pub fn split(&mut self, new_page_id: PageId) -> (Key, LeafNode) {
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

    /// Check if node is underfull (needs merging)
    ///
    /// This method will be used in Part 2b for tree-level delete operations
    #[allow(dead_code)]
    pub fn is_underfull(&self, degree: usize) -> bool {
        self.entries.len() < degree / 2
    }
}

/// B+ Tree Index with disk-backed storage
///
/// This structure provides a disk-backed B+ tree index that maintains
/// sorted key-value mappings with efficient range query support.
pub struct BTreeIndex {
    /// Page ID of the root node
    root_page_id: PageId,
    /// Key schema (data types of key columns)
    key_schema: Vec<DataType>,
    /// Maximum degree (fanout) of the tree
    degree: usize,
    /// Height of the tree (0 = empty, 1 = root is leaf)
    height: usize,
    /// Page manager for disk I/O
    page_manager: Arc<PageManager>,
    /// Metadata page ID (always page 0)
    /// This field will be used when supporting multiple B+ trees with different metadata pages
    #[allow(dead_code)]
    metadata_page_id: PageId,
}

impl BTreeIndex {
    /// Create a new B+ tree index
    ///
    /// # Arguments
    /// * `page_manager` - Page manager for disk I/O
    /// * `key_schema` - Data types of key columns
    ///
    /// # Returns
    /// A new B+ tree index with an empty root
    pub fn new(
        page_manager: Arc<PageManager>,
        key_schema: Vec<DataType>,
    ) -> Result<Self, StorageError> {
        let degree = calculate_degree(&key_schema);
        let metadata_page_id = 0;

        // Allocate page for root node
        let root_page_id = page_manager.allocate_page()?;

        // Create empty root leaf node
        let root_leaf = LeafNode::new(root_page_id);

        let index = BTreeIndex {
            root_page_id,
            key_schema,
            degree,
            height: 1,
            page_manager,
            metadata_page_id,
        };

        // Write empty root to disk
        index.write_leaf_node(&root_leaf)?;

        // Save metadata
        index.save_metadata()?;

        Ok(index)
    }

    /// Load an existing B+ tree index from disk
    ///
    /// # Arguments
    /// * `page_manager` - Page manager for disk I/O
    ///
    /// # Returns
    /// Loaded B+ tree index
    pub fn load(page_manager: Arc<PageManager>) -> Result<Self, StorageError> {
        let metadata_page = page_manager.read_page(0)?;

        // Parse metadata
        let mut offset = 0;

        // Read root_page_id
        let root_page_id_bytes: [u8; 8] =
            metadata_page.data[offset..offset + 8].try_into().unwrap();
        let root_page_id = u64::from_le_bytes(root_page_id_bytes);
        offset += 8;

        // Read degree
        let degree_bytes: [u8; 2] = metadata_page.data[offset..offset + 2].try_into().unwrap();
        let degree = u16::from_le_bytes(degree_bytes) as usize;
        offset += 2;

        // Read height
        let height_bytes: [u8; 2] = metadata_page.data[offset..offset + 2].try_into().unwrap();
        let height = u16::from_le_bytes(height_bytes) as usize;
        offset += 2;

        // Read key_schema length
        let schema_len_bytes: [u8; 2] =
            metadata_page.data[offset..offset + 2].try_into().unwrap();
        let schema_len = u16::from_le_bytes(schema_len_bytes) as usize;
        offset += 2;

        // Read key_schema using deserialize_datatype
        let mut key_schema = Vec::new();
        for _ in 0..schema_len {
            let (data_type, bytes_read) = deserialize_datatype(&metadata_page.data[offset..])?;
            key_schema.push(data_type);
            offset += bytes_read;
        }

        Ok(BTreeIndex {
            root_page_id,
            key_schema,
            degree,
            height,
            page_manager,
            metadata_page_id: 0,
        })
    }

    /// Save metadata to page 0
    fn save_metadata(&self) -> Result<(), StorageError> {
        let mut metadata_page = self.page_manager.read_page(0)?;
        let mut offset = 0;

        // Write root_page_id
        metadata_page.data[offset..offset + 8]
            .copy_from_slice(&self.root_page_id.to_le_bytes());
        offset += 8;

        // Write degree
        metadata_page.data[offset..offset + 2]
            .copy_from_slice(&(self.degree as u16).to_le_bytes());
        offset += 2;

        // Write height
        metadata_page.data[offset..offset + 2]
            .copy_from_slice(&(self.height as u16).to_le_bytes());
        offset += 2;

        // Write key_schema length
        metadata_page.data[offset..offset + 2]
            .copy_from_slice(&(self.key_schema.len() as u16).to_le_bytes());
        offset += 2;

        // Write key_schema using serialize_datatype
        for data_type in &self.key_schema {
            let bytes_written = serialize_datatype(data_type, &mut metadata_page.data[offset..])?;
            offset += bytes_written;
        }

        metadata_page.mark_dirty();
        self.page_manager.write_page(&mut metadata_page)?;

        Ok(())
    }

    /// Get the degree of this B+ tree
    pub fn degree(&self) -> usize {
        self.degree
    }

    /// Get the height of this B+ tree
    pub fn height(&self) -> usize {
        self.height
    }

    /// Get the root page ID
    pub fn root_page_id(&self) -> PageId {
        self.root_page_id
    }

    // Placeholder methods for serialization (implemented in serialize.rs)

    /// Write an internal node to disk
    ///
    /// This method will be used in Part 2b for tree-level operations
    #[allow(dead_code)]
    pub(crate) fn write_internal_node(&self, node: &InternalNode) -> Result<(), StorageError> {
        super::serialize::write_internal_node(&self.page_manager, node, self.degree)
    }

    pub(crate) fn write_leaf_node(&self, node: &LeafNode) -> Result<(), StorageError> {
        super::serialize::write_leaf_node(&self.page_manager, node, self.degree)
    }

    /// Read an internal node from disk
    ///
    /// This method will be used in Part 2b for tree-level operations
    #[allow(dead_code)]
    pub(crate) fn read_internal_node(&self, page_id: PageId) -> Result<InternalNode, StorageError> {
        super::serialize::read_internal_node(&self.page_manager, page_id)
    }

    /// Read a leaf node from disk
    ///
    /// This method will be used in Part 2b for tree-level operations
    #[allow(dead_code)]
    pub(crate) fn read_leaf_node(&self, page_id: PageId) -> Result<LeafNode, StorageError> {
        super::serialize::read_leaf_node(&self.page_manager, page_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
