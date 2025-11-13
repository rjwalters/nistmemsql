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

    /// Build B+ tree from pre-sorted data using bottom-up construction
    ///
    /// This is significantly faster than incremental inserts for large datasets
    /// because it avoids node splitting and builds optimally packed nodes.
    ///
    /// # Arguments
    /// * `sorted_entries` - Pre-sorted key-value pairs (must be sorted by key)
    /// * `key_schema` - Data types of key columns
    /// * `page_manager` - Page manager for disk I/O
    ///
    /// # Returns
    /// A new B+ tree index with all entries loaded
    ///
    /// # Performance
    /// - Sorting: O(n log n) if not already sorted
    /// - Leaf construction: O(n)
    /// - Internal node construction: O(n)
    /// - Total: O(n log n) dominated by sorting
    ///
    /// Approximately 10x faster than incremental insert for 100K+ rows
    pub fn bulk_load(
        sorted_entries: Vec<(Key, RowId)>,
        key_schema: Vec<DataType>,
        page_manager: Arc<PageManager>,
    ) -> Result<Self, StorageError> {
        let degree = calculate_degree(&key_schema);
        let metadata_page_id = 0;

        // Handle empty case
        if sorted_entries.is_empty() {
            return Self::new(page_manager, key_schema);
        }

        // Calculate optimal fill factor (75% - balance space vs future inserts)
        let leaf_capacity = (degree * 3) / 4;
        let leaf_capacity = leaf_capacity.max(1); // Ensure at least 1 entry per leaf

        // 1. Build leaf level
        let mut leaf_page_ids = Vec::new();
        let mut prev_leaf_page_id: Option<PageId> = None;

        let mut entries_iter = sorted_entries.into_iter().peekable();

        while entries_iter.peek().is_some() {
            // Allocate page for new leaf
            let page_id = page_manager.allocate_page()?;
            let mut leaf = LeafNode::new(page_id);

            // Fill leaf node to target capacity
            for _ in 0..leaf_capacity {
                if let Some((key, row_id)) = entries_iter.next() {
                    leaf.entries.push((key, row_id));
                } else {
                    break;
                }
            }

            // Update linked list pointers
            if let Some(prev_id) = prev_leaf_page_id {
                // We need to update the previous leaf's next_leaf pointer
                // Read previous leaf, update it, and write it back
                let mut prev_leaf = super::serialize::read_leaf_node(&page_manager, prev_id)?;
                prev_leaf.next_leaf = page_id;
                super::serialize::write_leaf_node(&page_manager, &prev_leaf, degree)?;
            }

            // Write current leaf to disk
            super::serialize::write_leaf_node(&page_manager, &leaf, degree)?;

            leaf_page_ids.push(page_id);
            prev_leaf_page_id = Some(page_id);
        }

        // 2. Build internal levels bottom-up
        let mut current_level = leaf_page_ids;
        let mut height = 1; // Start with leaf level

        while current_level.len() > 1 {
            height += 1;
            let mut next_level = Vec::new();

            // Calculate internal node capacity (similar fill factor)
            let internal_capacity = (degree * 3) / 4;
            let internal_capacity = internal_capacity.max(2); // At least 2 children

            let mut level_iter = current_level.into_iter().peekable();

            while level_iter.peek().is_some() {
                // Allocate page for new internal node
                let page_id = page_manager.allocate_page()?;
                let mut internal = InternalNode::new(page_id);

                // Add first child without a separator key
                if let Some(child_page_id) = level_iter.next() {
                    internal.children.push(child_page_id);
                }

                // Add remaining children with separator keys
                for _ in 1..internal_capacity {
                    if let Some(child_page_id) = level_iter.next() {
                        // Read first key from child node
                        let first_key = read_first_key_from_page(&page_manager, child_page_id)?;
                        internal.keys.push(first_key);
                        internal.children.push(child_page_id);
                    } else {
                        break;
                    }
                }

                // Write internal node to disk
                super::serialize::write_internal_node(&page_manager, &internal, degree)?;

                next_level.push(page_id);
            }

            current_level = next_level;
        }

        // 3. Root is the single remaining node
        let root_page_id = current_level[0];

        let index = BTreeIndex {
            root_page_id,
            key_schema,
            degree,
            height,
            page_manager,
            metadata_page_id,
        };

        // Save metadata
        index.save_metadata()?;

        Ok(index)
    }

    /// Navigate to leaf node that should contain the key, returning the path from root
    ///
    /// Returns (leaf_node, path) where path is Vec<(PageId, child_index)>
    /// The path tracks which child was taken at each internal node level
    fn find_leaf_path(&self, key: &Key) -> Result<(LeafNode, Vec<(PageId, usize)>), StorageError> {
        let mut path = Vec::new();
        let mut current_page_id = self.root_page_id;

        // Navigate down the tree
        for _ in 0..self.height - 1 {
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

    /// Propagate a split upward through internal nodes
    ///
    /// This is called after a child node splits, and needs to insert the split key
    /// into parent nodes, potentially causing cascading splits up to the root
    fn propagate_split(
        &mut self,
        mut path: Vec<(PageId, usize)>,
        mut split_key: Key,
        mut right_page_id: PageId,
    ) -> Result<(), StorageError> {
        // Work backwards up the tree
        while let Some((parent_page_id, _child_idx)) = path.pop() {
            let mut parent = self.read_internal_node(parent_page_id)?;

            // Insert the split key and right child into parent
            parent.insert_child(split_key.clone(), right_page_id);

            // Check if parent is now full and needs splitting
            if parent.is_full(self.degree) {
                // Allocate new page for right sibling
                let new_page_id = self.page_manager.allocate_page()?;

                // Split the parent
                let (middle_key, right_parent) = parent.split(new_page_id);

                // Write both nodes back
                self.write_internal_node(&parent)?;
                self.write_internal_node(&right_parent)?;

                // Continue propagating split upward
                split_key = middle_key;
                right_page_id = right_parent.page_id;
            } else {
                // No more splits needed, write parent and done
                self.write_internal_node(&parent)?;
                return Ok(());
            }
        }

        // If we get here, split propagated all the way to root
        // Need to create new root
        self.create_new_root(split_key, self.root_page_id, right_page_id)?;

        Ok(())
    }

    /// Create a new root node when a split propagates to the original root
    ///
    /// This increases the tree height by 1
    fn create_new_root(
        &mut self,
        split_key: Key,
        left_child: PageId,
        right_child: PageId,
    ) -> Result<(), StorageError> {
        // Allocate new page for the new root
        let new_root_page_id = self.page_manager.allocate_page()?;

        // Create new internal node as root
        let mut new_root = InternalNode::new(new_root_page_id);

        // Add left child, split key, and right child
        new_root.children.push(left_child);
        new_root.keys.push(split_key);
        new_root.children.push(right_child);

        // Write new root to disk
        self.write_internal_node(&new_root)?;

        // Update index metadata
        self.root_page_id = new_root_page_id;
        self.height += 1;
        self.save_metadata()?;

        Ok(())
    }

    /// Insert a key-value pair into the B+ tree
    ///
    /// # Arguments
    /// * `key` - The key to insert
    /// * `row_id` - The row ID associated with this key
    ///
    /// # Returns
    /// Ok(()) if successful, or StorageError if:
    /// - The key already exists (duplicate key error)
    /// - I/O error occurs
    ///
    /// # Implementation
    /// This method handles multi-level trees by:
    /// 1. Finding the appropriate leaf node
    /// 2. Inserting into the leaf
    /// 3. Handling splits that may propagate up the tree
    /// 4. Creating a new root if split reaches the original root
    pub fn insert(&mut self, key: Key, row_id: RowId) -> Result<(), StorageError> {
        // Special case: height 1 means root is a leaf
        if self.height == 1 {
            // Read root as leaf
            let mut root_leaf = self.read_leaf_node(self.root_page_id)?;

            // Try to insert
            if !root_leaf.insert(key.clone(), row_id) {
                return Err(StorageError::IoError("Duplicate key".to_string()));
            }

            // Check if leaf is now full and needs splitting
            if root_leaf.is_full(self.degree) {
                // Allocate page for right sibling
                let new_page_id = self.page_manager.allocate_page()?;

                // Split the leaf
                let (split_key, right_leaf) = root_leaf.split(new_page_id);

                // Write both leaves
                self.write_leaf_node(&root_leaf)?;
                self.write_leaf_node(&right_leaf)?;

                // Create new root (special case: first split increases height from 1 to 2)
                self.create_new_root(split_key, root_leaf.page_id, right_leaf.page_id)?;
            } else {
                // No split needed, just write leaf back
                self.write_leaf_node(&root_leaf)?;
            }

            return Ok(());
        }

        // Multi-level tree case
        // Find the target leaf and path from root
        let (mut leaf, path) = self.find_leaf_path(&key)?;

        // Insert into leaf
        if !leaf.insert(key.clone(), row_id) {
            return Err(StorageError::IoError("Duplicate key".to_string()));
        }

        // Check if leaf is full and needs splitting
        if leaf.is_full(self.degree) {
            // Allocate page for right sibling
            let new_page_id = self.page_manager.allocate_page()?;

            // Split the leaf
            let (split_key, right_leaf) = leaf.split(new_page_id);

            // Write both leaves
            self.write_leaf_node(&leaf)?;
            self.write_leaf_node(&right_leaf)?;

            // Propagate split upward
            self.propagate_split(path, split_key, right_leaf.page_id)?;
        } else {
            // No split needed, just write leaf back
            self.write_leaf_node(&leaf)?;
        }

        Ok(())
    }
}

/// Helper function to read the first key from a page (either internal or leaf)
fn read_first_key_from_page(
    page_manager: &Arc<PageManager>,
    page_id: PageId,
) -> Result<Key, StorageError> {
    let page = page_manager.read_page(page_id)?;

    // Read page type (first byte)
    let page_type = page.data[0];

    if page_type == super::PAGE_TYPE_LEAF {
        let leaf = super::serialize::read_leaf_node(page_manager, page_id)?;
        if leaf.entries.is_empty() {
            return Err(StorageError::IoError("Leaf node has no entries".to_string()));
        }
        Ok(leaf.entries[0].0.clone())
    } else if page_type == super::PAGE_TYPE_INTERNAL {
        let internal = super::serialize::read_internal_node(page_manager, page_id)?;
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

    #[test]
    fn test_insert_single_key() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.db");
        let page_manager = Arc::new(PageManager::new(&path).unwrap());

        let key_schema = vec![DataType::Integer];
        let mut index = BTreeIndex::new(page_manager, key_schema).unwrap();

        // Insert a single key
        let key = vec![SqlValue::Integer(42)];
        let row_id = 10;
        index.insert(key.clone(), row_id).unwrap();

        // Verify height is still 1 (root is leaf)
        assert_eq!(index.height(), 1);

        // Verify we can read the key back
        let root_leaf = index.read_leaf_node(index.root_page_id()).unwrap();
        assert_eq!(root_leaf.entries.len(), 1);
        assert_eq!(root_leaf.entries[0].0, key);
        assert_eq!(root_leaf.entries[0].1, row_id);
    }

    #[test]
    fn test_insert_duplicate_key() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.db");
        let page_manager = Arc::new(PageManager::new(&path).unwrap());

        let key_schema = vec![DataType::Integer];
        let mut index = BTreeIndex::new(page_manager, key_schema).unwrap();

        // Insert first key
        let key = vec![SqlValue::Integer(42)];
        index.insert(key.clone(), 10).unwrap();

        // Try to insert duplicate - should fail
        let result = index.insert(key.clone(), 20);
        assert!(result.is_err());
        assert!(matches!(result, Err(StorageError::IoError(_))));
    }

    #[test]
    fn test_insert_causes_leaf_split() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.db");
        let page_manager = Arc::new(PageManager::new(&path).unwrap());

        let key_schema = vec![DataType::Integer];
        let mut index = BTreeIndex::new(page_manager, key_schema).unwrap();

        let degree = index.degree();
        assert!(degree >= 5, "Degree should be at least 5");

        // Insert keys until we almost fill the root leaf (degree - 1 keys)
        for i in 0..(degree - 1) {
            let key = vec![SqlValue::Integer(i as i64 * 10)];
            index.insert(key, i).unwrap();
        }

        // Root should still be a leaf (height = 1)
        assert_eq!(index.height(), 1);

        // Insert one more key to reach capacity
        let key = vec![SqlValue::Integer((degree - 1) as i64 * 10)];
        index.insert(key, degree - 1).unwrap();

        // Now at capacity, insert one more to trigger split
        let key = vec![SqlValue::Integer(degree as i64 * 10)];
        index.insert(key, degree).unwrap();

        // Height should now be 2 (root is internal, with 2 leaf children)
        assert_eq!(index.height(), 2);

        // Root should be an internal node with 2 children
        let root = index.read_internal_node(index.root_page_id()).unwrap();
        assert_eq!(root.children.len(), 2);
        assert_eq!(root.keys.len(), 1);
    }

    #[test]
    fn test_insert_maintains_order() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.db");
        let page_manager = Arc::new(PageManager::new(&path).unwrap());

        let key_schema = vec![DataType::Integer];
        let mut index = BTreeIndex::new(page_manager, key_schema).unwrap();

        // Insert keys in random order
        let keys = vec![50, 20, 80, 10, 30, 70, 90, 40, 60];
        for (i, &k) in keys.iter().enumerate() {
            let key = vec![SqlValue::Integer(k)];
            index.insert(key, i).unwrap();
        }

        // Verify all keys are in the tree by checking the leaf nodes
        let root_leaf = index.read_leaf_node(index.root_page_id()).unwrap();

        // Verify entries are sorted
        for i in 1..root_leaf.entries.len() {
            assert!(
                root_leaf.entries[i - 1].0 < root_leaf.entries[i].0,
                "Entries should be sorted"
            );
        }
    }

    #[test]
    fn test_insert_increases_height() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.db");
        let page_manager = Arc::new(PageManager::new(&path).unwrap());

        let key_schema = vec![DataType::Integer];
        let mut index = BTreeIndex::new(page_manager, key_schema).unwrap();

        let degree = index.degree();
        let initial_height = index.height();
        assert_eq!(initial_height, 1);

        // Insert enough keys to cause height increase
        // We need slightly more than degree keys to get to height 2
        // (degree keys fill the root, degree+1 causes split to height 2)
        let num_keys = degree + 1;

        for i in 0..num_keys {
            let key = vec![SqlValue::Integer(i as i64)];
            index.insert(key, i).unwrap();
        }

        // Height should have increased to 2
        assert_eq!(
            index.height(),
            2,
            "Height should increase to 2 after inserting {} keys",
            num_keys
        );
    }

    #[test]
    fn test_large_sequential_inserts() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.db");
        let page_manager = Arc::new(PageManager::new(&path).unwrap());

        let key_schema = vec![DataType::Integer];
        let mut index = BTreeIndex::new(page_manager, key_schema).unwrap();

        // Insert 1000 keys sequentially
        let num_keys = 1000;
        for i in 0..num_keys {
            let key = vec![SqlValue::Integer(i * 10)];
            let result = index.insert(key, i as usize);
            assert!(result.is_ok(), "Insert {} failed: {:?}", i, result);
        }

        // Verify tree properties
        let degree = index.degree();
        let height = index.height();

        // Height should be logarithmic in number of keys
        // For 1000 keys with degree ~200, height should be 2-3
        assert!(height >= 2, "Height should be at least 2 for 1000 keys");
        assert!(height <= 4, "Height should not exceed 4 for 1000 keys with degree {}", degree);

        // Verify tree structure is valid
        if height > 1 {
            let root = index.read_internal_node(index.root_page_id()).unwrap();
            assert!(
                root.children.len() >= 2,
                "Root should have at least 2 children"
            );
            assert_eq!(
                root.keys.len() + 1,
                root.children.len(),
                "Internal node invariant: keys.len() + 1 == children.len()"
            );
        }
    }

    #[test]
    fn test_large_random_inserts() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.db");
        let page_manager = Arc::new(PageManager::new(&path).unwrap());

        let key_schema = vec![DataType::Integer];
        let mut index = BTreeIndex::new(page_manager, key_schema).unwrap();

        // Insert 1000 keys in shuffled order
        let mut keys: Vec<i64> = (0..1000).map(|i| i * 10).collect();

        // Simple shuffle using the key values themselves
        for i in (1..keys.len()).rev() {
            let j = (keys[i] as usize) % (i + 1);
            keys.swap(i, j);
        }

        for (i, &k) in keys.iter().enumerate() {
            let key = vec![SqlValue::Integer(k)];
            let result = index.insert(key, i);
            assert!(result.is_ok(), "Insert of key {} failed: {:?}", k, result);
        }

        // Verify tree properties
        let height = index.height();
        assert!(height >= 2, "Height should be at least 2 for 1000 random keys");
        assert!(height <= 4, "Height should not exceed 4 for 1000 keys");
    }

    #[test]
    fn test_insert_multi_column_keys() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.db");
        let page_manager = Arc::new(PageManager::new(&path).unwrap());

        let key_schema = vec![
            DataType::Integer,
            DataType::Varchar { max_length: Some(20) },
        ];
        let mut index = BTreeIndex::new(page_manager, key_schema).unwrap();

        // Insert multi-column keys
        let keys = vec![
            (vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())], 0),
            (vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())], 1),
            (vec![SqlValue::Integer(1), SqlValue::Varchar("Zoe".to_string())], 2),
        ];

        for (key, row_id) in keys {
            index.insert(key, row_id).unwrap();
        }

        // Verify entries are in sorted order
        let root_leaf = index.read_leaf_node(index.root_page_id()).unwrap();
        assert_eq!(root_leaf.entries.len(), 3);

        // Verify sorted: (1, "Alice"), (1, "Zoe"), (2, "Bob")
        assert!(root_leaf.entries[0].0[0] == SqlValue::Integer(1));
        assert!(root_leaf.entries[1].0[0] == SqlValue::Integer(1));
        assert!(root_leaf.entries[2].0[0] == SqlValue::Integer(2));
    }
}
