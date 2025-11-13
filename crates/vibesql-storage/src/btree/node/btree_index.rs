//! B+ Tree Index Implementation
//!
//! This module contains the main BTreeIndex structure that coordinates
//! all node operations and manages the tree as a whole.

use std::sync::Arc;
use vibesql_types::DataType;

use crate::page::{PageId, PageManager};
use crate::StorageError;

use super::super::calculate_degree;
use super::datatype_serialization::{deserialize_datatype, serialize_datatype};
use super::structure::{InternalNode, Key, LeafNode};

/// B+ Tree Index with disk-backed storage
///
/// This structure provides a disk-backed B+ tree index that maintains
/// sorted key-value mappings with efficient range query support.
#[derive(Debug)]
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
        super::super::serialize::write_internal_node(&self.page_manager, node, self.degree)
    }

    pub(crate) fn write_leaf_node(&self, node: &LeafNode) -> Result<(), StorageError> {
        super::super::serialize::write_leaf_node(&self.page_manager, node, self.degree)
    }

    /// Read an internal node from disk
    ///
    /// This method will be used in Part 2b for tree-level operations
    #[allow(dead_code)]
    pub(crate) fn read_internal_node(&self, page_id: PageId) -> Result<InternalNode, StorageError> {
        super::super::serialize::read_internal_node(&self.page_manager, page_id)
    }

    /// Read a leaf node from disk
    ///
    /// This method will be used in Part 2b for tree-level operations
    #[allow(dead_code)]
    pub(crate) fn read_leaf_node(&self, page_id: PageId) -> Result<LeafNode, StorageError> {
        super::super::serialize::read_leaf_node(&self.page_manager, page_id)
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
        sorted_entries: Vec<(Key, super::structure::RowId)>,
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
                let mut prev_leaf = super::super::serialize::read_leaf_node(&page_manager, prev_id)?;
                prev_leaf.next_leaf = page_id;
                super::super::serialize::write_leaf_node(&page_manager, &prev_leaf, degree)?;
            }

            // Write current leaf to disk
            super::super::serialize::write_leaf_node(&page_manager, &leaf, degree)?;

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
                super::super::serialize::write_internal_node(&page_manager, &internal, degree)?;

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

    /// Delete a key from the B+ tree
    ///
    /// Implements full multi-level tree deletion with node merging and rebalancing.
    /// When a deletion causes a leaf node to become underfull, it will try to borrow
    /// entries from sibling nodes or merge with a sibling if borrowing isn't possible.
    ///
    /// # Arguments
    /// * `key` - The key to delete
    ///
    /// # Returns
    /// * `Ok(true)` if the key was found and deleted
    /// * `Ok(false)` if the key was not found
    /// * `Err(_)` if an I/O error occurred
    ///
    /// # Algorithm
    /// 1. Find the leaf node containing the key
    /// 2. Delete the key from the leaf
    /// 3. If leaf becomes underfull, try to borrow from sibling or merge
    /// 4. Propagate rebalancing up the tree if necessary
    /// 5. Collapse the root if it has only one child
    pub fn delete(&mut self, key: &Key) -> Result<bool, StorageError> {
        // Handle single-level tree (root is leaf)
        if self.height == 1 {
            let mut root_leaf = self.read_leaf_node(self.root_page_id)?;
            let deleted = root_leaf.delete(key);
            if deleted {
                self.write_leaf_node(&root_leaf)?;
            }
            return Ok(deleted);
        }

        // Multi-level tree: find leaf and track path
        let (mut leaf, path) = self.find_leaf_path(key)?;

        // Delete from leaf
        if !leaf.delete(key) {
            return Ok(false);  // Key not found
        }

        // Write leaf back
        self.write_leaf_node(&leaf)?;

        // Check if rebalancing needed
        if leaf.is_underfull(self.degree) {
            self.rebalance_leaf(leaf, path)?;
        }

        // Check if root should be collapsed
        self.maybe_collapse_root()?;

        Ok(true)
    }

    /// Find the leaf node that should contain the given key
    ///
    /// Returns the leaf node and the path from root to leaf.
    /// Path is a vector of (parent_page_id, child_index) pairs.
    fn find_leaf_path(&self, key: &Key) -> Result<(LeafNode, Vec<(PageId, usize)>), StorageError> {
        let mut path = Vec::new();
        let mut current_page_id = self.root_page_id;
        let mut current_height = self.height;

        // Navigate down to leaf level
        while current_height > 1 {
            let internal = self.read_internal_node(current_page_id)?;
            let child_idx = internal.find_child_index(key);
            let child_page_id = internal.children[child_idx];

            path.push((current_page_id, child_idx));
            current_page_id = child_page_id;
            current_height -= 1;
        }

        let leaf = self.read_leaf_node(current_page_id)?;
        Ok((leaf, path))
    }

    /// Rebalance a leaf node after deletion
    ///
    /// Tries to borrow from siblings first, then merges if necessary.
    fn rebalance_leaf(&mut self, leaf: LeafNode, path: Vec<(PageId, usize)>) -> Result<(), StorageError> {
        if path.is_empty() {
            // Leaf is root, no rebalancing needed
            return Ok(());
        }

        let (parent_id, child_idx) = *path.last().unwrap();
        let mut parent = self.read_internal_node(parent_id)?;

        // Try to borrow from sibling
        if self.try_borrow_leaf(&leaf, &mut parent, child_idx)? {
            // Successfully borrowed
            self.write_internal_node(&parent)?;
            return Ok(());
        }

        // Must merge with sibling
        self.merge_leaf(leaf, &mut parent, child_idx)?;
        self.write_internal_node(&parent)?;

        // Check if parent is now underfull
        if parent.children.len() < self.degree / 2 && path.len() > 1 {
            // Propagate rebalancing up
            self.propagate_rebalance(path)?;
        }

        Ok(())
    }

    /// Try to borrow entries from sibling leaf nodes
    ///
    /// Returns true if successfully borrowed, false if no sibling has spare entries.
    fn try_borrow_leaf(&mut self, leaf: &LeafNode, parent: &mut InternalNode, idx: usize) -> Result<bool, StorageError> {
        // Try left sibling
        if idx > 0 {
            let mut left_sibling = self.read_leaf_node(parent.children[idx - 1])?;
            if left_sibling.entries.len() > self.degree / 2 {
                // Borrow last entry from left sibling
                let borrowed = left_sibling.entries.pop().unwrap();

                let mut updated_leaf = leaf.clone();
                updated_leaf.entries.insert(0, borrowed);

                // Update parent separator key
                parent.keys[idx - 1] = updated_leaf.entries[0].0.clone();

                // Write nodes back
                self.write_leaf_node(&left_sibling)?;
                self.write_leaf_node(&updated_leaf)?;

                return Ok(true);
            }
        }

        // Try right sibling
        if idx < parent.children.len() - 1 {
            let mut right_sibling = self.read_leaf_node(parent.children[idx + 1])?;
            if right_sibling.entries.len() > self.degree / 2 {
                // Borrow first entry from right sibling
                let borrowed = right_sibling.entries.remove(0);

                let mut updated_leaf = leaf.clone();
                updated_leaf.entries.push(borrowed);

                // Update parent separator key
                parent.keys[idx] = right_sibling.entries[0].0.clone();

                // Write nodes back
                self.write_leaf_node(&right_sibling)?;
                self.write_leaf_node(&updated_leaf)?;

                return Ok(true);
            }
        }

        Ok(false)  // No sibling has enough entries to borrow
    }

    /// Merge underfull leaf with sibling
    fn merge_leaf(&mut self, leaf: LeafNode, parent: &mut InternalNode, idx: usize) -> Result<(), StorageError> {
        // Prefer merging with left sibling
        if idx > 0 {
            let mut left_sibling = self.read_leaf_node(parent.children[idx - 1])?;

            // Merge leaf into left sibling
            left_sibling.entries.extend(leaf.entries);
            left_sibling.next_leaf = leaf.next_leaf;

            self.write_leaf_node(&left_sibling)?;

            // Remove from parent
            parent.children.remove(idx);
            parent.keys.remove(idx - 1);

            // Deallocate merged node
            self.page_manager.deallocate_page(leaf.page_id)?;
        } else {
            // Merge with right sibling
            let right_sibling = self.read_leaf_node(parent.children[idx + 1])?;

            // Merge right into leaf
            let mut updated_leaf = leaf.clone();
            updated_leaf.entries.extend(right_sibling.entries);
            updated_leaf.next_leaf = right_sibling.next_leaf;

            self.write_leaf_node(&updated_leaf)?;

            // Remove from parent
            parent.children.remove(idx + 1);
            parent.keys.remove(idx);

            // Deallocate merged node
            self.page_manager.deallocate_page(right_sibling.page_id)?;
        }

        Ok(())
    }

    /// Propagate rebalancing up the tree
    fn propagate_rebalance(&mut self, path: Vec<(PageId, usize)>) -> Result<(), StorageError> {
        // Process from bottom to top (skip the last entry which we already handled)
        for i in (0..path.len() - 1).rev() {
            let (parent_id, child_idx) = path[i];
            let (current_id, _) = path[i + 1];

            let mut parent = self.read_internal_node(parent_id)?;
            let current = self.read_internal_node(current_id)?;

            // Check if current node is underfull
            if current.children.len() < self.degree / 2 {
                // Try to borrow or merge at internal node level
                if !self.try_borrow_internal(&current, &mut parent, child_idx)? {
                    self.merge_internal(current, &mut parent, child_idx)?;
                }
                self.write_internal_node(&parent)?;
            } else {
                // Parent is OK, stop propagating
                self.write_internal_node(&parent)?;
                break;
            }
        }

        Ok(())
    }

    /// Try to borrow entries from sibling internal nodes
    fn try_borrow_internal(&mut self, node: &InternalNode, parent: &mut InternalNode, idx: usize) -> Result<bool, StorageError> {
        // Try left sibling
        if idx > 0 {
            let mut left_sibling = self.read_internal_node(parent.children[idx - 1])?;
            if left_sibling.children.len() > self.degree / 2 {
                // Borrow last child and key from left sibling
                let borrowed_child = left_sibling.children.pop().unwrap();
                let borrowed_key = left_sibling.keys.pop().unwrap();

                let mut updated_node = node.clone();
                updated_node.children.insert(0, borrowed_child);
                updated_node.keys.insert(0, parent.keys[idx - 1].clone());

                // Update parent separator key
                parent.keys[idx - 1] = borrowed_key;

                // Write nodes back
                self.write_internal_node(&left_sibling)?;
                self.write_internal_node(&updated_node)?;

                return Ok(true);
            }
        }

        // Try right sibling
        if idx < parent.children.len() - 1 {
            let mut right_sibling = self.read_internal_node(parent.children[idx + 1])?;
            if right_sibling.children.len() > self.degree / 2 {
                // Borrow first child and key from right sibling
                let borrowed_child = right_sibling.children.remove(0);
                let borrowed_key = right_sibling.keys.remove(0);

                let mut updated_node = node.clone();
                updated_node.children.push(borrowed_child);
                updated_node.keys.push(parent.keys[idx].clone());

                // Update parent separator key
                parent.keys[idx] = borrowed_key;

                // Write nodes back
                self.write_internal_node(&right_sibling)?;
                self.write_internal_node(&updated_node)?;

                return Ok(true);
            }
        }

        Ok(false)  // No sibling has enough entries to borrow
    }

    /// Merge underfull internal node with sibling
    fn merge_internal(&mut self, node: InternalNode, parent: &mut InternalNode, idx: usize) -> Result<(), StorageError> {
        // Prefer merging with left sibling
        if idx > 0 {
            let mut left_sibling = self.read_internal_node(parent.children[idx - 1])?;

            // Merge node into left sibling
            // Include the parent's separator key
            left_sibling.keys.push(parent.keys[idx - 1].clone());
            left_sibling.keys.extend(node.keys);
            left_sibling.children.extend(node.children);

            self.write_internal_node(&left_sibling)?;

            // Remove from parent
            parent.children.remove(idx);
            parent.keys.remove(idx - 1);

            // Deallocate merged node
            self.page_manager.deallocate_page(node.page_id)?;
        } else {
            // Merge with right sibling
            let right_sibling = self.read_internal_node(parent.children[idx + 1])?;

            // Merge right into node
            let mut updated_node = node.clone();
            updated_node.keys.push(parent.keys[idx].clone());
            updated_node.keys.extend(right_sibling.keys);
            updated_node.children.extend(right_sibling.children);

            self.write_internal_node(&updated_node)?;

            // Remove from parent
            parent.children.remove(idx + 1);
            parent.keys.remove(idx);

            // Deallocate merged node
            self.page_manager.deallocate_page(right_sibling.page_id)?;
        }

        Ok(())
    }

    /// Check if root should be collapsed (height decrease)
    fn maybe_collapse_root(&mut self) -> Result<(), StorageError> {
        if self.height > 1 {
            let root = self.read_internal_node(self.root_page_id)?;
            if root.children.len() == 1 {
                // Collapse root
                let old_root_id = self.root_page_id;
                self.root_page_id = root.children[0];
                self.height -= 1;
                self.page_manager.deallocate_page(old_root_id)?;
                self.save_metadata()?;
            }
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
