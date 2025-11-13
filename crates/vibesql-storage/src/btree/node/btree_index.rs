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
use super::structure::{InternalNode, Key, LeafNode, RowId};

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

    /// Lookup a single key in the B+ tree
    ///
    /// # Arguments
    /// * `key` - The key to search for
    ///
    /// # Returns
    /// * `Some(row_id)` if the key exists
    /// * `None` if the key is not found
    ///
    /// # Algorithm
    /// 1. Navigate to the appropriate leaf node using find_leaf_path
    /// 2. Search for the key in the leaf node
    pub fn lookup(&self, key: &Key) -> Result<Option<RowId>, StorageError> {
        // Find the leaf that would contain this key
        let (leaf, _) = self.find_leaf_path(key)?;

        // Search for the key in the leaf node
        Ok(leaf.search(key))
    }

    /// Perform a range scan on the B+ tree
    ///
    /// Returns all row_ids for keys in the specified range [start_key, end_key].
    /// The range can be inclusive or exclusive on either end based on the parameters.
    ///
    /// # Arguments
    /// * `start_key` - Optional lower bound key
    /// * `end_key` - Optional upper bound key
    /// * `inclusive_start` - Whether start_key is inclusive (default: true)
    /// * `inclusive_end` - Whether end_key is inclusive (default: true)
    ///
    /// # Returns
    /// Vector of row_ids in sorted key order
    ///
    /// # Algorithm
    /// 1. Find the starting leaf node
    /// 2. Iterate through leaf nodes using next_leaf pointers
    /// 3. Collect all row_ids within the range
    /// 4. Stop when reaching the end key or end of tree
    pub fn range_scan(
        &self,
        start_key: Option<&Key>,
        end_key: Option<&Key>,
        inclusive_start: bool,
        inclusive_end: bool,
    ) -> Result<Vec<RowId>, StorageError> {
        let mut result = Vec::new();

        // Find starting point
        let mut current_leaf = if let Some(start) = start_key {
            // Find leaf containing start_key
            let (leaf, _) = self.find_leaf_path(start)?;
            leaf
        } else {
            // Start from leftmost leaf
            self.find_leftmost_leaf()?
        };

        // Scan through leaves
        loop {
            // Process entries in current leaf
            for (key, row_id) in &current_leaf.entries {
                // Check if we're past the end key
                if let Some(end) = end_key {
                    let cmp = key.cmp(end);
                    if cmp > std::cmp::Ordering::Equal {
                        // Past end key, stop
                        return Ok(result);
                    }
                    if cmp == std::cmp::Ordering::Equal && !inclusive_end {
                        // At end key but not inclusive, stop
                        return Ok(result);
                    }
                }

                // Check if we're before the start key
                if let Some(start) = start_key {
                    let cmp = key.cmp(start);
                    if cmp < std::cmp::Ordering::Equal {
                        // Before start key, skip
                        continue;
                    }
                    if cmp == std::cmp::Ordering::Equal && !inclusive_start {
                        // At start key but not inclusive, skip
                        continue;
                    }
                }

                // Key is in range, add row_id
                result.push(*row_id);
            }

            // Move to next leaf
            if current_leaf.next_leaf == super::super::NULL_PAGE_ID {
                // No more leaves
                break;
            }
            current_leaf = self.read_leaf_node(current_leaf.next_leaf)?;
        }

        Ok(result)
    }

    /// Lookup multiple keys in the B+ tree (for IN predicates)
    ///
    /// # Arguments
    /// * `keys` - List of keys to look up
    ///
    /// # Returns
    /// Vector of row_ids for all keys that were found
    ///
    /// # Algorithm
    /// For each key, perform a lookup and collect the row_ids.
    /// This is a simple implementation that performs individual lookups.
    /// A more optimized version could sort keys and batch lookups by leaf node.
    pub fn multi_lookup(&self, keys: &[Key]) -> Result<Vec<RowId>, StorageError> {
        let mut result = Vec::new();

        for key in keys {
            if let Some(row_id) = self.lookup(key)? {
                result.push(row_id);
            }
        }

        Ok(result)
    }

    /// Find the leftmost (first) leaf node in the tree
    ///
    /// # Returns
    /// The leftmost leaf node
    fn find_leftmost_leaf(&self) -> Result<LeafNode, StorageError> {
        let mut current_page_id = self.root_page_id;

        // Navigate down the tree always taking the leftmost child
        for _ in 0..self.height - 1 {
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
            self.propagate_rebalance_delete(path)?;
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

    /// Propagate rebalancing up the tree after deletion
    fn propagate_rebalance_delete(&mut self, path: Vec<(PageId, usize)>) -> Result<(), StorageError> {
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
