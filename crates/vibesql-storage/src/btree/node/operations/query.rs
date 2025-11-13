//! Query operations for BTreeIndex
//!
//! This module contains all read operations including point lookups, range scans,
//! and multi-key lookups.

use crate::StorageError;

use super::super::structure::{Key, RowId};
use super::super::btree_index::BTreeIndex;

impl BTreeIndex {
    /// Look up all row IDs for a given key
    ///
    /// For non-unique indexes, this returns all row IDs associated with the key.
    /// For unique indexes, the Vec will contain at most one element.
    ///
    /// # Arguments
    /// * `key` - The key to search for
    ///
    /// # Returns
    /// * Vector of row_ids associated with this key (empty Vec if key not found)
    ///
    /// # Performance
    /// - O(log n) to find the key (where n = number of unique keys)
    /// - O(k) to clone row IDs (where k = number of duplicates)
    /// - Total: O(log n + k)
    ///
    /// # Example
    /// ```ignore
    /// use vibesql_types::SqlValue;
    ///
    /// // Insert duplicate keys
    /// index.insert(vec![SqlValue::Integer(42)], 1)?;
    /// index.insert(vec![SqlValue::Integer(42)], 2)?;
    /// index.insert(vec![SqlValue::Integer(42)], 3)?;
    ///
    /// // Lookup returns all row IDs
    /// let rows = index.lookup(&vec![SqlValue::Integer(42)])?;
    /// assert_eq!(rows, vec![1, 2, 3]);
    ///
    /// // Nonexistent key returns empty Vec
    /// let rows = index.lookup(&vec![SqlValue::Integer(99)])?;
    /// assert!(rows.is_empty());
    /// ```
    ///
    /// # Algorithm
    /// 1. Navigate to the appropriate leaf node using find_leaf_path
    /// 2. Search for the key in the leaf node
    /// 3. Return all row_ids associated with the key (or empty Vec if not found)
    pub fn lookup(&self, key: &Key) -> Result<Vec<RowId>, StorageError> {
        // Find the leaf that would contain this key
        let (leaf, _) = self.find_leaf_path(key)?;

        // Search for the key in the leaf node and return all row_ids
        Ok(leaf.search(key).map(|row_ids| row_ids.clone()).unwrap_or_default())
    }

    /// Perform a range scan on the B+ tree
    ///
    /// Returns all row_ids for keys in the specified range [start_key, end_key].
    /// The range can be inclusive or exclusive on either end based on the parameters.
    ///
    /// For non-unique indexes with duplicate keys, all row IDs for each key in the
    /// range are included in the result. The row IDs are returned in key order, with
    /// all duplicates for each key grouped together.
    ///
    /// # Arguments
    /// * `start_key` - Optional lower bound key (None = start from beginning)
    /// * `end_key` - Optional upper bound key (None = scan to end)
    /// * `inclusive_start` - Whether start_key is inclusive (default: true)
    /// * `inclusive_end` - Whether end_key is inclusive (default: true)
    ///
    /// # Returns
    /// Vector of row_ids in sorted key order (includes all duplicates)
    ///
    /// # Performance
    /// - O(log n) to find starting leaf (where n = number of unique keys)
    /// - O(m + k) to scan through results (where m = keys in range, k = total duplicates)
    /// - Total: O(log n + m + k)
    ///
    /// # Example
    /// ```ignore
    /// use vibesql_types::SqlValue;
    ///
    /// // Insert some data with duplicates
    /// index.insert(vec![SqlValue::Integer(10)], 1)?;
    /// index.insert(vec![SqlValue::Integer(20)], 2)?;
    /// index.insert(vec![SqlValue::Integer(20)], 3)?; // duplicate
    /// index.insert(vec![SqlValue::Integer(30)], 4)?;
    ///
    /// // Range scan [15, 25] returns both row IDs for key 20
    /// let rows = index.range_scan(
    ///     Some(&vec![SqlValue::Integer(15)]),
    ///     Some(&vec![SqlValue::Integer(25)]),
    ///     true,
    ///     true
    /// )?;
    /// assert_eq!(rows, vec![2, 3]); // Both duplicates included
    /// ```
    ///
    /// # Algorithm
    /// 1. Find the starting leaf node
    /// 2. Iterate through leaf nodes using next_leaf pointers
    /// 3. Collect all row_ids (including duplicates) within the range
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
            for (key, row_ids) in &current_leaf.entries {
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

                // Key is in range, add all row_ids
                result.extend(row_ids.iter().copied());
            }

            // Move to next leaf
            if current_leaf.next_leaf == super::super::super::NULL_PAGE_ID {
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
    /// For each key, perform a lookup and collect all row_ids.
    /// This is a simple implementation that performs individual lookups.
    /// A more optimized version could sort keys and batch lookups by leaf node.
    pub fn multi_lookup(&self, keys: &[Key]) -> Result<Vec<RowId>, StorageError> {
        let mut result = Vec::new();

        for key in keys {
            let row_ids = self.lookup(key)?;
            result.extend(row_ids);
        }

        Ok(result)
    }
}
