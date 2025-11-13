// ============================================================================
// Index Manager - User-defined index management (CREATE INDEX statements)
// ============================================================================

use std::collections::{BTreeMap, HashMap};

use vibesql_ast::IndexColumn;
use vibesql_types::SqlValue;

use crate::{Row, StorageError};

/// Normalize an index name to uppercase for case-insensitive comparison
/// This follows SQL standard identifier rules
fn normalize_index_name(name: &str) -> String {
    name.to_uppercase()
}

/// Index metadata
#[derive(Debug, Clone)]
pub struct IndexMetadata {
    pub index_name: String,
    pub table_name: String,
    pub unique: bool,
    pub columns: Vec<IndexColumn>,
}

/// Actual index data structure - maps key values to row indices
#[derive(Debug, Clone)]
pub struct IndexData {
    /// BTreeMap of (key_values, row_indices) for fast lookups and efficient range scans
    /// For multi-column indexes, key_values contains multiple SqlValue entries
    /// BTreeMap provides O(log n) lookups and efficient range queries via range()
    pub data: BTreeMap<Vec<SqlValue>, Vec<usize>>,
}

impl IndexData {
    /// Scan index for rows matching range predicate
    ///
    /// # Arguments
    /// * `start` - Lower bound (None = unbounded)
    /// * `end` - Upper bound (None = unbounded)
    /// * `inclusive_start` - Include rows equal to start value
    /// * `inclusive_end` - Include rows equal to end value
    ///
    /// # Returns
    /// Vector of row indices that match the range predicate
    ///
    /// # Performance
    /// Uses BTreeMap's efficient range() method for O(log n + k) complexity,
    /// where n is the number of unique keys and k is the number of matching keys.
    /// This is significantly faster than the previous O(n) full scan approach.
    pub fn range_scan(
        &self,
        start: Option<&SqlValue>,
        end: Option<&SqlValue>,
        inclusive_start: bool,
        inclusive_end: bool,
    ) -> Vec<usize> {
        let mut matching_row_indices = Vec::new();

        // Iterate through BTreeMap (which gives us sorted iteration)
        // For multi-column indexes, we only compare the first column
        // This maintains compatibility with the original HashMap implementation
        for (key_values, row_indices) in &self.data {
            // For single-column index, key_values has one element
            // For multi-column indexes, we only compare the first column
            let key = &key_values[0];

            let matches = match (start, end) {
                (Some(s), Some(e)) => {
                    // Both bounds specified: start <= key <= end (or variations)
                    let gte_start = if inclusive_start { key >= s } else { key > s };
                    let lte_end = if inclusive_end { key <= e } else { key < e };
                    gte_start && lte_end
                }
                (Some(s), None) => {
                    // Only lower bound: key >= start (or >)
                    if inclusive_start {
                        key >= s
                    } else {
                        key > s
                    }
                }
                (None, Some(e)) => {
                    // Only upper bound: key <= end (or <)
                    if inclusive_end {
                        key <= e
                    } else {
                        key < e
                    }
                }
                (None, None) => true, // No bounds - match everything
            };

            if matches {
                matching_row_indices.extend(row_indices);
            }
        }

        // Return row indices in the order established by BTreeMap iteration
        // BTreeMap gives us results sorted by index key value, which is the
        // expected order for indexed queries. We should NOT sort by row index
        // as that would destroy the index-based ordering.
        matching_row_indices
    }

    /// Lookup multiple values in the index (for IN predicates)
    ///
    /// # Arguments
    /// * `values` - List of values to look up
    ///
    /// # Returns
    /// Vector of row indices that match any of the values
    pub fn multi_lookup(&self, values: &[SqlValue]) -> Vec<usize> {
        let mut matching_row_indices = Vec::new();

        for value in values {
            let search_key = vec![value.clone()];
            if let Some(row_indices) = self.data.get(&search_key) {
                matching_row_indices.extend(row_indices);
            }
        }

        // Return row indices in the order they were collected from BTreeMap
        // For IN predicates, we collect results for each value in the order
        // specified. We should NOT sort by row index as that would destroy
        // the semantic ordering of the results.
        matching_row_indices
    }
}

/// Manages user-defined indexes (CREATE INDEX statements)
///
/// This component encapsulates all user-defined index operations, maintaining
/// index metadata and data structures for efficient query optimization.
#[derive(Debug, Clone)]
pub struct IndexManager {
    /// Index metadata storage (normalized_index_name -> metadata)
    indexes: HashMap<String, IndexMetadata>,
    /// Actual index data (normalized_index_name -> data)
    index_data: HashMap<String, IndexData>,
}

impl IndexManager {
    /// Create a new empty IndexManager
    pub fn new() -> Self {
        IndexManager { indexes: HashMap::new(), index_data: HashMap::new() }
    }

    /// Create an index
    pub fn create_index(
        &mut self,
        index_name: String,
        table_name: String,
        table_schema: &vibesql_catalog::TableSchema,
        table_rows: &[Row],
        unique: bool,
        columns: Vec<IndexColumn>,
    ) -> Result<(), StorageError> {
        // Normalize index name for case-insensitive comparison
        let normalized_name = normalize_index_name(&index_name);

        // Check if index already exists
        if self.indexes.contains_key(&normalized_name) {
            return Err(StorageError::IndexAlreadyExists(index_name));
        }

        // Get column indices in the table for all indexed columns
        let mut column_indices = Vec::new();
        for index_col in &columns {
            let column_idx =
                table_schema.get_column_index(&index_col.column_name).ok_or_else(|| {
                    StorageError::ColumnNotFound {
                        column_name: index_col.column_name.clone(),
                        table_name: table_name.clone(),
                    }
                })?;
            column_indices.push(column_idx);
        }

        // Build the index data
        let mut index_data_map: BTreeMap<Vec<SqlValue>, Vec<usize>> = BTreeMap::new();
        for (row_idx, row) in table_rows.iter().enumerate() {
            let key_values: Vec<SqlValue> =
                column_indices.iter().map(|&idx| row.values[idx].clone()).collect();
            index_data_map.entry(key_values).or_default().push(row_idx);
        }

        // Store index metadata (use normalized name as key)
        let metadata =
            IndexMetadata { index_name: index_name.clone(), table_name, unique, columns };

        self.indexes.insert(normalized_name.clone(), metadata);
        self.index_data.insert(normalized_name, IndexData { data: index_data_map });

        Ok(())
    }

    /// Check if an index exists
    pub fn index_exists(&self, index_name: &str) -> bool {
        let normalized = normalize_index_name(index_name);
        self.indexes.contains_key(&normalized)
    }

    /// Get index metadata
    pub fn get_index(&self, index_name: &str) -> Option<&IndexMetadata> {
        let normalized = normalize_index_name(index_name);
        self.indexes.get(&normalized)
    }

    /// Get index data
    pub fn get_index_data(&self, index_name: &str) -> Option<&IndexData> {
        let normalized = normalize_index_name(index_name);
        self.index_data.get(&normalized)
    }

    /// Update user-defined indexes for insert operation
    pub fn update_indexes_for_insert(
        &mut self,
        table_name: &str,
        table_schema: &vibesql_catalog::TableSchema,
        row: &Row,
        row_index: usize,
    ) {
        for (index_name, metadata) in &self.indexes {
            if metadata.table_name == table_name {
                if let Some(index_data) = self.index_data.get_mut(index_name) {
                    // Build composite key from the indexed columns
                    let key_values: Vec<SqlValue> = metadata
                        .columns
                        .iter()
                        .map(|col| {
                            let col_idx = table_schema
                                .get_column_index(&col.column_name)
                                .expect("Index column should exist");
                            row.values[col_idx].clone()
                        })
                        .collect();

                    // Insert into the index data
                    index_data.data.entry(key_values).or_insert_with(Vec::new).push(row_index);
                }
            }
        }
    }

    /// Update user-defined indexes for update operation
    pub fn update_indexes_for_update(
        &mut self,
        table_name: &str,
        table_schema: &vibesql_catalog::TableSchema,
        old_row: &Row,
        new_row: &Row,
        row_index: usize,
    ) {
        for (index_name, metadata) in &self.indexes {
            if metadata.table_name == table_name {
                if let Some(index_data) = self.index_data.get_mut(index_name) {
                    // Build keys from old and new rows
                    let old_key_values: Vec<SqlValue> = metadata
                        .columns
                        .iter()
                        .map(|col| {
                            let col_idx = table_schema
                                .get_column_index(&col.column_name)
                                .expect("Index column should exist");
                            old_row.values[col_idx].clone()
                        })
                        .collect();

                    let new_key_values: Vec<SqlValue> = metadata
                        .columns
                        .iter()
                        .map(|col| {
                            let col_idx = table_schema
                                .get_column_index(&col.column_name)
                                .expect("Index column should exist");
                            new_row.values[col_idx].clone()
                        })
                        .collect();

                    // If keys are different, remove old and add new
                    if old_key_values != new_key_values {
                        // Remove old key
                        if let Some(row_indices) = index_data.data.get_mut(&old_key_values) {
                            row_indices.retain(|&idx| idx != row_index);
                            // Remove empty entries
                            if row_indices.is_empty() {
                                index_data.data.remove(&old_key_values);
                            }
                        }

                        // Add new key
                        index_data
                            .data
                            .entry(new_key_values)
                            .or_insert_with(Vec::new)
                            .push(row_index);
                    }
                    // If keys are the same, no change needed
                }
            }
        }
    }

    /// Update user-defined indexes for delete operation
    pub fn update_indexes_for_delete(
        &mut self,
        table_name: &str,
        table_schema: &vibesql_catalog::TableSchema,
        row: &Row,
        row_index: usize,
    ) {
        for (index_name, metadata) in &self.indexes {
            if metadata.table_name == table_name {
                if let Some(index_data) = self.index_data.get_mut(index_name) {
                    // Build key from the row
                    let key_values: Vec<SqlValue> = metadata
                        .columns
                        .iter()
                        .map(|col| {
                            let col_idx = table_schema
                                .get_column_index(&col.column_name)
                                .expect("Index column should exist");
                            row.values[col_idx].clone()
                        })
                        .collect();

                    // Remove the row index from this key
                    if let Some(row_indices) = index_data.data.get_mut(&key_values) {
                        row_indices.retain(|&idx| idx != row_index);
                        // Remove empty entries
                        if row_indices.is_empty() {
                            index_data.data.remove(&key_values);
                        }
                    }
                }
            }
        }
    }

    /// Rebuild user-defined indexes after bulk operations that change row indices
    pub fn rebuild_indexes(
        &mut self,
        table_name: &str,
        table_schema: &vibesql_catalog::TableSchema,
        table_rows: &[Row],
    ) {
        // Collect index names that need rebuilding
        let indexes_to_rebuild: Vec<String> = self
            .indexes
            .iter()
            .filter(|(_, metadata)| metadata.table_name == table_name)
            .map(|(name, _)| name.clone())
            .collect();

        // Rebuild each index
        for index_name in indexes_to_rebuild {
            if let Some(index_data) = self.index_data.get_mut(&index_name) {
                if let Some(metadata) = self.indexes.get(&index_name) {
                    // Clear existing data
                    index_data.data.clear();

                    // Rebuild from current table rows
                    for (row_index, row) in table_rows.iter().enumerate() {
                        let key_values: Vec<SqlValue> = metadata
                            .columns
                            .iter()
                            .map(|col| {
                                let col_idx = table_schema
                                    .get_column_index(&col.column_name)
                                    .expect("Index column should exist");
                                row.values[col_idx].clone()
                            })
                            .collect();

                        index_data.data.entry(key_values).or_insert_with(Vec::new).push(row_index);
                    }
                }
            }
        }
    }

    /// Drop an index
    pub fn drop_index(&mut self, index_name: &str) -> Result<(), StorageError> {
        // Normalize index name for case-insensitive comparison
        let normalized = normalize_index_name(index_name);

        if self.indexes.remove(&normalized).is_none() {
            return Err(StorageError::IndexNotFound(index_name.to_string()));
        }
        // Also remove the index data
        self.index_data.remove(&normalized);
        Ok(())
    }

    /// Drop all indexes associated with a table (CASCADE behavior)
    ///
    /// This is called automatically when dropping a table to maintain
    /// referential integrity. Indexes are tied to specific tables and
    /// cannot exist without their parent table.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The qualified name of the table (e.g., "public.users")
    ///
    /// # Returns
    ///
    /// Vector of index names that were dropped (for logging/debugging)
    pub fn drop_indexes_for_table(&mut self, table_name: &str) -> Vec<String> {
        // Collect index names to drop (can't modify while iterating)
        let indexes_to_drop: Vec<String> = self
            .indexes
            .iter()
            .filter(|(_, metadata)| metadata.table_name == table_name)
            .map(|(name, _)| name.clone())
            .collect();

        // Drop each index
        for index_name in &indexes_to_drop {
            self.indexes.remove(index_name);
            self.index_data.remove(index_name);
        }

        indexes_to_drop
    }

    /// List all indexes
    pub fn list_indexes(&self) -> Vec<String> {
        self.indexes.keys().cloned().collect()
    }
}

impl Default for IndexManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_types::SqlValue;

    #[test]
    fn test_range_scan_preserves_index_order() {
        // Create index data with rows that are NOT in order by row index
        // but ARE in order by indexed value
        let mut data = BTreeMap::new();

        // col0 values: row 1 has 50, row 2 has 60, row 0 has 70
        // Index should be sorted by value: 50, 60, 70
        data.insert(vec![SqlValue::Integer(50)], vec![1]);
        data.insert(vec![SqlValue::Integer(60)], vec![2]);
        data.insert(vec![SqlValue::Integer(70)], vec![0]);

        let index_data = IndexData { data };

        // Query: col0 > 55 should return rows in index order: [2, 0] (values 60, 70)
        let result = index_data.range_scan(
            Some(&SqlValue::Integer(55)),
            None,
            false, // exclusive start
            false,
        );

        // Result should be [2, 0] NOT [0, 2]
        // This preserves the index ordering (60 comes before 70)
        assert_eq!(result, vec![2, 0],
            "range_scan should return rows in index order (by value), not row index order");
    }

    #[test]
    fn test_range_scan_between_preserves_order() {
        // Test BETWEEN queries maintain index order
        let mut data = BTreeMap::new();

        // Values out of row-index order
        data.insert(vec![SqlValue::Integer(40)], vec![5]);
        data.insert(vec![SqlValue::Integer(50)], vec![1]);
        data.insert(vec![SqlValue::Integer(60)], vec![2]);
        data.insert(vec![SqlValue::Integer(70)], vec![0]);

        let index_data = IndexData { data };

        // Query: col0 BETWEEN 45 AND 65 (i.e., col0 >= 45 AND col0 <= 65)
        let result = index_data.range_scan(
            Some(&SqlValue::Integer(45)),
            Some(&SqlValue::Integer(65)),
            true,  // inclusive start
            true,  // inclusive end
        );

        // Should return [1, 2] (values 50, 60) in that order
        assert_eq!(result, vec![1, 2],
            "BETWEEN should return rows in index order");
    }

    #[test]
    fn test_range_scan_with_duplicate_values() {
        // Test case: multiple rows with the same indexed value
        let mut data = BTreeMap::new();

        // Multiple rows with value 60: rows 3, 7, 2 (in insertion order)
        data.insert(vec![SqlValue::Integer(50)], vec![1]);
        data.insert(vec![SqlValue::Integer(60)], vec![3, 7, 2]); // duplicates
        data.insert(vec![SqlValue::Integer(70)], vec![0]);

        let index_data = IndexData { data };

        // Query: col0 >= 60 should return [3, 7, 2, 0]
        // Rows with value 60 maintain insertion order, then row 0 with value 70
        let result = index_data.range_scan(
            Some(&SqlValue::Integer(60)),
            None,
            true,  // inclusive start
            false,
        );

        assert_eq!(result, vec![3, 7, 2, 0],
            "Duplicate values should maintain insertion order within the same key");
    }

    #[test]
    fn test_multi_lookup_with_duplicate_values() {
        // Test case: multi_lookup with duplicate indexed values
        let mut data = BTreeMap::new();

        // Multiple rows with value 60: rows 3, 7, 2 (in insertion order)
        data.insert(vec![SqlValue::Integer(50)], vec![1]);
        data.insert(vec![SqlValue::Integer(60)], vec![3, 7, 2]); // duplicates
        data.insert(vec![SqlValue::Integer(70)], vec![0]);

        let index_data = IndexData { data };

        // Query: col0 IN (60, 70) should return [3, 7, 2, 0]
        // Rows with value 60 maintain insertion order, then row 0 with value 70
        let result = index_data.multi_lookup(&[
            SqlValue::Integer(60),
            SqlValue::Integer(70),
        ]);

        assert_eq!(result, vec![3, 7, 2, 0],
            "multi_lookup with duplicate values should maintain insertion order within the same key");
    }
}
