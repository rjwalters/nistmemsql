// ============================================================================
// Index Manager - User-defined index management (CREATE INDEX statements)
// ============================================================================

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use vibesql_ast::IndexColumn;
use vibesql_types::{DataType, SqlValue};

use crate::btree::{BTreeIndex, Key};
use crate::database::{DatabaseConfig, ResourceTracker};
use crate::page::PageManager;
use crate::{Row, StorageBackend, StorageError};

#[cfg(not(target_arch = "wasm32"))]
use crate::NativeStorage;

#[cfg(target_arch = "wasm32")]
use crate::OpfsStorage;

/// Normalize an index name to uppercase for case-insensitive comparison
/// This follows SQL standard identifier rules
fn normalize_index_name(name: &str) -> String {
    name.to_uppercase()
}

/// Threshold for choosing disk-backed indexes (number of table rows)
/// Tables with more rows than this will use disk-backed B+ tree indexes
/// Set to very high value (100K) to keep Phase 2 conservative - disk-backed
/// indexes are functional but not enabled by default yet
///
/// For tests, disable disk-backed indexes entirely (use usize::MAX) to ensure
/// fast test execution. The specific test that verifies disk-backed functionality
/// is marked with #[ignore] and must be run explicitly.
#[cfg(not(test))]
const DISK_BACKED_THRESHOLD: usize = 100_000;

#[cfg(test)]
const DISK_BACKED_THRESHOLD: usize = usize::MAX;

/// Helper to extend a key with a row_id for non-unique disk-backed indexes
/// This allows storing multiple rows with the same key value
fn extend_key_with_row_id(key: Vec<SqlValue>, row_id: usize) -> Vec<SqlValue> {
    let mut extended = key;
    extended.push(SqlValue::Integer(row_id as i64));
    extended
}

/// Index metadata
#[derive(Debug, Clone)]
pub struct IndexMetadata {
    pub index_name: String,
    pub table_name: String,
    pub unique: bool,
    pub columns: Vec<IndexColumn>,
}

/// Backend type for index storage
#[derive(Debug, Clone)]
pub enum IndexData {
    /// In-memory BTreeMap (for small indexes or backward compatibility)
    InMemory {
        data: BTreeMap<Vec<SqlValue>, Vec<usize>>,
    },
    /// Disk-backed B+ tree (for large indexes or persistence)
    /// Note: The B+ tree stores (key, row_id) pairs. For non-unique indexes,
    /// we serialize Vec<usize> as the row_id value to support multiple rows per key.
    DiskBacked {
        btree: Arc<std::sync::Mutex<BTreeIndex>>,
        page_manager: Arc<PageManager>,
    },
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
        match self {
            IndexData::InMemory { data } => {
                let mut matching_row_indices = Vec::new();

                // Iterate through BTreeMap (which gives us sorted iteration)
                // For multi-column indexes, we only compare the first column
                // This maintains compatibility with the original HashMap implementation
                for (key_values, row_indices) in data {
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
            IndexData::DiskBacked { btree, .. } => {
                // Convert SqlValue bounds to Key (Vec<SqlValue>) bounds
                // For single-column indexes, wrap in vec
                // For multi-column indexes, only first column is compared (same as InMemory)
                let start_key = start.map(|v| vec![v.clone()]);
                let end_key = end.map(|v| vec![v.clone()]);

                // Lock and call BTreeIndex::range_scan
                btree
                    .lock()
                    .unwrap()
                    .range_scan(
                        start_key.as_ref(),
                        end_key.as_ref(),
                        inclusive_start,
                        inclusive_end,
                    )
                    .unwrap_or_else(|_| vec![])
            }
        }
    }

    /// Lookup multiple values in the index (for IN predicates)
    ///
    /// # Arguments
    /// * `values` - List of values to look up
    ///
    /// # Returns
    /// Vector of row indices that match any of the values
    pub fn multi_lookup(&self, values: &[SqlValue]) -> Vec<usize> {
        match self {
            IndexData::InMemory { data } => {
                let mut matching_row_indices = Vec::new();

                for value in values {
                    let search_key = vec![value.clone()];
                    if let Some(row_indices) = data.get(&search_key) {
                        matching_row_indices.extend(row_indices);
                    }
                }

                // Return row indices in the order they were collected from BTreeMap
                // For IN predicates, we collect results for each value in the order
                // specified. We should NOT sort by row index as that would destroy
                // the semantic ordering of the results.
                matching_row_indices
            }
            IndexData::DiskBacked { btree, .. } => {
                // Convert SqlValue values to Key (Vec<SqlValue>) format
                let keys: Vec<Vec<SqlValue>> = values
                    .iter()
                    .map(|v| vec![v.clone()])
                    .collect();

                // Lock and call BTreeIndex::multi_lookup
                btree
                    .lock()
                    .unwrap()
                    .multi_lookup(&keys)
                    .unwrap_or_else(|_| vec![])
            }
        }
    }

    /// Get an iterator over all key-value pairs in the index
    ///
    /// # Returns
    /// Iterator yielding references to (key, row_indices) pairs
    ///
    /// # Note
    /// For in-memory indexes, iteration is in sorted key order (BTreeMap ordering).
    /// This method enables index scanning operations without exposing internal data structures.
    pub fn iter(&self) -> Box<dyn Iterator<Item = (&Vec<SqlValue>, &Vec<usize>)> + '_> {
        match self {
            IndexData::InMemory { data } => Box::new(data.iter()),
            IndexData::DiskBacked { .. } => {
                // TODO: Implement when DiskBacked is active
                unimplemented!("DiskBacked iteration not yet implemented")
            }
        }
    }

    /// Lookup exact key in the index
    ///
    /// # Arguments
    /// * `key` - Key to look up
    ///
    /// # Returns
    /// Reference to vector of row indices if key exists, None otherwise
    ///
    /// # Note
    /// This is the primary point-lookup API for index queries.
    pub fn get(&self, key: &[SqlValue]) -> Option<&Vec<usize>> {
        match self {
            IndexData::InMemory { data } => data.get(key),
            IndexData::DiskBacked { .. } => {
                // TODO: Implement when DiskBacked is active
                unimplemented!("DiskBacked lookup not yet implemented")
            }
        }
    }

    /// Check if a key exists in the index
    ///
    /// # Arguments
    /// * `key` - Key to check
    ///
    /// # Returns
    /// true if key exists, false otherwise
    ///
    /// # Note
    /// Used primarily for UNIQUE constraint validation.
    pub fn contains_key(&self, key: &[SqlValue]) -> bool {
        match self {
            IndexData::InMemory { data } => data.contains_key(key),
            IndexData::DiskBacked { .. } => {
                // TODO: Implement when DiskBacked is active
                unimplemented!("DiskBacked contains_key not yet implemented")
            }
        }
    }

    /// Get an iterator over all row index vectors in the index
    ///
    /// # Returns
    /// Iterator yielding references to row index vectors
    ///
    /// # Note
    /// This method is used for full index scans where we need all row indices
    /// regardless of the key values.
    pub fn values(&self) -> Box<dyn Iterator<Item = &Vec<usize>> + '_> {
        match self {
            IndexData::InMemory { data } => Box::new(data.values()),
            IndexData::DiskBacked { .. } => {
                // TODO: Implement when DiskBacked is active
                unimplemented!("DiskBacked values iteration not yet implemented")
            }
        }
    }
}

/// Manages user-defined indexes (CREATE INDEX statements)
///
/// This component encapsulates all user-defined index operations, maintaining
/// index metadata and data structures for efficient query optimization.
///
/// Supports adaptive index management with resource budgets and LRU eviction,
/// enabling efficient operation in both browser (limited memory) and server
/// (abundant memory) environments.
#[derive(Clone)]
pub struct IndexManager {
    /// Index metadata storage (normalized_index_name -> metadata)
    indexes: HashMap<String, IndexMetadata>,
    /// Actual index data (normalized_index_name -> data)
    index_data: HashMap<String, IndexData>,
    /// Resource budget configuration
    config: DatabaseConfig,
    /// Resource usage tracker for budget enforcement
    pub(crate) resource_tracker: ResourceTracker,
    /// Database directory path for index file storage
    database_path: Option<PathBuf>,
    /// Storage backend for file operations
    storage: Arc<dyn StorageBackend>,
}

impl std::fmt::Debug for IndexManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexManager")
            .field("indexes", &self.indexes)
            .field("index_data", &self.index_data)
            .field("config", &self.config)
            .field("resource_tracker", &self.resource_tracker)
            .field("database_path", &self.database_path)
            .finish()
    }
}

impl IndexManager {
    /// Create a new empty IndexManager with default configuration
    pub fn new() -> Self {
        // Create a default in-memory storage (will be replaced when database_path is set)
        #[cfg(not(target_arch = "wasm32"))]
        let storage = Arc::new(NativeStorage::new(".").unwrap());
        #[cfg(target_arch = "wasm32")]
        let storage = Arc::new(OpfsStorage::new().unwrap());

        IndexManager {
            indexes: HashMap::new(),
            index_data: HashMap::new(),
            config: DatabaseConfig::default(),
            resource_tracker: ResourceTracker::new(),
            database_path: None,
            storage,
        }
    }

    /// Create a new IndexManager with custom configuration
    pub fn with_config(config: DatabaseConfig) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        let storage = Arc::new(NativeStorage::new(".").unwrap());
        #[cfg(target_arch = "wasm32")]
        let storage = Arc::new(OpfsStorage::new().unwrap());

        IndexManager {
            indexes: HashMap::new(),
            index_data: HashMap::new(),
            config,
            resource_tracker: ResourceTracker::new(),
            database_path: None,
            storage,
        }
    }

    /// Set the database directory path for index file storage
    pub fn set_database_path(&mut self, path: PathBuf) {
        // Update storage backend to use the correct path
        #[cfg(not(target_arch = "wasm32"))]
        {
            if let Ok(storage) = NativeStorage::new(&path) {
                self.storage = Arc::new(storage);
            }
        }
        #[cfg(target_arch = "wasm32")]
        {
            // OPFS doesn't use directory paths the same way
            // Keep the existing OPFS storage
            let _ = path; // Suppress unused variable warning
        }
        self.database_path = Some(path);
    }

    /// Set the resource budget configuration
    pub fn set_config(&mut self, config: DatabaseConfig) {
        self.config = config;
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

        // Store index metadata (use normalized name as key)
        let metadata =
            IndexMetadata { index_name: index_name.clone(), table_name: table_name.clone(), unique, columns: columns.clone() };

        self.indexes.insert(normalized_name.clone(), metadata);

        // Choose backend based on table size
        let use_disk_backed = table_rows.len() >= DISK_BACKED_THRESHOLD;

        let (index_data, memory_bytes, disk_bytes, backend) = if use_disk_backed {
            // Create disk-backed B+ tree index using proper database path
            let index_file = self.get_index_file_path(&table_name, &index_name)?;
            let index_file_str = index_file.to_str()
                .ok_or_else(|| StorageError::IoError("Invalid index file path".to_string()))?;

            let page_manager = Arc::new(PageManager::new(index_file_str, self.storage.clone())
                .map_err(|e| StorageError::IoError(format!("Failed to create index file: {}", e)))?);

            // Build key schema from indexed columns
            let key_schema: Vec<DataType> = column_indices
                .iter()
                .map(|&idx| table_schema.columns[idx].data_type.clone())
                .collect();

            // Prepare sorted entries for bulk loading directly without intermediate BTreeMap
            // For non-unique indexes, we extend keys with row_id to make them unique
            let mut sorted_entries: Vec<(Key, usize)> = Vec::new();
            for (row_idx, row) in table_rows.iter().enumerate() {
                let key_values: Vec<SqlValue> =
                    column_indices.iter().map(|&idx| row.values[idx].clone()).collect();

                let extended_key = if unique {
                    key_values
                } else {
                    extend_key_with_row_id(key_values, row_idx)
                };
                sorted_entries.push((extended_key, row_idx));
            }
            // Sort by key for bulk_load
            sorted_entries.sort_by(|a, b| a.0.cmp(&b.0));

            // Extend key schema with Integer for non-unique indexes
            let btree_key_schema = if unique {
                key_schema
            } else {
                let mut schema = key_schema;
                schema.push(DataType::Integer);  // For row_id suffix
                schema
            };

            // Use bulk_load for efficient index creation
            let btree = BTreeIndex::bulk_load(sorted_entries, btree_key_schema, page_manager.clone())
                .map_err(|e| StorageError::IoError(format!("Failed to bulk load index: {}", e)))?;

            // Calculate disk size
            let disk_bytes = if let Ok(file_meta) = std::fs::metadata(&index_file) {
                file_meta.len() as usize
            } else {
                0
            };

            let data = IndexData::DiskBacked {
                btree: Arc::new(std::sync::Mutex::new(btree)),
                page_manager,
            };

            (data, 0, disk_bytes, crate::database::IndexBackend::DiskBacked)
        } else {
            // Build the index data in-memory for small tables
            let mut index_data_map: BTreeMap<Vec<SqlValue>, Vec<usize>> = BTreeMap::new();
            for (row_idx, row) in table_rows.iter().enumerate() {
                let key_values: Vec<SqlValue> =
                    column_indices.iter().map(|&idx| row.values[idx].clone()).collect();
                index_data_map.entry(key_values).or_default().push(row_idx);
            }

            // Estimate memory usage
            let key_size = std::mem::size_of::<Vec<SqlValue>>(); // Rough estimate
            let memory_bytes = self.estimate_index_memory(table_rows.len(), key_size);

            let data = IndexData::InMemory { data: index_data_map };

            (data, memory_bytes, 0, crate::database::IndexBackend::InMemory)
        };

        // Register the index with resource tracker
        self.resource_tracker.register_index(
            normalized_name.clone(),
            memory_bytes,
            disk_bytes,
            backend,
        );

        self.index_data.insert(normalized_name.clone(), index_data);

        // Enforce memory budget after creating index
        self.enforce_memory_budget()?;

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

        // Record access for LRU tracking (uses interior mutability)
        self.resource_tracker.record_access(&normalized);

        self.index_data.get(&normalized)
    }

    /// Check unique constraints for user-defined indexes before insert
    /// This should be called BEFORE adding the row to the table
    pub fn check_unique_constraints_for_insert(
        &self,
        table_name: &str,
        table_schema: &vibesql_catalog::TableSchema,
        row: &Row,
    ) -> Result<(), StorageError> {
        for (index_name, metadata) in &self.indexes {
            if metadata.table_name == table_name && metadata.unique {
                if let Some(index_data) = self.index_data.get(index_name) {
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

                    // Check if key already exists (skip NULLs)
                    if !key_values.contains(&SqlValue::Null) {
                        match index_data {
                            IndexData::InMemory { data } => {
                                if data.contains_key(&key_values) {
                                    let column_names: Vec<String> = metadata
                                        .columns
                                        .iter()
                                        .map(|c| c.column_name.clone())
                                        .collect();
                                    return Err(StorageError::UniqueConstraintViolation(format!(
                                        "UNIQUE constraint '{}' violated: duplicate key value for ({})",
                                        index_name,
                                        column_names.join(", ")
                                    )));
                                }
                            }
                            IndexData::DiskBacked { btree, .. } => {
                                // Lock and check if key exists in B+tree
                                if let Ok(Some(_)) = btree.lock().unwrap().lookup(&key_values) {
                                    let column_names: Vec<String> = metadata
                                        .columns
                                        .iter()
                                        .map(|c| c.column_name.clone())
                                        .collect();
                                    return Err(StorageError::UniqueConstraintViolation(format!(
                                        "UNIQUE constraint '{}' violated: duplicate key value for ({})",
                                        index_name,
                                        column_names.join(", ")
                                    )));
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Add row to user-defined indexes after insert
    /// This should be called AFTER the row has been added to the table
    pub fn add_to_indexes_for_insert(
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
                    match index_data {
                        IndexData::InMemory { data } => {
                            data.entry(key_values).or_insert_with(Vec::new).push(row_index);
                        }
                        IndexData::DiskBacked { btree, .. } => {
                            // Lock and insert into B+tree
                            // Note: BTreeIndex::insert will return an error for duplicate keys.
                            // For non-unique indexes, we should allow this, but currently
                            // the B+tree implementation doesn't support duplicate keys.
                            // This is a known limitation that will need to be addressed.
                            if let Err(e) = btree.lock().unwrap().insert(key_values, row_index) {
                                // Log error but don't fail - this may happen for non-unique indexes
                                log::warn!("Failed to insert into disk-backed index '{}': {:?}", index_name, e);
                            }
                        }
                    }
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
                        match index_data {
                            IndexData::InMemory { data } => {
                                // Remove old key
                                if let Some(row_indices) = data.get_mut(&old_key_values) {
                                    row_indices.retain(|&idx| idx != row_index);
                                    // Remove empty entries
                                    if row_indices.is_empty() {
                                        data.remove(&old_key_values);
                                    }
                                }

                                // Add new key
                                data
                                    .entry(new_key_values)
                                    .or_insert_with(Vec::new)
                                    .push(row_index);
                            }
                            IndexData::DiskBacked { btree, .. } => {
                                // Lock and update B+tree: delete old key, insert new key
                                let mut btree_guard = btree.lock().unwrap();
                                let _ = btree_guard.delete(&old_key_values);
                                if let Err(e) = btree_guard.insert(new_key_values, row_index) {
                                    log::warn!("Failed to update disk-backed index '{}': {:?}", index_name, e);
                                }
                            }
                        }
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
                    match index_data {
                        IndexData::InMemory { data } => {
                            if let Some(row_indices) = data.get_mut(&key_values) {
                                row_indices.retain(|&idx| idx != row_index);
                                // Remove empty entries
                                if row_indices.is_empty() {
                                    data.remove(&key_values);
                                }
                            }
                        }
                        IndexData::DiskBacked { btree, .. } => {
                            // Lock and delete from B+tree
                            let _ = btree.lock().unwrap().delete(&key_values);
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
                    match index_data {
                        IndexData::InMemory { data } => {
                            // Clear existing data
                            data.clear();

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

                                data.entry(key_values).or_insert_with(Vec::new).push(row_index);
                            }
                        }
                        IndexData::DiskBacked { btree, page_manager } => {
                            // For rebuild, we need to create a new B+tree from scratch
                            // First, collect all entries
                            let mut sorted_entries = Vec::new();
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
                                sorted_entries.push((key_values, row_index));
                            }

                            // Sort entries by key for bulk_load
                            sorted_entries.sort_by(|a, b| a.0.cmp(&b.0));

                            // Get key schema from metadata
                            let key_schema: Vec<vibesql_types::DataType> = metadata
                                .columns
                                .iter()
                                .map(|col| {
                                    let col_idx = table_schema
                                        .get_column_index(&col.column_name)
                                        .expect("Index column should exist");
                                    table_schema.columns[col_idx].data_type.clone()
                                })
                                .collect();

                            // Use bulk_load to create new B+tree
                            if let Ok(new_btree) = crate::btree::BTreeIndex::bulk_load(
                                sorted_entries,
                                key_schema,
                                page_manager.clone(),
                            ) {
                                // Replace old btree with new one
                                *btree.lock().unwrap() = new_btree;
                            }
                        }
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

        // Unregister from resource tracker
        self.resource_tracker.unregister_index(&normalized);

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

            // Unregister from resource tracker
            self.resource_tracker.unregister_index(index_name);
        }

        indexes_to_drop
    }

    /// List all indexes
    pub fn list_indexes(&self) -> Vec<String> {
        self.indexes.keys().cloned().collect()
    }

    // ========================================================================
    // Resource Budget and Eviction Methods
    // ========================================================================

    /// Get the file path for an index file
    fn get_index_file_path(&self, table_name: &str, index_name: &str) -> Result<PathBuf, StorageError> {
        let index_dir = self.database_path
            .as_ref()
            .map(|p| p.join("indexes"))
            .unwrap_or_else(|| std::env::temp_dir().join("vibesql_indexes"));

        // Create indexes directory if needed
        std::fs::create_dir_all(&index_dir)
            .map_err(|e| StorageError::IoError(format!("Failed to create index directory: {}", e)))?;

        // Sanitize names for filesystem
        let safe_table = table_name.replace('/', "_");
        let safe_index = index_name.replace('/', "_");
        Ok(index_dir.join(format!("{}_{}.idx", safe_table, safe_index)))
    }

    /// Estimate memory usage for an index
    fn estimate_index_memory(&self, row_count: usize, key_size: usize) -> usize {
        // Rough estimate: (key_size + Vec<usize> overhead) * row_count
        // Add BTreeMap overhead (~32 bytes per entry)
        (key_size + std::mem::size_of::<Vec<usize>>() + 32) * row_count
    }

    /// Enforce memory budget by evicting cold indexes if needed
    pub fn enforce_memory_budget(&mut self) -> Result<(), StorageError> {
        use crate::database::SpillPolicy;

        while self.resource_tracker.memory_used() > self.config.memory_budget {
            match self.config.spill_policy {
                SpillPolicy::Reject => {
                    return Err(StorageError::MemoryBudgetExceeded {
                        used: self.resource_tracker.memory_used(),
                        budget: self.config.memory_budget,
                    });
                }
                SpillPolicy::SpillToDisk => {
                    // Find coldest index and spill it
                    let coldest = self.resource_tracker
                        .find_coldest_in_memory_index()
                        .ok_or(StorageError::NoIndexToEvict)?;

                    self.spill_index_to_disk(&coldest.0)?;
                }
                SpillPolicy::BestEffort => {
                    // Try to spill, but don't fail if we can't
                    if let Some((coldest, _)) = self.resource_tracker.find_coldest_in_memory_index() {
                        let _ = self.spill_index_to_disk(&coldest);
                    } else {
                        // No more indexes to evict, give up
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// Convert an InMemory index to DiskBacked (eviction/spilling)
    fn spill_index_to_disk(&mut self, index_name: &str) -> Result<(), StorageError> {
        // Get the index data
        let index_data = self.index_data.remove(index_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;

        // Extract InMemory data, or return if already DiskBacked
        let data = match index_data {
            IndexData::InMemory { data } => data,
            IndexData::DiskBacked { .. } => {
                // Already disk-backed, just put it back
                self.index_data.insert(index_name.to_string(), index_data);
                return Ok(());
            }
        };

        // Get metadata for this index
        let metadata = self.indexes.get(index_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?
            .clone();

        // Create disk-backed version
        let index_file = self.get_index_file_path(&metadata.table_name, index_name)?;
        let index_file_str = index_file.to_str()
            .ok_or_else(|| StorageError::IoError("Invalid index file path".to_string()))?;

        let page_manager = Arc::new(PageManager::new(index_file_str, self.storage.clone())
            .map_err(|e| StorageError::IoError(format!("Failed to create index file: {}", e)))?);

        // Convert BTreeMap to sorted entries for bulk_load
        let mut sorted_entries: Vec<(Key, usize)> = Vec::new();
        for (key, row_indices) in &data {
            for &row_idx in row_indices {
                let extended_key = if metadata.unique {
                    key.clone()
                } else {
                    extend_key_with_row_id(key.clone(), row_idx)
                };
                sorted_entries.push((extended_key, row_idx));
            }
        }
        sorted_entries.sort_by(|a, b| a.0.cmp(&b.0));

        // Build key schema from metadata
        // Note: We need access to table schema to get column data types
        // For now, we'll estimate based on SqlValue types in the data
        let key_schema: Vec<DataType> = if let Some((first_key, _)) = sorted_entries.first() {
            first_key.iter().map(|v| match v {
                SqlValue::Null => DataType::Integer,  // Placeholder
                SqlValue::Integer(_) | SqlValue::Smallint(_) | SqlValue::Bigint(_) | SqlValue::Unsigned(_) => DataType::Integer,
                SqlValue::Real(_) | SqlValue::Float(_) | SqlValue::Double(_) | SqlValue::Numeric(_) => DataType::Real,
                SqlValue::Character(_) | SqlValue::Varchar(_) => DataType::Varchar { max_length: None },
                _ => DataType::Integer,  // Fallback for other types
            }).collect()
        } else {
            // Empty index, use Integer as placeholder
            vec![DataType::Integer; metadata.columns.len()]
        };

        // Bulk load into B+ tree
        let btree = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager.clone())
            .map_err(|e| StorageError::IoError(format!("Failed to bulk load index: {}", e)))?;

        // Calculate disk size (approximate)
        let disk_bytes = if let Ok(file_meta) = std::fs::metadata(&index_file) {
            file_meta.len() as usize
        } else {
            0
        };

        // Replace with disk-backed version
        let disk_backed = IndexData::DiskBacked {
            btree: Arc::new(std::sync::Mutex::new(btree)),
            page_manager,
        };

        self.index_data.insert(index_name.to_string(), disk_backed);

        // Update resource tracking
        self.resource_tracker.mark_spilled(index_name, disk_bytes);

        Ok(())
    }
}

impl Default for IndexManager {
    fn default() -> Self {
        Self::new()
    }
}

