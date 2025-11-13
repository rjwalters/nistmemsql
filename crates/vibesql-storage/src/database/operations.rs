// ============================================================================
// Table and Index Operations
// ============================================================================

use super::indexes::IndexManager;
use crate::index::{extract_mbr_from_sql_value, SpatialIndex};
use crate::{Row, StorageError, Table};
use std::collections::HashMap;
use vibesql_ast::IndexColumn;

/// Metadata for a spatial index
#[derive(Debug, Clone)]
pub struct SpatialIndexMetadata {
    pub index_name: String,
    pub table_name: String,
    pub column_name: String,
    pub created_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Manages table and index operations
#[derive(Debug, Clone)]
pub struct Operations {
    /// User-defined index manager (B-tree indexes)
    index_manager: IndexManager,
    /// Spatial indexes (R-tree) - stored separately from B-tree indexes
    /// Key: normalized index name (uppercase)
    /// Value: (metadata, spatial index)
    spatial_indexes: HashMap<String, (SpatialIndexMetadata, SpatialIndex)>,
}

impl Operations {
    /// Create a new operations manager
    pub fn new() -> Self {
        Operations {
            index_manager: IndexManager::new(),
            spatial_indexes: HashMap::new(),
        }
    }

    /// Set the database path for index storage
    pub fn set_database_path(&mut self, path: std::path::PathBuf) {
        self.index_manager.set_database_path(path);
    }

    /// Set the database configuration (memory budgets, spill policy)
    pub fn set_config(&mut self, config: super::DatabaseConfig) {
        self.index_manager.set_config(config);
    }

    // ============================================================================
    // Table Operations
    // ============================================================================

    /// Create a table in the catalog and storage
    pub fn create_table(
        &mut self,
        catalog: &mut vibesql_catalog::Catalog,
        schema: vibesql_catalog::TableSchema,
    ) -> Result<(), StorageError> {
        let _table_name = schema.name.clone();

        // Add to catalog
        catalog
            .create_table(schema.clone())
            .map_err(|e| StorageError::CatalogError(e.to_string()))?;

        Ok(())
    }

    /// Drop a table from the catalog
    pub fn drop_table(
        &mut self,
        catalog: &mut vibesql_catalog::Catalog,
        tables: &mut HashMap<String, Table>,
        name: &str,
    ) -> Result<(), StorageError> {
        // Normalize table name for lookup (matches catalog normalization)
        let normalized_name = if catalog.is_case_sensitive_identifiers() {
            name.to_string()
        } else {
            name.to_uppercase()
        };

        // Get qualified table name for index cleanup
        let qualified_name = if normalized_name.contains('.') {
            normalized_name.clone()
        } else {
            let current_schema = catalog.get_current_schema();
            format!("{}.{}", current_schema, normalized_name)
        };

        // Drop associated indexes BEFORE dropping table (CASCADE behavior)
        self.index_manager.drop_indexes_for_table(&qualified_name);

        // Drop associated spatial indexes too
        self.drop_spatial_indexes_for_table(&qualified_name);

        // Remove from catalog
        catalog.drop_table(name).map_err(|e| StorageError::CatalogError(e.to_string()))?;

        // Remove table data - try normalized name first, then try with schema prefix
        if tables.remove(&normalized_name).is_none() {
            tables.remove(&qualified_name);
        }

        Ok(())
    }

    /// Insert a row into a table
    pub fn insert_row(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        tables: &mut HashMap<String, Table>,
        table_name: &str,
        row: Row,
    ) -> Result<usize, StorageError> {
        // Normalize table name for lookup (matches catalog normalization)
        let normalized_name = if catalog.is_case_sensitive_identifiers() {
            table_name.to_string()
        } else {
            table_name.to_uppercase()
        };

        // First try direct lookup, then try with schema prefix if needed
        let table = if let Some(tbl) = tables.get_mut(&normalized_name) {
            tbl
        } else if !table_name.contains('.') {
            // Try with schema prefix
            let current_schema = catalog.get_current_schema();
            let qualified_name = format!("{}.{}", current_schema, normalized_name);
            tables
                .get_mut(&qualified_name)
                .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?
        } else {
            return Err(StorageError::TableNotFound(table_name.to_string()));
        };

        let row_index = table.row_count();

        // Check user-defined unique indexes BEFORE inserting
        if let Some(table_schema) = catalog.get_table(table_name) {
            self.index_manager.check_unique_constraints_for_insert(table_name, table_schema, &row)?;
        }

        // Insert the row (this validates table-level constraints like PK, UNIQUE)
        table.insert(row.clone())?;

        // Update user-defined indexes
        if let Some(table_schema) = catalog.get_table(table_name) {
            self.index_manager.add_to_indexes_for_insert(table_name, table_schema, &row, row_index);
        }

        // Update spatial indexes
        self.update_spatial_indexes_for_insert(catalog, table_name, &row, row_index);

        Ok(row_index)
    }

    // ============================================================================
    // Index Management - Delegates to IndexManager
    // ============================================================================

    /// Create an index
    pub fn create_index(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        tables: &HashMap<String, Table>,
        index_name: String,
        table_name: String,
        unique: bool,
        columns: Vec<IndexColumn>,
    ) -> Result<(), StorageError> {
        // Normalize table name for lookup (matches catalog normalization)
        let normalized_name = if catalog.is_case_sensitive_identifiers() {
            table_name.clone()
        } else {
            table_name.to_uppercase()
        };

        // Try to find the table with normalized name or qualified name
        let table = if let Some(tbl) = tables.get(&normalized_name) {
            tbl
        } else if !table_name.contains('.') {
            // Try with schema prefix
            let current_schema = catalog.get_current_schema();
            let qualified_name = format!("{}.{}", current_schema, normalized_name);
            tables
                .get(&qualified_name)
                .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?
        } else {
            return Err(StorageError::TableNotFound(table_name.clone()));
        };

        let table_schema = catalog
            .get_table(&table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

        let table_rows: Vec<Row> = table.scan().to_vec();

        self.index_manager.create_index(
            index_name,
            table_name,
            table_schema,
            &table_rows,
            unique,
            columns,
        )
    }

    /// Check if an index exists
    pub fn index_exists(&self, index_name: &str) -> bool {
        self.index_manager.index_exists(index_name)
    }

    /// Get index metadata
    pub fn get_index(&self, index_name: &str) -> Option<&super::indexes::IndexMetadata> {
        self.index_manager.get_index(index_name)
    }

    /// Get index data
    pub fn get_index_data(&self, index_name: &str) -> Option<&super::indexes::IndexData> {
        self.index_manager.get_index_data(index_name)
    }

    /// Update user-defined indexes for update operation
    pub fn update_indexes_for_update(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        table_name: &str,
        old_row: &Row,
        new_row: &Row,
        row_index: usize,
    ) {
        if let Some(table_schema) = catalog.get_table(table_name) {
            self.index_manager.update_indexes_for_update(
                table_name,
                table_schema,
                old_row,
                new_row,
                row_index,
            );
        }

        self.update_spatial_indexes_for_update(catalog, table_name, old_row, new_row, row_index);
    }

    /// Update user-defined indexes for delete operation
    pub fn update_indexes_for_delete(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        table_name: &str,
        row: &Row,
        row_index: usize,
    ) {
        if let Some(table_schema) = catalog.get_table(table_name) {
            self.index_manager.update_indexes_for_delete(table_name, table_schema, row, row_index);
        }

        self.update_spatial_indexes_for_delete(catalog, table_name, row, row_index);
    }

    /// Rebuild user-defined indexes after bulk operations that change row indices
    pub fn rebuild_indexes(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        tables: &HashMap<String, Table>,
        table_name: &str,
    ) {
        let table_rows: Vec<Row> = if let Some(table) = tables.get(table_name) {
            table.scan().to_vec()
        } else {
            return;
        };

        let table_schema = match catalog.get_table(table_name) {
            Some(schema) => schema,
            None => return,
        };

        self.index_manager.rebuild_indexes(table_name, table_schema, &table_rows);
    }

    /// Drop an index
    pub fn drop_index(&mut self, index_name: &str) -> Result<(), StorageError> {
        self.index_manager.drop_index(index_name)
    }

    /// List all indexes
    pub fn list_indexes(&self) -> Vec<String> {
        self.index_manager.list_indexes()
    }

    /// List all indexes for a specific table
    pub fn list_indexes_for_table(&self, table_name: &str) -> Vec<String> {
        // Normalize for case-insensitive comparison
        let normalized_search = table_name.to_uppercase();

        self.index_manager
            .list_indexes()
            .into_iter()
            .filter(|index_name| {
                self.index_manager
                    .get_index(index_name)
                    .map(|metadata| {
                        // Normalize both sides for comparison
                        metadata.table_name.to_uppercase() == normalized_search
                    })
                    .unwrap_or(false)
            })
            .collect()
    }

    // ========================================================================
    // Spatial Index Methods
    // ========================================================================

    /// Normalize an index name to uppercase for case-insensitive comparison
    fn normalize_index_name(name: &str) -> String {
        name.to_uppercase()
    }

    /// Create a spatial index
    pub fn create_spatial_index(
        &mut self,
        metadata: SpatialIndexMetadata,
        spatial_index: SpatialIndex,
    ) -> Result<(), StorageError> {
        let normalized_name = Self::normalize_index_name(&metadata.index_name);

        if self.index_manager.index_exists(&metadata.index_name) {
            return Err(StorageError::IndexAlreadyExists(metadata.index_name.clone()));
        }
        if self.spatial_indexes.contains_key(&normalized_name) {
            return Err(StorageError::IndexAlreadyExists(metadata.index_name.clone()));
        }

        self.spatial_indexes.insert(normalized_name, (metadata, spatial_index));
        Ok(())
    }

    /// Check if a spatial index exists
    pub fn spatial_index_exists(&self, index_name: &str) -> bool {
        let normalized = Self::normalize_index_name(index_name);
        self.spatial_indexes.contains_key(&normalized)
    }

    /// Get spatial index metadata
    pub fn get_spatial_index_metadata(&self, index_name: &str) -> Option<&SpatialIndexMetadata> {
        let normalized = Self::normalize_index_name(index_name);
        self.spatial_indexes.get(&normalized).map(|(metadata, _)| metadata)
    }

    /// Get spatial index (immutable)
    pub fn get_spatial_index(&self, index_name: &str) -> Option<&SpatialIndex> {
        let normalized = Self::normalize_index_name(index_name);
        self.spatial_indexes.get(&normalized).map(|(_, index)| index)
    }

    /// Get spatial index (mutable)
    pub fn get_spatial_index_mut(&mut self, index_name: &str) -> Option<&mut SpatialIndex> {
        let normalized = Self::normalize_index_name(index_name);
        self.spatial_indexes.get_mut(&normalized).map(|(_, index)| index)
    }

    /// Get all spatial indexes for a specific table
    pub fn get_spatial_indexes_for_table(
        &self,
        table_name: &str,
    ) -> Vec<(&SpatialIndexMetadata, &SpatialIndex)> {
        self.spatial_indexes
            .values()
            .filter(|(metadata, _)| metadata.table_name == table_name)
            .map(|(metadata, index)| (metadata, index))
            .collect()
    }

    /// Get all spatial indexes for a specific table (mutable)
    pub fn get_spatial_indexes_for_table_mut(
        &mut self,
        table_name: &str,
    ) -> Vec<(&SpatialIndexMetadata, &mut SpatialIndex)> {
        self.spatial_indexes
            .iter_mut()
            .filter(|(_, (metadata, _))| metadata.table_name == table_name)
            .map(|(_, (metadata, index))| (metadata as &SpatialIndexMetadata, index))
            .collect()
    }

    /// Drop a spatial index
    pub fn drop_spatial_index(&mut self, index_name: &str) -> Result<(), StorageError> {
        let normalized = Self::normalize_index_name(index_name);

        if self.spatial_indexes.remove(&normalized).is_none() {
            return Err(StorageError::IndexNotFound(index_name.to_string()));
        }

        Ok(())
    }

    /// Drop all spatial indexes associated with a table (CASCADE behavior)
    pub fn drop_spatial_indexes_for_table(&mut self, table_name: &str) -> Vec<String> {
        let indexes_to_drop: Vec<String> = self
            .spatial_indexes
            .iter()
            .filter(|(_, (metadata, _))| metadata.table_name == table_name)
            .map(|(name, _)| name.clone())
            .collect();

        for index_name in &indexes_to_drop {
            self.spatial_indexes.remove(index_name);
        }

        indexes_to_drop
    }

    /// List all spatial indexes
    pub fn list_spatial_indexes(&self) -> Vec<String> {
        self.spatial_indexes.keys().cloned().collect()
    }

    /// Update spatial indexes for insert operation
    fn update_spatial_indexes_for_insert(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        table_name: &str,
        row: &Row,
        row_index: usize,
    ) {
        let table_schema = match catalog.get_table(table_name) {
            Some(schema) => schema,
            None => return,
        };

        let indexes_to_update: Vec<(String, usize)> = self
            .spatial_indexes
            .iter()
            .filter(|(_, (metadata, _))| metadata.table_name == table_name)
            .filter_map(|(index_name, (metadata, _))| {
                table_schema
                    .get_column_index(&metadata.column_name)
                    .map(|col_idx| (index_name.clone(), col_idx))
            })
            .collect();

        for (index_name, col_idx) in indexes_to_update {
            let geom_value = &row.values[col_idx];

            if let Some(mbr) = extract_mbr_from_sql_value(geom_value) {
                if let Some((_, index)) = self.spatial_indexes.get_mut(&index_name) {
                    index.insert(row_index, mbr);
                }
            }
        }
    }

    /// Update spatial indexes for update operation
    fn update_spatial_indexes_for_update(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        table_name: &str,
        old_row: &Row,
        new_row: &Row,
        row_index: usize,
    ) {
        let table_schema = match catalog.get_table(table_name) {
            Some(schema) => schema,
            None => return,
        };

        let indexes_to_update: Vec<(String, usize)> = self
            .spatial_indexes
            .iter()
            .filter(|(_, (metadata, _))| metadata.table_name == table_name)
            .filter_map(|(index_name, (metadata, _))| {
                table_schema
                    .get_column_index(&metadata.column_name)
                    .map(|col_idx| (index_name.clone(), col_idx))
            })
            .collect();

        for (index_name, col_idx) in indexes_to_update {
            let old_geom = &old_row.values[col_idx];
            let new_geom = &new_row.values[col_idx];

            if old_geom != new_geom {
                if let Some((_, index)) = self.spatial_indexes.get_mut(&index_name) {
                    if let Some(old_mbr) = extract_mbr_from_sql_value(old_geom) {
                        index.remove(row_index, &old_mbr);
                    }

                    if let Some(new_mbr) = extract_mbr_from_sql_value(new_geom) {
                        index.insert(row_index, new_mbr);
                    }
                }
            }
        }
    }

    /// Update spatial indexes for delete operation
    fn update_spatial_indexes_for_delete(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        table_name: &str,
        row: &Row,
        row_index: usize,
    ) {
        let table_schema = match catalog.get_table(table_name) {
            Some(schema) => schema,
            None => return,
        };

        let indexes_to_update: Vec<(String, usize)> = self
            .spatial_indexes
            .iter()
            .filter(|(_, (metadata, _))| metadata.table_name == table_name)
            .filter_map(|(index_name, (metadata, _))| {
                table_schema
                    .get_column_index(&metadata.column_name)
                    .map(|col_idx| (index_name.clone(), col_idx))
            })
            .collect();

        for (index_name, col_idx) in indexes_to_update {
            let geom_value = &row.values[col_idx];

            if let Some(mbr) = extract_mbr_from_sql_value(geom_value) {
                if let Some((_, index)) = self.spatial_indexes.get_mut(&index_name) {
                    index.remove(row_index, &mbr);
                }
            }
        }
    }
}

impl Default for Operations {
    fn default() -> Self {
        Self::new()
    }
}
