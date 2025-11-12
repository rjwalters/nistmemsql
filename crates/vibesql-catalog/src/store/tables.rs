//! Table management operations for the catalog.
//!
//! This module handles all table-related operations including creation,
//! modification, deletion, and queries.

use crate::{errors::CatalogError, table::TableSchema};

impl super::Catalog {
    /// Create a table schema in the current schema.
    pub fn create_table(&mut self, schema: TableSchema) -> Result<(), CatalogError> {
        let current_schema = self
            .schemas
            .get_mut(&self.current_schema)
            .ok_or_else(|| CatalogError::SchemaNotFound(self.current_schema.clone()))?;

        current_schema.create_table(schema)
    }

    /// Create a table schema in a specific schema.
    pub fn create_table_in_schema(
        &mut self,
        schema_name: &str,
        schema: TableSchema,
    ) -> Result<(), CatalogError> {
        let target_schema = self
            .schemas
            .get_mut(schema_name)
            .ok_or_else(|| CatalogError::SchemaNotFound(schema_name.to_string()))?;

        target_schema.create_table(schema)
    }

    /// Get a table schema by name (supports qualified names like "schema.table").
    pub fn get_table(&self, name: &str) -> Option<&TableSchema> {
        // Parse qualified name: schema.table or just table
        if let Some((schema_name, table_name)) = name.split_once('.') {
            let normalized_schema = self.normalize_identifier(schema_name);
            let normalized_table = self.normalize_identifier(table_name);
            self.schemas
                .get(&normalized_schema)
                .and_then(|schema| schema.get_table(&normalized_table, self.case_sensitive_identifiers))
        } else {
            // Use current schema for unqualified names
            let normalized_table = self.normalize_identifier(name);
            self.schemas
                .get(&self.current_schema)
                .and_then(|schema| schema.get_table(&normalized_table, self.case_sensitive_identifiers))
        }
    }

    /// Drop a table schema (supports qualified names like "schema.table").
    /// Respects the `case_sensitive_identifiers` setting.
    pub fn drop_table(&mut self, name: &str) -> Result<(), CatalogError> {
        // Parse qualified name: schema.table or just table
        let (schema_name, table_name, original_table_name) =
            if let Some((schema_part, table_part)) = name.split_once('.') {
                let normalized_schema = self.normalize_identifier(schema_part);
                (normalized_schema, table_part, table_part)
            } else {
                (self.current_schema.clone(), name, name)
            };

        let normalized_table = self.normalize_identifier(table_name);

        let schema = self
            .schemas
            .get_mut(&schema_name)
            .ok_or(CatalogError::SchemaNotFound(schema_name.clone()))?;

        // For error messages, we want to use the original input name, not the normalized one
        schema.drop_table(&normalized_table, self.case_sensitive_identifiers)
            .map_err(|e| match e {
                CatalogError::TableNotFound(_) => CatalogError::TableNotFound(original_table_name.to_string()),
                other => other,
            })
    }

    /// List all table names in the current schema.
    pub fn list_tables(&self) -> Vec<String> {
        self.schemas
            .get(&self.current_schema)
            .map(|schema| schema.list_tables())
            .unwrap_or_default()
    }

    /// List all table names with qualified names (schema.table).
    pub fn list_all_tables(&self) -> Vec<String> {
        let mut result = Vec::new();
        for (schema_name, schema) in &self.schemas {
            for table_name in schema.list_tables() {
                result.push(format!("{}.{}", schema_name, table_name));
            }
        }
        result
    }

    /// Check if table exists (supports qualified names).
    pub fn table_exists(&self, name: &str) -> bool {
        self.get_table(name).is_some()
    }
}
