use std::collections::{HashMap, HashSet};

use crate::errors::CatalogError;
use crate::privilege::PrivilegeGrant;
use crate::schema::Schema;
use crate::table::TableSchema;

/// Database catalog - manages all schemas and their objects.
#[derive(Debug, Clone)]
pub struct Catalog {
    schemas: HashMap<String, Schema>,
    current_schema: String,
    privilege_grants: Vec<PrivilegeGrant>,
    roles: HashSet<String>,
}

impl Catalog {
    /// Create a new empty catalog.
    pub fn new() -> Self {
        let mut catalog = Catalog {
            schemas: HashMap::new(),
            current_schema: "public".to_string(),
            privilege_grants: Vec::new(),
            roles: HashSet::new(),
        };

        // Create the default "public" schema
        catalog.schemas.insert("public".to_string(), Schema::new("public".to_string()));

        catalog
    }

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
            self.schemas.get(schema_name).and_then(|schema| schema.get_table(table_name))
        } else {
            // Use current schema for unqualified names
            self.schemas.get(&self.current_schema).and_then(|schema| schema.get_table(name))
        }
    }

    /// Drop a table schema (supports qualified names like "schema.table").
    pub fn drop_table(&mut self, name: &str) -> Result<(), CatalogError> {
        // Parse qualified name: schema.table or just table
        let (schema_name, table_name) =
            if let Some((schema_part, table_part)) = name.split_once('.') {
                (schema_part.to_string(), table_part)
            } else {
                (self.current_schema.clone(), name)
            };

        let schema = self
            .schemas
            .get_mut(&schema_name)
            .ok_or(CatalogError::SchemaNotFound(schema_name.clone()))?;

        schema.drop_table(table_name)
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

    // ============================================================================
    // Schema Management Methods
    // ============================================================================

    /// Create a new schema.
    pub fn create_schema(&mut self, name: String) -> Result<(), CatalogError> {
        if self.schemas.contains_key(&name) {
            return Err(CatalogError::SchemaAlreadyExists(name));
        }
        self.schemas.insert(name.clone(), Schema::new(name));
        Ok(())
    }

    /// Drop a schema.
    pub fn drop_schema(&mut self, name: &str, cascade: bool) -> Result<(), CatalogError> {
        // Don't allow dropping the public schema
        if name == "public" {
            return Err(CatalogError::SchemaNotEmpty("public".to_string()));
        }

        let schema =
            self.schemas.get(name).ok_or_else(|| CatalogError::SchemaNotFound(name.to_string()))?;

        if !cascade && !schema.is_empty() {
            return Err(CatalogError::SchemaNotEmpty(name.to_string()));
        }

        self.schemas.remove(name);
        Ok(())
    }

    /// Get a schema by name.
    pub fn get_schema(&self, name: &str) -> Option<&Schema> {
        self.schemas.get(name)
    }

    /// List all schema names.
    pub fn list_schemas(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    /// Check if schema exists.
    pub fn schema_exists(&self, name: &str) -> bool {
        self.schemas.contains_key(name)
    }

    /// Set the current schema for unqualified table references.
    pub fn set_current_schema(&mut self, name: &str) -> Result<(), CatalogError> {
        if !self.schema_exists(name) {
            return Err(CatalogError::SchemaNotFound(name.to_string()));
        }
        self.current_schema = name.to_string();
        Ok(())
    }

    /// Get the current schema name.
    pub fn get_current_schema(&self) -> &str {
        &self.current_schema
    }

    // ============================================================================
    // Privilege Management Methods
    // ============================================================================

    /// Add a privilege grant to the catalog.
    pub fn add_grant(&mut self, grant: PrivilegeGrant) {
        self.privilege_grants.push(grant);
    }

    /// Check if a grantee has a specific privilege on an object.
    pub fn has_privilege(
        &self,
        grantee: &str,
        object: &str,
        priv_type: &ast::PrivilegeType,
    ) -> bool {
        self.privilege_grants.iter().any(|g| {
            g.grantee == grantee && g.object == object && g.privilege == *priv_type
        })
    }

    /// Get all grants for a specific grantee.
    pub fn get_grants_for_grantee(&self, grantee: &str) -> Vec<&PrivilegeGrant> {
        self.privilege_grants.iter().filter(|g| g.grantee == grantee).collect()
    }

    /// Get all grants for a specific object.
    pub fn get_grants_for_object(&self, object: &str) -> Vec<&PrivilegeGrant> {
        self.privilege_grants.iter().filter(|g| g.object == object).collect()
    }

    // ============================================================================
    // Role Management Methods
    // ============================================================================

    /// Create a new role.
    pub fn create_role(&mut self, name: String) -> Result<(), CatalogError> {
        if self.roles.contains(&name) {
            return Err(CatalogError::RoleAlreadyExists(name));
        }
        self.roles.insert(name);
        Ok(())
    }

    /// Drop a role.
    pub fn drop_role(&mut self, name: &str) -> Result<(), CatalogError> {
        if !self.roles.contains(name) {
            return Err(CatalogError::RoleNotFound(name.to_string()));
        }
        self.roles.remove(name);
        Ok(())
    }

    /// Check if a role exists.
    pub fn role_exists(&self, name: &str) -> bool {
        self.roles.contains(name)
    }

    /// List all roles.
    pub fn list_roles(&self) -> Vec<String> {
        self.roles.iter().cloned().collect()
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}
