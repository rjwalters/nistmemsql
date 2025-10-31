use std::collections::{HashMap, HashSet};

use crate::advanced_objects::{Assertion, CharacterSet, Collation, Sequence, Translation};
use crate::domain::DomainDefinition;
use crate::errors::CatalogError;
use crate::privilege::PrivilegeGrant;
use crate::schema::Schema;
use crate::table::TableSchema;
use crate::trigger::TriggerDefinition;
use crate::type_definition::TypeDefinition;
use crate::view::ViewDefinition;

/// Database catalog - manages all schemas and their objects.
#[derive(Debug, Clone)]
pub struct Catalog {
    schemas: HashMap<String, Schema>,
    current_schema: String,
    privilege_grants: Vec<PrivilegeGrant>,
    roles: HashSet<String>,
    // Advanced SQL:1999 objects
    domains: HashMap<String, DomainDefinition>,
    sequences: HashMap<String, Sequence>,
    type_definitions: HashMap<String, TypeDefinition>, // Comprehensive type support
    collations: HashMap<String, Collation>,
    character_sets: HashMap<String, CharacterSet>,
    translations: HashMap<String, Translation>,
    views: HashMap<String, ViewDefinition>,
    triggers: HashMap<String, TriggerDefinition>,
    assertions: HashMap<String, Assertion>, // SQL:1999 Feature F671/F672
    // Session state (SQL:1999 session configuration)
    current_catalog: Option<String>,
    current_charset: String,
    current_collation: Option<String>,
    current_timezone: String,
}

impl Catalog {
    /// Create a new empty catalog.
    pub fn new() -> Self {
        let mut catalog = Catalog {
            schemas: HashMap::new(),
            current_schema: "public".to_string(),
            privilege_grants: Vec::new(),
            roles: HashSet::new(),
            domains: HashMap::new(),
            sequences: HashMap::new(),
            type_definitions: HashMap::new(),
            collations: HashMap::new(),
            character_sets: HashMap::new(),
            translations: HashMap::new(),
            views: HashMap::new(),
            triggers: HashMap::new(),
            assertions: HashMap::new(),
            // Session defaults (SQL:1999)
            current_catalog: None,
            current_charset: "UTF8".to_string(),
            current_collation: None,
            current_timezone: "UTC".to_string(),
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
    // Session Configuration Methods (SQL:1999)
    // ============================================================================

    /// Set the current catalog name.
    pub fn set_current_catalog(&mut self, name: Option<String>) {
        self.current_catalog = name;
    }

    /// Get the current catalog name.
    pub fn get_current_catalog(&self) -> Option<&str> {
        self.current_catalog.as_deref()
    }

    /// Set the current character set.
    pub fn set_current_charset(&mut self, charset: String) {
        self.current_charset = charset;
    }

    /// Get the current character set.
    pub fn get_current_charset(&self) -> &str {
        &self.current_charset
    }

    /// Set the current collation.
    pub fn set_current_collation(&mut self, collation: Option<String>) {
        self.current_collation = collation;
    }

    /// Get the current collation.
    pub fn get_current_collation(&self) -> Option<&str> {
        self.current_collation.as_deref()
    }

    /// Set the current timezone.
    pub fn set_current_timezone(&mut self, timezone: String) {
        self.current_timezone = timezone;
    }

    /// Get the current timezone.
    pub fn get_current_timezone(&self) -> &str {
        &self.current_timezone
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
        self.privilege_grants
            .iter()
            .any(|g| g.grantee == grantee && g.object == object && g.privilege == *priv_type)
    }

    /// Get all grants for a specific grantee.
    pub fn get_grants_for_grantee(&self, grantee: &str) -> Vec<&PrivilegeGrant> {
        self.privilege_grants.iter().filter(|g| g.grantee == grantee).collect()
    }

    /// Get all grants for a specific object.
    pub fn get_grants_for_object(&self, object: &str) -> Vec<&PrivilegeGrant> {
        self.privilege_grants.iter().filter(|g| g.object == object).collect()
    }

    /// Get all privilege grants.
    pub fn get_all_grants(&self) -> &[PrivilegeGrant] {
        &self.privilege_grants
    }

    /// Remove privilege grants matching the given criteria.
    ///
    /// Returns the number of grants removed.
    pub fn remove_grants(
        &mut self,
        object: &str,
        grantee: &str,
        privilege: &ast::PrivilegeType,
        grant_option_only: bool,
    ) -> usize {
        let initial_len = self.privilege_grants.len();

        if grant_option_only {
            // Only remove the grant option, not the privilege itself
            for grant in self.privilege_grants.iter_mut() {
                if grant.object == object
                    && grant.grantee == grantee
                    && grant.privilege == *privilege
                {
                    grant.with_grant_option = false;
                }
            }
            // Return 0 since we didn't remove any grants, just modified them
            0
        } else {
            // Remove the entire grant
            self.privilege_grants.retain(|g| {
                !(g.object == object && g.grantee == grantee && g.privilege == *privilege)
            });

            initial_len - self.privilege_grants.len()
        }
    }

    /// Check if there are dependent grants (grants made by the grantee with grant option).
    ///
    /// Used for RESTRICT behavior - errors if dependent grants exist.
    pub fn has_dependent_grants(
        &self,
        object: &str,
        grantee: &str,
        privilege: &ast::PrivilegeType,
    ) -> bool {
        // Check if the grantee has granted this privilege to others
        self.privilege_grants
            .iter()
            .any(|g| g.object == object && g.grantor == grantee && g.privilege == *privilege)
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

    // ============================================================================
    // Type Definition Management Methods
    // ============================================================================

    /// Create a new user-defined type.
    pub fn create_type(&mut self, type_def: TypeDefinition) -> Result<(), CatalogError> {
        let type_name = type_def.name.clone();
        if self.type_definitions.contains_key(&type_name) {
            return Err(CatalogError::TypeAlreadyExists(type_name));
        }
        self.type_definitions.insert(type_name, type_def);
        Ok(())
    }

    /// Drop a user-defined type.
    pub fn drop_type(&mut self, name: &str, cascade: bool) -> Result<(), CatalogError> {
        if !self.type_definitions.contains_key(name) {
            return Err(CatalogError::TypeNotFound(name.to_string()));
        }

        // Check for dependencies if not CASCADE
        if !cascade {
            // Check if any tables use this type
            for schema in self.schemas.values() {
                for table_name in schema.list_tables() {
                    if let Some(table) = schema.get_table(&table_name) {
                        for column in &table.columns {
                            if let types::DataType::UserDefined { type_name } = &column.data_type {
                                if type_name == name {
                                    return Err(CatalogError::TypeInUse(name.to_string()));
                                }
                            }
                        }
                    }
                }
            }
        }

        self.type_definitions.remove(name);

        // If CASCADE, also drop dependent objects (tables with columns of this type)
        if cascade {
            let mut tables_to_drop = Vec::new();
            for (schema_name, schema) in &self.schemas {
                for table_name in schema.list_tables() {
                    if let Some(table) = schema.get_table(&table_name) {
                        for column in &table.columns {
                            if let types::DataType::UserDefined { type_name } = &column.data_type {
                                if type_name == name {
                                    tables_to_drop.push(format!("{}.{}", schema_name, table_name));
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            // Drop the dependent tables
            for qualified_table_name in tables_to_drop {
                let _ = self.drop_table(&qualified_table_name);
            }
        }

        Ok(())
    }

    /// Get a type definition by name.
    pub fn get_type(&self, name: &str) -> Option<&TypeDefinition> {
        self.type_definitions.get(name)
    }

    /// Check if a type exists.
    pub fn type_exists(&self, name: &str) -> bool {
        self.type_definitions.contains_key(name)
    }

    /// List all user-defined type names.
    pub fn list_types(&self) -> Vec<String> {
        self.type_definitions.keys().cloned().collect()
    }

    // ========================================================================
    // Advanced SQL:1999 Object Management (stubs)
    // ========================================================================

    // ============================================================================
    // Domain Management Methods (Full Implementation)
    // ============================================================================

    /// Create a new domain.
    pub fn create_domain(&mut self, domain: DomainDefinition) -> Result<(), CatalogError> {
        let name = domain.name.clone();
        if self.domains.contains_key(&name) {
            return Err(CatalogError::DomainAlreadyExists(name));
        }
        self.domains.insert(name, domain);
        Ok(())
    }

    /// Get a domain definition by name.
    pub fn get_domain(&self, name: &str) -> Option<&DomainDefinition> {
        self.domains.get(name)
    }

    /// Drop a domain.
    pub fn drop_domain(&mut self, name: &str, cascade: bool) -> Result<(), CatalogError> {
        if !self.domains.contains_key(name) {
            return Err(CatalogError::DomainNotFound(name.to_string()));
        }

        // Check if any tables use this domain
        if !cascade {
            // For RESTRICT, we need to check if any columns use this domain
            // We'll check all tables in all schemas
            for schema in self.schemas.values() {
                for table in schema.list_tables() {
                    if let Some(table_schema) = schema.get_table(&table) {
                        // For now, we'll skip this check since we haven't implemented
                        // domain usage in column definitions yet.
                        // TODO: Check if any columns use this domain when we implement
                        // domain support in CREATE TABLE
                        let _ = table_schema;
                    }
                }
            }
        }

        self.domains.remove(name);
        Ok(())
    }

    /// Check if a domain exists.
    pub fn domain_exists(&self, name: &str) -> bool {
        self.domains.contains_key(&name.to_uppercase())
    }

    /// List all domain names.
    pub fn list_domains(&self) -> Vec<String> {
        self.domains.keys().cloned().collect()
    }

    // ============================================================================
    // Other Advanced SQL Objects (Stub Implementations)
    // ============================================================================

    /// Create a SEQUENCE
    pub fn create_sequence(
        &mut self,
        name: String,
        start_with: Option<i64>,
        increment_by: i64,
        min_value: Option<i64>,
        max_value: Option<i64>,
        cycle: bool,
    ) -> Result<(), CatalogError> {
        if self.sequences.contains_key(&name) {
            return Err(CatalogError::SequenceAlreadyExists(name));
        }
        self.sequences.insert(
            name.clone(),
            Sequence::new(name, start_with, increment_by, min_value, max_value, cycle),
        );
        Ok(())
    }

    /// Drop a SEQUENCE
    pub fn drop_sequence(&mut self, name: &str) -> Result<(), CatalogError> {
        self.sequences
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| CatalogError::SequenceNotFound(name.to_string()))
    }

    /// Alter a SEQUENCE
    pub fn alter_sequence(
        &mut self,
        name: &str,
        restart_with: Option<i64>,
        increment_by: Option<i64>,
        min_value: Option<Option<i64>>,
        max_value: Option<Option<i64>>,
        cycle: Option<bool>,
    ) -> Result<(), CatalogError> {
        let seq = self
            .sequences
            .get_mut(name)
            .ok_or_else(|| CatalogError::SequenceNotFound(name.to_string()))?;

        if let Some(restart) = restart_with {
            seq.restart(Some(restart));
        }
        if let Some(incr) = increment_by {
            seq.increment_by = incr;
        }
        if let Some(min) = min_value {
            seq.min_value = min;
        }
        if let Some(max) = max_value {
            seq.max_value = max;
        }
        if let Some(cyc) = cycle {
            seq.cycle = cyc;
        }

        Ok(())
    }

    /// Get a mutable reference to a SEQUENCE for NEXT VALUE FOR
    pub fn get_sequence_mut(&mut self, name: &str) -> Result<&mut Sequence, CatalogError> {
        self.sequences.get_mut(name).ok_or_else(|| CatalogError::SequenceNotFound(name.to_string()))
    }

    /// Create a COLLATION
    pub fn create_collation(
        &mut self,
        name: String,
        character_set: Option<String>,
        source_collation: Option<String>,
        pad_space: Option<bool>,
    ) -> Result<(), CatalogError> {
        if self.collations.contains_key(&name) {
            return Err(CatalogError::CollationAlreadyExists(name));
        }
        self.collations.insert(
            name.clone(),
            Collation::new(name, character_set, source_collation, pad_space),
        );
        Ok(())
    }

    /// Drop a COLLATION
    pub fn drop_collation(&mut self, name: &str) -> Result<(), CatalogError> {
        self.collations
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| CatalogError::CollationNotFound(name.to_string()))
    }

    /// Create a CHARACTER SET
    pub fn create_character_set(
        &mut self,
        name: String,
        source: Option<String>,
        collation: Option<String>,
    ) -> Result<(), CatalogError> {
        if self.character_sets.contains_key(&name) {
            return Err(CatalogError::CharacterSetAlreadyExists(name));
        }
        self.character_sets.insert(name.clone(), CharacterSet::new(name, source, collation));
        Ok(())
    }

    /// Drop a CHARACTER SET
    pub fn drop_character_set(&mut self, name: &str) -> Result<(), CatalogError> {
        self.character_sets
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| CatalogError::CharacterSetNotFound(name.to_string()))
    }

    /// Create a TRANSLATION
    pub fn create_translation(
        &mut self,
        name: String,
        source_charset: Option<String>,
        target_charset: Option<String>,
        translation_source: Option<String>,
    ) -> Result<(), CatalogError> {
        if self.translations.contains_key(&name) {
            return Err(CatalogError::TranslationAlreadyExists(name));
        }
        self.translations.insert(
            name.clone(),
            Translation::new(name, source_charset, target_charset, translation_source),
        );
        Ok(())
    }

    /// Drop a TRANSLATION
    pub fn drop_translation(&mut self, name: &str) -> Result<(), CatalogError> {
        self.translations
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| CatalogError::TranslationNotFound(name.to_string()))
    }

    /// Create a VIEW
    pub fn create_view(&mut self, view: ViewDefinition) -> Result<(), CatalogError> {
        let name = view.name.clone();
        if self.views.contains_key(&name) {
            return Err(CatalogError::ViewAlreadyExists(name));
        }
        self.views.insert(name, view);
        Ok(())
    }

    /// Get a VIEW definition by name (supports qualified names like "schema.view")
    pub fn get_view(&self, name: &str) -> Option<&ViewDefinition> {
        self.views.get(name)
    }

    /// Drop a VIEW
    pub fn drop_view(&mut self, name: &str) -> Result<(), CatalogError> {
        self.views
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| CatalogError::ViewNotFound(name.to_string()))
    }

    /// Create a TRIGGER
    pub fn create_trigger(&mut self, trigger: TriggerDefinition) -> Result<(), CatalogError> {
        let name = trigger.name.clone();
        if self.triggers.contains_key(&name) {
            return Err(CatalogError::TriggerAlreadyExists(name));
        }
        self.triggers.insert(name, trigger);
        Ok(())
    }

    /// Get a TRIGGER definition by name
    pub fn get_trigger(&self, name: &str) -> Option<&TriggerDefinition> {
        self.triggers.get(name)
    }

    /// Drop a TRIGGER
    pub fn drop_trigger(&mut self, name: &str) -> Result<(), CatalogError> {
        self.triggers
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| CatalogError::TriggerNotFound(name.to_string()))
    }

    /// Create an ASSERTION (SQL:1999 Feature F671/F672)
    pub fn create_assertion(&mut self, assertion: Assertion) -> Result<(), CatalogError> {
        let name = assertion.name.clone();
        if self.assertions.contains_key(&name) {
            return Err(CatalogError::AssertionAlreadyExists(name));
        }
        self.assertions.insert(name, assertion);
        Ok(())
    }

    /// Get an ASSERTION definition by name
    pub fn get_assertion(&self, name: &str) -> Option<&Assertion> {
        self.assertions.get(name)
    }

    /// Drop an ASSERTION
    pub fn drop_assertion(&mut self, name: &str, cascade: bool) -> Result<(), CatalogError> {
        // For now, assertions don't have dependencies, so cascade is ignored
        // In the future, cascade might be used if assertions can reference other assertions
        let _ = cascade;

        self.assertions
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| CatalogError::AssertionNotFound(name.to_string()))
    }

    /// Check if an assertion exists
    pub fn assertion_exists(&self, name: &str) -> bool {
        self.assertions.contains_key(name)
    }

    /// List all assertion names
    pub fn list_assertions(&self) -> Vec<String> {
        self.assertions.keys().cloned().collect()
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}
