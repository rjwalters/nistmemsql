//! Advanced SQL:1999 object management.
//!
//! This module handles user-defined types, domains, sequences, collations,
//! character sets, translations, views, and triggers.

use crate::advanced_objects::{CharacterSet, Collation, Sequence, Translation};
use crate::domain::DomainDefinition;
use crate::errors::CatalogError;
use crate::trigger::TriggerDefinition;
use crate::type_definition::TypeDefinition;
use crate::view::ViewDefinition;

impl super::Catalog {
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

    // ============================================================================
    // Domain Management Methods
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
    // Sequence Management Methods
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

    // ============================================================================
    // Collation Management Methods
    // ============================================================================

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

    // ============================================================================
    // Character Set Management Methods
    // ============================================================================

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

    // ============================================================================
    // Translation Management Methods
    // ============================================================================

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

    // ============================================================================
    // View Management Methods
    // ============================================================================

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

    // ============================================================================
    // Trigger Management Methods
    // ============================================================================

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
}
