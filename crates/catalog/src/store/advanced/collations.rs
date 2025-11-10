//! Collation management methods.

use crate::{advanced_objects::Collation, errors::CatalogError};

impl super::super::Catalog {
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
        self.collations
            .insert(name.clone(), Collation::new(name, character_set, source_collation, pad_space));
        Ok(())
    }

    /// Drop a COLLATION
    pub fn drop_collation(&mut self, name: &str) -> Result<(), CatalogError> {
        self.collations
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| CatalogError::CollationNotFound(name.to_string()))
    }
}
