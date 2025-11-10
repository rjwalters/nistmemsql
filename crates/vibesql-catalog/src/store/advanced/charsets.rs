//! Character set management methods.

use crate::{advanced_objects::CharacterSet, errors::CatalogError};

impl super::super::Catalog {
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
}
