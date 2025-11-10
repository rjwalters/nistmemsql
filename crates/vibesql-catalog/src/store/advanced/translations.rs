//! Translation management methods.

use crate::{advanced_objects::Translation, errors::CatalogError};

impl super::super::Catalog {
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
}
