//! Session configuration operations (SQL:1999).
//!
//! This module handles session-level configuration including catalog,
//! character set, collation, and timezone settings.

impl super::Catalog {
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
}
