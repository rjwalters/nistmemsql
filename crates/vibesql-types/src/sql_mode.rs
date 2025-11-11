//! SQL dialect mode configuration
//!
//! Supports multiple SQL dialects to enable compatibility with different test suites
//! and database behaviors.

/// SQL dialect mode
///
/// Controls type coercion behavior for arithmetic operations and other dialect-specific features.
///
/// # Examples
///
/// ```
/// use vibesql_types::SqlMode;
///
/// // Default mode is SQL:1999 standard
/// let mode = SqlMode::default();
/// assert_eq!(mode, SqlMode::Standard);
///
/// // MySQL mode for compatibility
/// let mysql_mode = SqlMode::MySQL;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqlMode {
    /// SQL:1999 standard compliance (default)
    ///
    /// - Integer arithmetic returns Integer type
    /// - Example: `SELECT 1 + 2` → `3` (Integer)
    Standard,

    /// MySQL 8.x compatibility mode
    ///
    /// - Integer arithmetic returns Numeric type with decimal formatting
    /// - Example: `SELECT 1 + 2` → `3.000` (Numeric)
    MySQL,
}

impl Default for SqlMode {
    fn default() -> Self {
        // Default to SQL:1999 standard for backward compatibility
        SqlMode::Standard
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_mode() {
        let mode = SqlMode::default();
        assert_eq!(mode, SqlMode::Standard);
    }

    #[test]
    fn test_mode_equality() {
        assert_eq!(SqlMode::Standard, SqlMode::Standard);
        assert_eq!(SqlMode::MySQL, SqlMode::MySQL);
        assert_ne!(SqlMode::Standard, SqlMode::MySQL);
    }

    #[test]
    fn test_mode_clone() {
        let mode1 = SqlMode::MySQL;
        let mode2 = mode1;
        assert_eq!(mode1, mode2);
    }
}
