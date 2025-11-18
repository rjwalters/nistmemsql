mod config;

pub use config::MySqlModeFlags;

/// SQL compatibility mode
///
/// VibeSQL supports different SQL dialect modes to match the behavior
/// of different database systems. This is necessary because SQL standards
/// allow implementation-defined behavior in certain areas.
///
/// ## Differences by Mode
///
/// See SQL_COMPATIBILITY_MODE.md in the repository root for a comprehensive list
/// of behavioral differences between modes.
///
/// ### Division Operator (`/`)
/// - **MySQL**: `INTEGER / INTEGER → DECIMAL` (floating-point division)
///   - Example: `83 / 6 = 13.8333`
/// - **SQLite**: `INTEGER / INTEGER → INTEGER` (truncated division)
///   - Example: `83 / 6 = 13`
///
/// ## Default Mode
///
/// MySQL mode is the default to maximize compatibility with the
/// dolthub/sqllogictest test suite, which was generated from MySQL 8.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SqlMode {
    /// MySQL 8.0+ compatibility mode (default)
    ///
    /// - Division returns DECIMAL (floating-point)
    /// - Other MySQL-specific behaviors controlled by flags
    MySQL { flags: MySqlModeFlags },

    /// SQLite 3 compatibility mode
    ///
    /// - Division returns INTEGER (truncated)
    /// - Other SQLite-specific behaviors
    ///
    /// Note: Currently not fully implemented. Many features will error
    /// with "TODO: SQLite mode not yet supported" messages.
    SQLite,
}

impl Default for SqlMode {
    fn default() -> Self {
        // Default to MySQL mode for SQLLogicTest compatibility
        SqlMode::MySQL { flags: MySqlModeFlags::default() }
    }
}

impl SqlMode {
    /// Check if division should return floating-point (true) or integer (false)
    pub fn division_returns_float(&self) -> bool {
        match self {
            SqlMode::MySQL { .. } => true,
            SqlMode::SQLite => false,
        }
    }

    /// Get the MySQL mode flags, if in MySQL mode
    pub fn mysql_flags(&self) -> Option<&MySqlModeFlags> {
        match self {
            SqlMode::MySQL { flags } => Some(flags),
            SqlMode::SQLite => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_is_mysql() {
        let mode = SqlMode::default();
        assert!(matches!(mode, SqlMode::MySQL { .. }));
    }

    #[test]
    fn test_default_mysql_has_default_flags() {
        let mode = SqlMode::default();
        if let SqlMode::MySQL { flags } = mode {
            assert_eq!(flags, MySqlModeFlags::default());
        } else {
            panic!("Expected MySQL mode");
        }
    }

    #[test]
    fn test_division_behavior() {
        let mysql_mode = SqlMode::MySQL { flags: MySqlModeFlags::default() };
        assert!(mysql_mode.division_returns_float());

        let sqlite_mode = SqlMode::SQLite;
        assert!(!sqlite_mode.division_returns_float());
    }

    #[test]
    fn test_mysql_flags_accessor() {
        let mysql_mode = SqlMode::MySQL { flags: MySqlModeFlags::with_pipes_as_concat() };
        assert!(mysql_mode.mysql_flags().is_some());
        assert!(mysql_mode.mysql_flags().unwrap().pipes_as_concat);

        let sqlite_mode = SqlMode::SQLite;
        assert!(sqlite_mode.mysql_flags().is_none());
    }

    #[test]
    fn test_sqlmode_with_flags() {
        let mode = SqlMode::MySQL {
            flags: MySqlModeFlags { pipes_as_concat: true, ..Default::default() },
        };

        match mode {
            SqlMode::MySQL { flags } => {
                assert!(flags.pipes_as_concat);
                assert!(!flags.ansi_quotes);
                assert!(!flags.strict_mode);
            }
            _ => panic!("Expected MySQL mode"),
        }
    }

    #[test]
    fn test_sqlmode_with_custom_flags() {
        let mode = SqlMode::MySQL { flags: MySqlModeFlags::ansi() };

        match mode {
            SqlMode::MySQL { flags } => {
                assert!(flags.pipes_as_concat);
                assert!(flags.ansi_quotes);
                assert!(!flags.strict_mode);
            }
            _ => panic!("Expected MySQL mode"),
        }
    }
}

// Comprehensive test suites for mode-specific behavior
#[cfg(test)]
mod operators_tests;

#[cfg(test)]
mod types_tests;

#[cfg(test)]
mod strings_tests;

#[cfg(test)]
mod config_tests;
