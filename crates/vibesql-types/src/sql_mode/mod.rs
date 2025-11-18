pub mod types;

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

mod operators;

// Re-export operator types and traits
pub use operators::{ConcatOperator, DivisionBehavior, OperatorBehavior};
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqlMode {
    /// MySQL 8.0+ compatibility mode (default)
    ///
    /// - Division returns DECIMAL (floating-point)
    /// - Other MySQL-specific behaviors
    MySQL,

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
        SqlMode::MySQL
    }
}

impl SqlMode {
    /// Check if division should return floating-point (true) or integer (false)
    pub fn division_returns_float(self) -> bool {
        match self {
            SqlMode::MySQL => true,
            SqlMode::SQLite => false,
        }
    }
}

impl OperatorBehavior for SqlMode {
    fn integer_division_behavior(&self) -> DivisionBehavior {
        match self {
            SqlMode::MySQL => DivisionBehavior::Decimal,
            SqlMode::SQLite => DivisionBehavior::Integer,
        }
    }

    fn supports_xor(&self) -> bool {
        match self {
            SqlMode::MySQL => true,
            SqlMode::SQLite => false,
        }
    }

    fn supports_integer_div_operator(&self) -> bool {
        match self {
            SqlMode::MySQL => true,
            SqlMode::SQLite => false,
        }
    }

    fn string_concat_operator(&self) -> ConcatOperator {
        match self {
            SqlMode::MySQL => ConcatOperator::Function,
            SqlMode::SQLite => ConcatOperator::PipePipe,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_is_mysql() {
        assert_eq!(SqlMode::default(), SqlMode::MySQL);
    }

    #[test]
    fn test_division_behavior() {
        assert!(SqlMode::MySQL.division_returns_float());
        assert!(!SqlMode::SQLite.division_returns_float());
    }

    // Tests for OperatorBehavior trait implementation

    #[test]
    fn test_integer_division_behavior() {
        assert_eq!(
            SqlMode::MySQL.integer_division_behavior(),
            DivisionBehavior::Decimal
        );
        assert_eq!(
            SqlMode::SQLite.integer_division_behavior(),
            DivisionBehavior::Integer
        );
    }

    #[test]
    fn test_xor_support() {
        assert!(SqlMode::MySQL.supports_xor());
        assert!(!SqlMode::SQLite.supports_xor());
    }

    #[test]
    fn test_integer_div_operator_support() {
        assert!(SqlMode::MySQL.supports_integer_div_operator());
        assert!(!SqlMode::SQLite.supports_integer_div_operator());
    }

    #[test]
    fn test_string_concat_operator() {
        assert_eq!(
            SqlMode::MySQL.string_concat_operator(),
            ConcatOperator::Function
        );
        assert_eq!(
            SqlMode::SQLite.string_concat_operator(),
            ConcatOperator::PipePipe
        );
    }
}
