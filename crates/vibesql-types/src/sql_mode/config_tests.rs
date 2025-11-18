//! Tests for SQL mode configuration and flags
//!
//! These tests verify that MySqlModeFlags and SqlMode configuration
//! work correctly and can be composed properly.

use super::*;

// ============================================================================
// MySqlModeFlags Default Values
// ============================================================================

#[test]
fn test_mysql_flags_default() {
    let flags = MySqlModeFlags::default();

    assert!(!flags.pipes_as_concat, "|| should be OR by default");
    assert!(!flags.ansi_quotes, "\" should be string by default");
    assert!(!flags.strict_mode, "Should be permissive by default");
}

#[test]
fn test_mysql_flags_new_equals_default() {
    let new_flags = MySqlModeFlags::new();
    let default_flags = MySqlModeFlags::default();

    assert_eq!(new_flags, default_flags, "new() should equal default()");
}

// ============================================================================
// Individual Flag Constructors
// ============================================================================

#[test]
fn test_with_pipes_as_concat() {
    let flags = MySqlModeFlags::with_pipes_as_concat();

    assert!(flags.pipes_as_concat, "pipes_as_concat should be true");
    assert!(!flags.ansi_quotes, "ansi_quotes should remain false");
    assert!(!flags.strict_mode, "strict_mode should remain false");
}

#[test]
fn test_with_ansi_quotes() {
    let flags = MySqlModeFlags::with_ansi_quotes();

    assert!(!flags.pipes_as_concat, "pipes_as_concat should remain false");
    assert!(flags.ansi_quotes, "ansi_quotes should be true");
    assert!(!flags.strict_mode, "strict_mode should remain false");
}

#[test]
fn test_with_strict_mode() {
    let flags = MySqlModeFlags::with_strict_mode();

    assert!(!flags.pipes_as_concat, "pipes_as_concat should remain false");
    assert!(!flags.ansi_quotes, "ansi_quotes should remain false");
    assert!(flags.strict_mode, "strict_mode should be true");
}

// ============================================================================
// Combination Flag Constructors
// ============================================================================

#[test]
fn test_ansi_mode() {
    let flags = MySqlModeFlags::ansi();

    assert!(flags.pipes_as_concat, "ANSI mode should enable pipes_as_concat");
    assert!(flags.ansi_quotes, "ANSI mode should enable ansi_quotes");
    assert!(!flags.strict_mode, "ANSI mode should not enable strict_mode");
}

// ============================================================================
// Custom Flag Combinations
// ============================================================================

#[test]
fn test_all_flags_enabled() {
    let flags = MySqlModeFlags { pipes_as_concat: true, ansi_quotes: true, strict_mode: true };

    assert!(flags.pipes_as_concat);
    assert!(flags.ansi_quotes);
    assert!(flags.strict_mode);
}

#[test]
fn test_all_flags_disabled() {
    let flags = MySqlModeFlags { pipes_as_concat: false, ansi_quotes: false, strict_mode: false };

    assert!(!flags.pipes_as_concat);
    assert!(!flags.ansi_quotes);
    assert!(!flags.strict_mode);
}

#[test]
fn test_partial_flag_combinations() {
    // Test various combinations
    let combo1 = MySqlModeFlags { pipes_as_concat: true, ansi_quotes: false, strict_mode: true };
    assert!(combo1.pipes_as_concat && combo1.strict_mode && !combo1.ansi_quotes);

    let combo2 = MySqlModeFlags { pipes_as_concat: false, ansi_quotes: true, strict_mode: true };
    assert!(!combo2.pipes_as_concat && combo2.ansi_quotes && combo2.strict_mode);
}

// ============================================================================
// SqlMode Configuration Tests
// ============================================================================

#[test]
fn test_sqlmode_default_is_mysql() {
    let mode = SqlMode::default();

    match mode {
        SqlMode::MySQL { flags } => {
            assert_eq!(flags, MySqlModeFlags::default());
        }
        _ => panic!("Default mode should be MySQL"),
    }
}

#[test]
fn test_sqlmode_mysql_with_default_flags() {
    let mode = SqlMode::MySQL { flags: MySqlModeFlags::default() };

    assert_eq!(mode, SqlMode::default());
}

#[test]
fn test_sqlmode_mysql_with_custom_flags() {
    let mode = SqlMode::MySQL { flags: MySqlModeFlags::ansi() };

    match mode {
        SqlMode::MySQL { flags } => {
            assert!(flags.pipes_as_concat);
            assert!(flags.ansi_quotes);
        }
        _ => panic!("Expected MySQL mode"),
    }
}

#[test]
fn test_sqlmode_sqlite() {
    let mode = SqlMode::SQLite;

    // SQLite mode should not have MySQL flags
    assert!(mode.mysql_flags().is_none());
    assert!(!mode.division_returns_float());
}

// ============================================================================
// Flag Equality and Comparison
// ============================================================================

#[test]
fn test_flags_equality() {
    let flags1 = MySqlModeFlags::default();
    let flags2 = MySqlModeFlags::new();
    let flags3 = MySqlModeFlags::with_strict_mode();

    assert_eq!(flags1, flags2);
    assert_ne!(flags1, flags3);
}

#[test]
fn test_mode_equality_with_flags() {
    let mode1 = SqlMode::MySQL { flags: MySqlModeFlags::default() };
    let mode2 = SqlMode::MySQL { flags: MySqlModeFlags::new() };
    let mode3 = SqlMode::MySQL { flags: MySqlModeFlags::with_strict_mode() };

    assert_eq!(mode1, mode2, "Same flags should be equal");
    assert_ne!(mode1, mode3, "Different flags should not be equal");
}

#[test]
fn test_mode_equality_mysql_vs_sqlite() {
    let mysql = SqlMode::MySQL { flags: MySqlModeFlags::default() };
    let sqlite = SqlMode::SQLite;

    assert_ne!(mysql, sqlite, "MySQL and SQLite modes should not be equal");
}

// ============================================================================
// Flag Cloning Tests
// ============================================================================

#[test]
fn test_flags_clone() {
    let original = MySqlModeFlags { pipes_as_concat: true, ansi_quotes: true, strict_mode: false };

    let cloned = original.clone();

    assert_eq!(original, cloned);
    assert!(cloned.pipes_as_concat);
    assert!(cloned.ansi_quotes);
    assert!(!cloned.strict_mode);
}

#[test]
fn test_mode_clone() {
    let original = SqlMode::MySQL { flags: MySqlModeFlags::ansi() };

    let cloned = original.clone();

    assert_eq!(original, cloned);
}

// ============================================================================
// Flag Modification Patterns
// ============================================================================

#[test]
fn test_struct_update_syntax() {
    let base = MySqlModeFlags::default();

    let modified = MySqlModeFlags { strict_mode: true, ..base };

    assert!(!modified.pipes_as_concat, "Should inherit default");
    assert!(!modified.ansi_quotes, "Should inherit default");
    assert!(modified.strict_mode, "Should be modified");
}

#[test]
fn test_multiple_modifications() {
    let base = MySqlModeFlags::default();

    let modified = MySqlModeFlags { pipes_as_concat: true, strict_mode: true, ..base };

    assert!(modified.pipes_as_concat, "First modification");
    assert!(!modified.ansi_quotes, "Should inherit default");
    assert!(modified.strict_mode, "Second modification");
}

// ============================================================================
// Mode Accessor Methods
// ============================================================================

#[test]
fn test_mysql_flags_accessor() {
    let mode = SqlMode::MySQL { flags: MySqlModeFlags::with_strict_mode() };

    let flags = mode.mysql_flags();
    assert!(flags.is_some(), "MySQL mode should have flags");
    assert!(flags.unwrap().strict_mode);
}

#[test]
fn test_sqlite_flags_accessor() {
    let mode = SqlMode::SQLite;

    let flags = mode.mysql_flags();
    assert!(flags.is_none(), "SQLite mode should not have MySQL flags");
}

#[test]
fn test_division_behavior_accessor() {
    let mysql = SqlMode::MySQL { flags: MySqlModeFlags::default() };
    let sqlite = SqlMode::SQLite;

    assert!(mysql.division_returns_float(), "MySQL uses float division");
    assert!(!sqlite.division_returns_float(), "SQLite uses integer division");
}

// ============================================================================
// Debug and Display
// ============================================================================

#[test]
fn test_flags_debug() {
    let flags = MySqlModeFlags::ansi();
    let debug_str = format!("{:?}", flags);

    // Debug should be meaningful and contain flag info
    assert!(!debug_str.is_empty(), "Debug output should not be empty");
}

#[test]
fn test_mode_debug() {
    let mysql = SqlMode::MySQL { flags: MySqlModeFlags::default() };
    let sqlite = SqlMode::SQLite;

    let mysql_debug = format!("{:?}", mysql);
    let sqlite_debug = format!("{:?}", sqlite);

    assert!(!mysql_debug.is_empty());
    assert!(!sqlite_debug.is_empty());
    assert_ne!(mysql_debug, sqlite_debug);
}

// ============================================================================
// Hash Behavior
// ============================================================================

#[test]
fn test_flags_can_be_hashed() {
    use std::collections::HashSet;

    let mut set = HashSet::new();
    set.insert(MySqlModeFlags::default());
    set.insert(MySqlModeFlags::with_strict_mode());
    set.insert(MySqlModeFlags::ansi());

    assert_eq!(set.len(), 3, "All different flags should hash uniquely");

    // Same flags should deduplicate
    set.insert(MySqlModeFlags::default());
    assert_eq!(set.len(), 3, "Duplicate flags should not increase size");
}

#[test]
fn test_mode_can_be_hashed() {
    use std::collections::HashSet;

    let mut set = HashSet::new();
    set.insert(SqlMode::MySQL { flags: MySqlModeFlags::default() });
    set.insert(SqlMode::MySQL { flags: MySqlModeFlags::ansi() });
    set.insert(SqlMode::SQLite);

    assert_eq!(set.len(), 3, "All different modes should hash uniquely");
}

// ============================================================================
// Configuration Consistency Tests
// ============================================================================

#[test]
fn test_flags_consistent_across_constructors() {
    // Verify that using constructors vs struct literal gives same result
    let via_constructor = MySqlModeFlags::with_pipes_as_concat();
    let via_literal = MySqlModeFlags { pipes_as_concat: true, ..Default::default() };

    assert_eq!(via_constructor, via_literal);
}

#[test]
fn test_mode_consistent_across_creation_methods() {
    let default_mode = SqlMode::default();
    let explicit_mode = SqlMode::MySQL { flags: MySqlModeFlags::default() };

    assert_eq!(default_mode, explicit_mode);
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_flags_builder_pattern_simulation() {
    // Simulate a builder pattern using struct update syntax
    let flags = MySqlModeFlags::default();
    let flags = MySqlModeFlags { pipes_as_concat: true, ..flags };
    let flags = MySqlModeFlags { ansi_quotes: true, ..flags };
    let flags = MySqlModeFlags { strict_mode: true, ..flags };

    assert!(flags.pipes_as_concat);
    assert!(flags.ansi_quotes);
    assert!(flags.strict_mode);
}

// ============================================================================
// Future Configuration Tests (Placeholders)
// ============================================================================

#[test]
#[ignore] // Enable when more flags are added
fn test_future_mysql_flags() {
    // TODO: Test additional MySQL mode flags as they are added
    // Examples: NO_ZERO_DATE, ERROR_FOR_DIVISION_BY_ZERO, etc.
}

#[test]
#[ignore] // Enable when SQLite flags are added
fn test_sqlite_configuration() {
    // TODO: Test SQLite-specific configuration if added
    // Examples: PRAGMA settings, compatibility modes
}

#[test]
#[ignore] // Enable when mode switching is implemented
fn test_mode_switching_behavior() {
    // TODO: Test behavior when switching between modes
    // TODO: Test state preservation or reset
}

#[test]
#[ignore] // Enable when session variables are implemented
fn test_session_mode_configuration() {
    // TODO: Test setting mode via session variables
    // TODO: Test persistence across queries
}
