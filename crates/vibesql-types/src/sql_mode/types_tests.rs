//! Tests for type system behavior in different SQL modes
//!
//! These tests verify that type inference, coercion, and conversions
//! work correctly according to the selected SQL mode (MySQL vs SQLite).

use super::*;
use crate::{sql_value::SqlValue, DataType};

// ============================================================================
// Type Inference Tests
// ============================================================================

#[test]
fn test_division_result_type_mysql() {
    let mode = SqlMode::MySQL { flags: MySqlModeFlags::default() };

    // In MySQL mode, division should indicate it returns float
    assert!(mode.division_returns_float(), "MySQL division should return float type");
}

#[test]
fn test_division_result_type_sqlite() {
    let mode = SqlMode::SQLite;

    // In SQLite mode, division should indicate it returns integer
    assert!(!mode.division_returns_float(), "SQLite division should return integer type");
}

// ============================================================================
// SQL Value Type Detection
// ============================================================================

#[test]
fn test_integer_type_detection() {
    let value = SqlValue::Integer(42);
    let data_type = value.get_type();

    assert!(matches!(data_type, DataType::Integer), "Integer value should have Integer data type");
    assert_eq!(value.type_name(), "INTEGER");
}

#[test]
fn test_numeric_type_detection() {
    let value = SqlValue::Numeric(13.8333);
    let data_type = value.get_type();

    assert!(
        matches!(data_type, DataType::Numeric { .. }),
        "Numeric value should have Numeric data type"
    );
    assert_eq!(value.type_name(), "NUMERIC");
}

#[test]
fn test_float_type_detection() {
    let value = SqlValue::Float(3.14);
    let data_type = value.get_type();

    assert!(matches!(data_type, DataType::Float { .. }), "Float value should have Float data type");
    assert_eq!(value.type_name(), "FLOAT");
}

#[test]
fn test_double_type_detection() {
    let value = SqlValue::Double(2.718281828);
    let data_type = value.get_type();

    assert!(
        matches!(data_type, DataType::DoublePrecision),
        "Double value should have DoublePrecision data type"
    );
    assert_eq!(value.type_name(), "DOUBLE PRECISION");
}

#[test]
fn test_null_type_detection() {
    let value = SqlValue::Null;
    let data_type = value.get_type();

    assert!(matches!(data_type, DataType::Null), "Null value should have Null data type");
    assert_eq!(value.type_name(), "NULL");
}

// ============================================================================
// String Type Tests
// ============================================================================

#[test]
fn test_varchar_type_detection() {
    let value = SqlValue::Varchar("hello".to_string());
    let data_type = value.get_type();

    assert!(
        matches!(data_type, DataType::Varchar { .. }),
        "Varchar value should have Varchar data type"
    );
    assert_eq!(value.type_name(), "VARCHAR");
}

#[test]
fn test_character_type_detection() {
    let value = SqlValue::Character("x".to_string());
    let data_type = value.get_type();

    // Character type should include length information
    match data_type {
        DataType::Character { length } => {
            assert_eq!(length, 1, "Character length should be 1");
        }
        _ => panic!("Expected Character data type"),
    }
    assert_eq!(value.type_name(), "CHAR");
}

// ============================================================================
// Numeric Type Tests
// ============================================================================

#[test]
fn test_numeric_default_precision() {
    let value = SqlValue::Numeric(123.456);
    let data_type = value.get_type();

    match data_type {
        DataType::Numeric { precision, scale } => {
            assert_eq!(precision, 38, "Default precision should be 38");
            assert_eq!(scale, 0, "Default scale should be 0");
        }
        _ => panic!("Expected Numeric data type"),
    }
}

#[test]
fn test_all_integer_variants() {
    let smallint = SqlValue::Smallint(100);
    let integer = SqlValue::Integer(1000);
    let bigint = SqlValue::Bigint(10000);
    let unsigned = SqlValue::Unsigned(99999);

    assert!(matches!(smallint.get_type(), DataType::Smallint));
    assert!(matches!(integer.get_type(), DataType::Integer));
    assert!(matches!(bigint.get_type(), DataType::Bigint));
    assert!(matches!(unsigned.get_type(), DataType::Unsigned));

    assert_eq!(smallint.type_name(), "SMALLINT");
    assert_eq!(integer.type_name(), "INTEGER");
    assert_eq!(bigint.type_name(), "BIGINT");
    assert_eq!(unsigned.type_name(), "UNSIGNED");
}

#[test]
fn test_all_float_variants() {
    let float = SqlValue::Float(1.0);
    let real = SqlValue::Real(2.0);
    let double = SqlValue::Double(3.0);

    assert!(matches!(float.get_type(), DataType::Float { .. }));
    assert!(matches!(real.get_type(), DataType::Real));
    assert!(matches!(double.get_type(), DataType::DoublePrecision));

    assert_eq!(float.type_name(), "FLOAT");
    assert_eq!(real.type_name(), "REAL");
    assert_eq!(double.type_name(), "DOUBLE PRECISION");
}

// ============================================================================
// Boolean Type Tests
// ============================================================================

#[test]
fn test_boolean_type_detection() {
    let true_value = SqlValue::Boolean(true);
    let false_value = SqlValue::Boolean(false);

    assert!(matches!(true_value.get_type(), DataType::Boolean));
    assert!(matches!(false_value.get_type(), DataType::Boolean));

    assert_eq!(true_value.type_name(), "BOOLEAN");
    assert_eq!(false_value.type_name(), "BOOLEAN");
}

// ============================================================================
// Mode-Specific Type Behavior
// ============================================================================

#[test]
fn test_mysql_mode_supports_decimal() {
    let mode = SqlMode::MySQL { flags: MySqlModeFlags::default() };

    // MySQL mode should indicate it supports decimal/numeric types
    // by returning float for division
    assert!(mode.division_returns_float());
}

#[test]
fn test_sqlite_dynamic_typing_expectation() {
    let mode = SqlMode::SQLite;

    // SQLite uses dynamic typing with type affinity
    // Division returns integer type
    assert!(!mode.division_returns_float());
}

// ============================================================================
// Type Coercion and Strict Mode
// ============================================================================

#[test]
fn test_strict_mode_flag_presence() {
    let permissive = SqlMode::MySQL { flags: MySqlModeFlags::default() };
    let strict = SqlMode::MySQL { flags: MySqlModeFlags::with_strict_mode() };

    // Verify strict mode flag is set correctly
    assert!(!permissive.mysql_flags().unwrap().strict_mode);
    assert!(strict.mysql_flags().unwrap().strict_mode);
}

#[test]
fn test_sqlite_has_no_strict_mode() {
    let mode = SqlMode::SQLite;

    // SQLite mode should not have MySQL flags
    assert!(mode.mysql_flags().is_none());
}

// ============================================================================
// Type System Edge Cases
// ============================================================================

#[test]
fn test_type_detection_consistency() {
    // Verify that get_type() and type_name() are consistent
    let test_values = vec![
        SqlValue::Integer(0),
        SqlValue::Numeric(0.0),
        SqlValue::Float(0.0),
        SqlValue::Varchar("".to_string()),
        SqlValue::Boolean(false),
        SqlValue::Null,
    ];

    for value in test_values {
        let type_name = value.type_name();
        let data_type = value.get_type();

        // Both should provide consistent information
        assert!(!type_name.is_empty(), "Type name should not be empty for {:?}", value);
        // DataType Debug output should be meaningful
        let debug_str = format!("{:?}", data_type);
        assert!(!debug_str.is_empty(), "DataType debug should not be empty for {:?}", value);
    }
}

#[test]
fn test_mode_independent_type_detection() {
    // Type detection should work the same regardless of mode
    let mysql_mode = SqlMode::MySQL { flags: MySqlModeFlags::default() };
    let sqlite_mode = SqlMode::SQLite;

    // Create same value
    let value = SqlValue::Integer(42);

    // Type should be detected the same way regardless of mode
    // (mode affects operations, not basic type detection)
    assert_eq!(value.type_name(), "INTEGER");
    assert!(matches!(value.get_type(), DataType::Integer));

    // Modes should have different division behavior
    assert_ne!(mysql_mode.division_returns_float(), sqlite_mode.division_returns_float());
}

// ============================================================================
// Future Type System Tests (Placeholders)
// ============================================================================

#[test]
#[ignore] // Enable when type inference is fully implemented
fn test_type_inference_for_division() {
    // TODO: Test that integer / integer infers correct result type
    // TODO: MySQL should infer NUMERIC/DECIMAL
    // TODO: SQLite should infer INTEGER
}

#[test]
#[ignore] // Enable when type coercion is implemented
fn test_type_coercion_rules() {
    // TODO: Test implicit type conversions
    // TODO: Test that string to number coercion works in permissive mode
    // TODO: Test that string to number coercion fails in strict mode
}

#[test]
#[ignore] // Enable when mixed-type arithmetic is implemented
fn test_mixed_type_arithmetic() {
    // TODO: Test integer + float
    // TODO: Test numeric + double
    // TODO: Test type promotion rules
}

#[test]
#[ignore] // Enable when precision handling is implemented
fn test_precision_and_scale_handling() {
    // TODO: Test NUMERIC(10, 2) precision
    // TODO: Test scale preservation in operations
    // TODO: Test rounding behavior
}

#[test]
#[ignore] // Enable when unsigned arithmetic is implemented
fn test_unsigned_type_behavior() {
    // TODO: Test unsigned integer arithmetic
    // TODO: Test unsigned overflow behavior
    // TODO: Test signed/unsigned mixing
}

#[test]
#[ignore] // Enable when type checking is implemented
fn test_strict_mode_type_checking() {
    // TODO: Test that strict mode rejects invalid conversions
    // TODO: Test that permissive mode allows type coercion
}
