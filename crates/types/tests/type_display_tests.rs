use types::*;

// ============================================================================
// Display/Format Tests - How types are displayed
// ============================================================================

#[test]
fn test_integer_display() {
    let value = SqlValue::Integer(42);
    assert_eq!(format!("{}", value), "42");
}

#[test]
fn test_varchar_display() {
    let value = SqlValue::Varchar("hello".to_string());
    assert_eq!(format!("{}", value), "hello");
}

#[test]
fn test_boolean_true_display() {
    let value = SqlValue::Boolean(true);
    assert_eq!(format!("{}", value), "TRUE");
}

#[test]
fn test_boolean_false_display() {
    let value = SqlValue::Boolean(false);
    assert_eq!(format!("{}", value), "FALSE");
}

#[test]
fn test_null_display() {
    let value = SqlValue::Null;
    assert_eq!(format!("{}", value), "NULL");
}

#[test]
fn test_smallint_display() {
    let value = SqlValue::Smallint(100);
    assert_eq!(format!("{}", value), "100");
}

#[test]
fn test_bigint_display() {
    let value = SqlValue::Bigint(1000000);
    assert_eq!(format!("{}", value), "1000000");
}

#[test]
fn test_numeric_display() {
    let value = SqlValue::Numeric("123.45".to_string());
    assert_eq!(format!("{}", value), "123.45");
}

#[test]
fn test_float_display() {
    let value = SqlValue::Float(2.5);
    assert_eq!(format!("{}", value), "2.5");
}

#[test]
fn test_real_display() {
    let value = SqlValue::Real(2.71);
    assert_eq!(format!("{}", value), "2.71");
}

#[test]
fn test_double_display() {
    let value = SqlValue::Double(123.456);
    assert_eq!(format!("{}", value), "123.456");
}

#[test]
fn test_character_display() {
    let value = SqlValue::Character("test".to_string());
    assert_eq!(format!("{}", value), "test");
}

#[test]
fn test_date_display() {
    let value = SqlValue::Date("2024-01-01".to_string());
    assert_eq!(format!("{}", value), "2024-01-01");
}

#[test]
fn test_time_display() {
    let value = SqlValue::Time("12:30:00".to_string());
    assert_eq!(format!("{}", value), "12:30:00");
}

#[test]
fn test_timestamp_display() {
    let value = SqlValue::Timestamp("2024-01-01 12:30:00".to_string());
    assert_eq!(format!("{}", value), "2024-01-01 12:30:00");
}
