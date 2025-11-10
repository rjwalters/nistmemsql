use vibesql_types::*;

// ============================================================================
// Type Compatibility Tests - Can we assign/compare values?
// ============================================================================

#[test]
fn test_integer_compatible_with_integer() {
    assert!(DataType::Integer.is_compatible_with(&DataType::Integer));
}

#[test]
fn test_integer_not_compatible_with_varchar() {
    assert!(!DataType::Integer.is_compatible_with(&DataType::Varchar { max_length: Some(10) }));
}

#[test]
fn test_null_compatible_with_any_type() {
    assert!(DataType::Null.is_compatible_with(&DataType::Integer));
    assert!(DataType::Null.is_compatible_with(&DataType::Boolean));
    assert!(DataType::Null.is_compatible_with(&DataType::Varchar { max_length: Some(10) }));
}

#[test]
fn test_any_type_compatible_with_null() {
    assert!(DataType::Integer.is_compatible_with(&DataType::Null));
    assert!(DataType::Boolean.is_compatible_with(&DataType::Null));
    assert!(DataType::Varchar { max_length: Some(10) }.is_compatible_with(&DataType::Null));
}

#[test]
fn test_varchar_different_lengths_compatible() {
    // VARCHAR(10) and VARCHAR(20) should be compatible for comparison
    let v1 = DataType::Varchar { max_length: Some(10) };
    let v2 = DataType::Varchar { max_length: Some(20) };
    assert!(v1.is_compatible_with(&v2));
}

#[test]
fn test_boolean_compatible_with_boolean() {
    assert!(DataType::Boolean.is_compatible_with(&DataType::Boolean));
}

#[test]
fn test_date_compatible_with_date() {
    assert!(DataType::Date.is_compatible_with(&DataType::Date));
}
