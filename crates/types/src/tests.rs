use super::*;

#[test]
fn test_integer_type_creation() {
    let int_type = DataType::Integer;
    assert_eq!(format!("{:?}", int_type), "Integer");
}

#[test]
fn test_varchar_type_with_length() {
    let varchar_type = DataType::Varchar { max_length: 255 };
    assert_eq!(format!("{:?}", varchar_type), "Varchar { max_length: 255 }");
}

#[test]
fn test_boolean_type_creation() {
    let bool_type = DataType::Boolean;
    assert_eq!(format!("{:?}", bool_type), "Boolean");
}

#[test]
fn test_numeric_type_with_precision_scale() {
    let numeric_type = DataType::Numeric { precision: 10, scale: 2 };
    assert_eq!(format!("{:?}", numeric_type), "Numeric { precision: 10, scale: 2 }");
}

#[test]
fn test_date_type_creation() {
    let date_type = DataType::Date;
    assert_eq!(format!("{:?}", date_type), "Date");
}

#[test]
fn test_integer_value_creation() {
    let value = SqlValue::Integer(42);
    assert_eq!(format!("{:?}", value), "Integer(42)");
}

#[test]
fn test_varchar_value_creation() {
    let value = SqlValue::Varchar("hello".to_string());
    assert_eq!(format!("{:?}", value), "Varchar(\"hello\")");
}

#[test]
fn test_boolean_true_value() {
    let value = SqlValue::Boolean(true);
    assert_eq!(format!("{:?}", value), "Boolean(true)");
}

#[test]
fn test_boolean_false_value() {
    let value = SqlValue::Boolean(false);
    assert_eq!(format!("{:?}", value), "Boolean(false)");
}

#[test]
fn test_null_value() {
    let value = SqlValue::Null;
    assert_eq!(format!("{:?}", value), "Null");
}

#[test]
fn test_null_is_null() {
    let value = SqlValue::Null;
    assert!(value.is_null());
}

#[test]
fn test_integer_is_not_null() {
    let value = SqlValue::Integer(42);
    assert!(!value.is_null());
}

#[test]
fn test_varchar_is_not_null() {
    let value = SqlValue::Varchar("test".to_string());
    assert!(!value.is_null());
}

#[test]
fn test_integer_value_has_integer_type() {
    let value = SqlValue::Integer(42);
    assert_eq!(value.get_type(), DataType::Integer);
}

#[test]
fn test_varchar_value_has_varchar_type() {
    let value = SqlValue::Varchar("test".to_string());
    match value.get_type() {
        DataType::Varchar { .. } => {}
        _ => panic!("Expected Varchar type"),
    }
}

#[test]
fn test_boolean_value_has_boolean_type() {
    let value = SqlValue::Boolean(true);
    assert_eq!(value.get_type(), DataType::Boolean);
}

#[test]
fn test_null_value_has_null_type() {
    let value = SqlValue::Null;
    assert_eq!(value.get_type(), DataType::Null);
}

#[test]
fn test_integer_compatible_with_integer() {
    assert!(DataType::Integer.is_compatible_with(&DataType::Integer));
}

#[test]
fn test_integer_not_compatible_with_varchar() {
    assert!(!DataType::Integer.is_compatible_with(&DataType::Varchar { max_length: 10 }));
}

#[test]
fn test_null_compatible_with_any_type() {
    assert!(DataType::Null.is_compatible_with(&DataType::Integer));
    assert!(DataType::Null.is_compatible_with(&DataType::Boolean));
    assert!(DataType::Null.is_compatible_with(&DataType::Varchar { max_length: 10 }));
}

#[test]
fn test_any_type_compatible_with_null() {
    assert!(DataType::Integer.is_compatible_with(&DataType::Null));
    assert!(DataType::Boolean.is_compatible_with(&DataType::Null));
    assert!(DataType::Varchar { max_length: 10 }.is_compatible_with(&DataType::Null));
}

#[test]
fn test_varchar_different_lengths_compatible() {
    let v1 = DataType::Varchar { max_length: 10 };
    let v2 = DataType::Varchar { max_length: 20 };
    assert!(v1.is_compatible_with(&v2));
}

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
fn test_boolean_display() {
    let true_val = SqlValue::Boolean(true);
    let false_val = SqlValue::Boolean(false);
    assert_eq!(format!("{}", true_val), "TRUE");
    assert_eq!(format!("{}", false_val), "FALSE");
}

#[test]
fn test_null_display() {
    let value = SqlValue::Null;
    assert_eq!(format!("{}", value), "NULL");
}
