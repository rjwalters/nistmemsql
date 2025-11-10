use vibesql_types::*;

// ============================================================================
// DataType Tests - Define what we want our type system to do
// ============================================================================

#[test]
fn test_integer_type_creation() {
    let int_type = DataType::Integer;
    assert_eq!(format!("{:?}", int_type), "Integer");
}

#[test]
fn test_varchar_type_with_length() {
    let varchar_type = DataType::Varchar { max_length: Some(255) };
    assert_eq!(format!("{:?}", varchar_type), "Varchar { max_length: Some(255) }");
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
