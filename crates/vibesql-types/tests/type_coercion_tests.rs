use vibesql_types::*;

// ============================================================================
// SQL:1999 Type Coercion Tests
// ============================================================================

// ----------------------------------------------------------------------------
// Numeric Type Coercion
// ----------------------------------------------------------------------------

#[test]
fn test_integer_to_bigint_coercion() {
    let result = DataType::Integer.coerce_to_common(&DataType::Bigint);
    assert_eq!(result, Some(DataType::Bigint));
}

#[test]
fn test_smallint_to_integer_coercion() {
    let result = DataType::Smallint.coerce_to_common(&DataType::Integer);
    assert_eq!(result, Some(DataType::Integer));
}

#[test]
fn test_integer_to_decimal_coercion() {
    let result = DataType::Integer.coerce_to_common(&DataType::Decimal { precision: 10, scale: 2 });
    assert_eq!(result, Some(DataType::Decimal { precision: 10, scale: 2 }));
}

#[test]
fn test_decimal_to_double_coercion() {
    let result =
        DataType::Decimal { precision: 10, scale: 2 }.coerce_to_common(&DataType::DoublePrecision);
    assert_eq!(result, Some(DataType::DoublePrecision));
}

#[test]
fn test_integer_to_real_coercion() {
    let result = DataType::Integer.coerce_to_common(&DataType::Real);
    assert_eq!(result, Some(DataType::Real));
}

#[test]
fn test_bigint_to_integer_coercion() {
    // Coercion should work in both directions (narrowing allowed)
    let result = DataType::Bigint.coerce_to_common(&DataType::Integer);
    assert_eq!(result, Some(DataType::Bigint)); // Bigint has higher precedence
}

#[test]
fn test_decimal_precision_scale_combination() {
    let dec1 = DataType::Decimal { precision: 10, scale: 2 };
    let dec2 = DataType::Decimal { precision: 8, scale: 4 };
    let result = dec1.coerce_to_common(&dec2);
    // Should use max of both precision and scale
    assert_eq!(result, Some(DataType::Decimal { precision: 10, scale: 4 }));
}

#[test]
fn test_numeric_types_are_compatible() {
    assert!(DataType::Smallint.is_compatible_with(&DataType::Integer));
    assert!(DataType::Integer.is_compatible_with(&DataType::Bigint));
    assert!(DataType::Integer.is_compatible_with(&DataType::Decimal { precision: 10, scale: 0 }));
    assert!(DataType::Decimal { precision: 10, scale: 2 }.is_compatible_with(&DataType::Real));
    assert!(DataType::Integer.is_compatible_with(&DataType::DoublePrecision));
}

// ----------------------------------------------------------------------------
// String Type Coercion
// ----------------------------------------------------------------------------

#[test]
fn test_varchar_length_coercion() {
    let v1 = DataType::Varchar { max_length: Some(10) };
    let v2 = DataType::Varchar { max_length: Some(20) };
    let result = v1.coerce_to_common(&v2);
    // Should use the larger length
    assert_eq!(result, Some(DataType::Varchar { max_length: Some(20) }));
}

#[test]
fn test_varchar_unlimited_coercion() {
    let v1 = DataType::Varchar { max_length: Some(10) };
    let v2 = DataType::Varchar { max_length: None };
    let result = v1.coerce_to_common(&v2);
    // Should result in unlimited length
    assert_eq!(result, Some(DataType::Varchar { max_length: None }));
}

#[test]
fn test_name_to_varchar_coercion() {
    let result = DataType::Name.coerce_to_common(&DataType::Varchar { max_length: Some(50) });
    // VARCHAR has higher precedence than NAME
    assert_eq!(result, Some(DataType::Varchar { max_length: Some(50) }));
}

#[test]
fn test_character_to_varchar_coercion() {
    let result = DataType::Character { length: 10 }
        .coerce_to_common(&DataType::Varchar { max_length: Some(20) });
    assert_eq!(result, Some(DataType::Varchar { max_length: Some(20) }));
}

#[test]
fn test_string_types_are_compatible() {
    assert!(DataType::Character { length: 10 }
        .is_compatible_with(&DataType::Varchar { max_length: Some(20) }));
    assert!(DataType::Name.is_compatible_with(&DataType::Varchar { max_length: Some(50) }));
    assert!(DataType::Varchar { max_length: Some(10) }
        .is_compatible_with(&DataType::CharacterLargeObject));
}

// ----------------------------------------------------------------------------
// Temporal Type Coercion
// ----------------------------------------------------------------------------

#[test]
fn test_date_to_timestamp_coercion() {
    let result = DataType::Date.coerce_to_common(&DataType::Timestamp { with_timezone: false });
    assert_eq!(result, Some(DataType::Timestamp { with_timezone: false }));
}

#[test]
fn test_time_to_timestamp_coercion() {
    let result = DataType::Time { with_timezone: false }
        .coerce_to_common(&DataType::Timestamp { with_timezone: false });
    assert_eq!(result, Some(DataType::Timestamp { with_timezone: false }));
}

#[test]
fn test_temporal_types_are_compatible() {
    assert!(DataType::Date.is_compatible_with(&DataType::Timestamp { with_timezone: false }));
    assert!(DataType::Time { with_timezone: false }
        .is_compatible_with(&DataType::Timestamp { with_timezone: false }));
}

// ----------------------------------------------------------------------------
// NULL Coercion
// ----------------------------------------------------------------------------

#[test]
fn test_null_coerces_to_integer() {
    let result = DataType::Null.coerce_to_common(&DataType::Integer);
    assert_eq!(result, Some(DataType::Integer));
}

#[test]
fn test_integer_coerces_with_null() {
    let result = DataType::Integer.coerce_to_common(&DataType::Null);
    assert_eq!(result, Some(DataType::Integer));
}

#[test]
fn test_null_coerces_to_varchar() {
    let result = DataType::Null.coerce_to_common(&DataType::Varchar { max_length: Some(50) });
    assert_eq!(result, Some(DataType::Varchar { max_length: Some(50) }));
}

// ----------------------------------------------------------------------------
// Type Incompatibility
// ----------------------------------------------------------------------------

#[test]
fn test_integer_not_compatible_with_boolean() {
    assert!(!DataType::Integer.is_compatible_with(&DataType::Boolean));
    assert_eq!(DataType::Integer.coerce_to_common(&DataType::Boolean), None);
}

#[test]
fn test_integer_not_compatible_with_date() {
    assert!(!DataType::Integer.is_compatible_with(&DataType::Date));
    assert_eq!(DataType::Integer.coerce_to_common(&DataType::Date), None);
}

#[test]
fn test_varchar_not_compatible_with_integer() {
    // Strings and numbers don't implicitly coerce in SQL:1999
    assert!(!DataType::Varchar { max_length: Some(10) }.is_compatible_with(&DataType::Integer));
    assert_eq!(
        DataType::Varchar { max_length: Some(10) }.coerce_to_common(&DataType::Integer),
        None
    );
}

#[test]
fn test_boolean_not_compatible_with_varchar() {
    assert!(!DataType::Boolean.is_compatible_with(&DataType::Varchar { max_length: Some(10) }));
    assert_eq!(
        DataType::Boolean.coerce_to_common(&DataType::Varchar { max_length: Some(10) }),
        None
    );
}

#[test]
fn test_date_not_compatible_with_integer() {
    assert!(!DataType::Date.is_compatible_with(&DataType::Integer));
    assert_eq!(DataType::Date.coerce_to_common(&DataType::Integer), None);
}

// ----------------------------------------------------------------------------
// User-Defined Types
// ----------------------------------------------------------------------------

#[test]
fn test_user_defined_types_same_name_compatible() {
    let udt1 = DataType::UserDefined { type_name: "MyType".to_string() };
    let udt2 = DataType::UserDefined { type_name: "MyType".to_string() };
    assert!(udt1.is_compatible_with(&udt2));
    assert_eq!(udt1.coerce_to_common(&udt2), Some(udt1.clone()));
}

#[test]
fn test_user_defined_types_different_name_incompatible() {
    let udt1 = DataType::UserDefined { type_name: "TypeA".to_string() };
    let udt2 = DataType::UserDefined { type_name: "TypeB".to_string() };
    assert!(!udt1.is_compatible_with(&udt2));
    assert_eq!(udt1.coerce_to_common(&udt2), None);
}

#[test]
fn test_user_defined_type_not_compatible_with_builtin() {
    let udt = DataType::UserDefined { type_name: "MyType".to_string() };
    assert!(!udt.is_compatible_with(&DataType::Integer));
    assert_eq!(udt.coerce_to_common(&DataType::Integer), None);
}

// ----------------------------------------------------------------------------
// Interval Types
// ----------------------------------------------------------------------------

#[test]
fn test_interval_same_fields_compatible() {
    use vibesql_types::IntervalField;

    let i1 = DataType::Interval {
        start_field: IntervalField::Year,
        end_field: Some(IntervalField::Month),
    };
    let i2 = DataType::Interval {
        start_field: IntervalField::Year,
        end_field: Some(IntervalField::Month),
    };

    assert!(i1.is_compatible_with(&i2));
    assert_eq!(i1.coerce_to_common(&i2), Some(i1.clone()));
}

#[test]
fn test_interval_different_fields_compatible() {
    use vibesql_types::IntervalField;

    let i1 = DataType::Interval {
        start_field: IntervalField::Year,
        end_field: Some(IntervalField::Month),
    };
    let i2 = DataType::Interval {
        start_field: IntervalField::Day,
        end_field: Some(IntervalField::Second),
    };

    // Different interval types are compatible (can be coerced)
    assert!(i1.is_compatible_with(&i2));
}

// ----------------------------------------------------------------------------
// Type Precedence Verification
// ----------------------------------------------------------------------------

#[test]
fn test_type_precedence_ordering() {
    // Verify the precedence ordering: VARCHAR > DOUBLE > DECIMAL > INTEGER

    // VARCHAR has highest precedence in numerics/strings mix
    let result = DataType::Integer.coerce_to_common(&DataType::Varchar { max_length: Some(10) });
    assert_eq!(result, None); // No coercion between numbers and strings

    // DOUBLE has precedence over DECIMAL
    let result =
        DataType::Decimal { precision: 10, scale: 2 }.coerce_to_common(&DataType::DoublePrecision);
    assert_eq!(result, Some(DataType::DoublePrecision));

    // DECIMAL has precedence over INTEGER
    let result = DataType::Integer.coerce_to_common(&DataType::Decimal { precision: 10, scale: 2 });
    assert_eq!(result, Some(DataType::Decimal { precision: 10, scale: 2 }));

    // BIGINT has precedence over INTEGER
    let result = DataType::Integer.coerce_to_common(&DataType::Bigint);
    assert_eq!(result, Some(DataType::Bigint));
}

// ----------------------------------------------------------------------------
// Edge Cases
// ----------------------------------------------------------------------------

#[test]
fn test_same_type_coercion_returns_self() {
    assert_eq!(DataType::Integer.coerce_to_common(&DataType::Integer), Some(DataType::Integer));
    assert_eq!(
        DataType::Varchar { max_length: Some(10) }
            .coerce_to_common(&DataType::Varchar { max_length: Some(10) }),
        Some(DataType::Varchar { max_length: Some(10) })
    );
}

#[test]
fn test_null_with_null_coercion() {
    let result = DataType::Null.coerce_to_common(&DataType::Null);
    assert_eq!(result, Some(DataType::Null));
}

#[test]
fn test_unsigned_integer_coercion() {
    // UNSIGNED and BIGINT have same precedence (both 64-bit)
    let result = DataType::Integer.coerce_to_common(&DataType::Unsigned);
    assert_eq!(result, Some(DataType::Unsigned));

    let result = DataType::Unsigned.coerce_to_common(&DataType::Bigint);
    // Should return either (both have same precedence), implementation returns second
    assert!(result == Some(DataType::Unsigned) || result == Some(DataType::Bigint));
}
