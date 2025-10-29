use types::*;

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

    // ============================================================================
    // SqlValue Tests - Test SQL value representation
    // ============================================================================

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

    // ============================================================================
    // SqlValue::is_null() Tests - Check if value is NULL
    // ============================================================================

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

    // ============================================================================
    // SqlValue::get_type() Tests - Get the type of a value
    // ============================================================================

    #[test]
    fn test_integer_value_has_integer_type() {
        let value = SqlValue::Integer(42);
        assert_eq!(value.get_type(), DataType::Integer);
    }

    #[test]
    fn test_varchar_value_has_varchar_type() {
        let value = SqlValue::Varchar("test".to_string());
        // Note: VARCHAR values don't have a specific max_length in the value itself
        // We'll handle this properly when we implement it
        match value.get_type() {
            DataType::Varchar { .. } => {} // Success
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

    // ============================================================================
    // Type Compatibility Tests - Can we assign/compare values?
    // ============================================================================

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
        // VARCHAR(10) and VARCHAR(20) should be compatible for comparison
        let v1 = DataType::Varchar { max_length: 10 };
        let v2 = DataType::Varchar { max_length: 20 };
        assert!(v1.is_compatible_with(&v2));
    }

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

    // ============================================================================
    // PartialOrd Tests for SqlValue
    // ============================================================================

    #[test]
    fn test_integer_ordering() {
        assert!(SqlValue::Integer(1) < SqlValue::Integer(2));
        assert!(SqlValue::Integer(2) > SqlValue::Integer(1));
        assert_eq!(
            SqlValue::Integer(1).partial_cmp(&SqlValue::Integer(1)),
            Some(std::cmp::Ordering::Equal)
        );
    }

    #[test]
    fn test_smallint_ordering() {
        assert!(SqlValue::Smallint(10) < SqlValue::Smallint(20));
        assert!(SqlValue::Smallint(20) > SqlValue::Smallint(10));
    }

    #[test]
    fn test_bigint_ordering() {
        assert!(SqlValue::Bigint(1000) < SqlValue::Bigint(2000));
        assert!(SqlValue::Bigint(2000) > SqlValue::Bigint(1000));
    }

    #[test]
    fn test_float_ordering() {
        assert!(SqlValue::Float(1.5) < SqlValue::Float(2.5));
        assert!(SqlValue::Float(2.5) > SqlValue::Float(1.5));
    }

    #[test]
    fn test_float_nan_is_incomparable() {
        let nan = SqlValue::Float(f32::NAN);
        let one = SqlValue::Float(1.0);
        assert_eq!(nan.partial_cmp(&one), None);
        assert_eq!(one.partial_cmp(&nan), None);
        assert_eq!(nan.partial_cmp(&nan), None);
    }

    #[test]
    fn test_double_ordering() {
        assert!(SqlValue::Double(1.5) < SqlValue::Double(2.5));
        assert!(SqlValue::Double(2.5) > SqlValue::Double(1.5));
    }

    #[test]
    fn test_double_nan_is_incomparable() {
        let nan = SqlValue::Double(f64::NAN);
        let one = SqlValue::Double(1.0);
        assert_eq!(nan.partial_cmp(&one), None);
        assert_eq!(one.partial_cmp(&nan), None);
    }

    #[test]
    fn test_varchar_ordering() {
        assert!(SqlValue::Varchar("apple".to_string()) < SqlValue::Varchar("banana".to_string()));
        assert!(SqlValue::Varchar("zebra".to_string()) > SqlValue::Varchar("aardvark".to_string()));
    }

    #[test]
    fn test_character_ordering() {
        assert!(SqlValue::Character("a".to_string()) < SqlValue::Character("b".to_string()));
        assert!(SqlValue::Character("z".to_string()) > SqlValue::Character("a".to_string()));
    }

    #[test]
    fn test_boolean_ordering() {
        // In SQL, FALSE < TRUE
        assert!(SqlValue::Boolean(false) < SqlValue::Boolean(true));
        assert!(SqlValue::Boolean(true) > SqlValue::Boolean(false));
        assert_eq!(
            SqlValue::Boolean(true).partial_cmp(&SqlValue::Boolean(true)),
            Some(std::cmp::Ordering::Equal)
        );
    }

    #[test]
    fn test_numeric_ordering() {
        assert!(SqlValue::Numeric("1.5".to_string()) < SqlValue::Numeric("2.5".to_string()));
        assert!(SqlValue::Numeric("100.0".to_string()) > SqlValue::Numeric("50.5".to_string()));
    }

    #[test]
    fn test_numeric_invalid_is_incomparable() {
        let invalid = SqlValue::Numeric("not-a-number".to_string());
        let valid = SqlValue::Numeric("1.0".to_string());
        assert_eq!(invalid.partial_cmp(&valid), None);
        assert_eq!(valid.partial_cmp(&invalid), None);
    }

    #[test]
    fn test_date_ordering() {
        assert!(
            SqlValue::Date("2024-01-01".to_string()) < SqlValue::Date("2024-12-31".to_string())
        );
        assert!(
            SqlValue::Date("2024-12-31".to_string()) > SqlValue::Date("2024-01-01".to_string())
        );
    }

    #[test]
    fn test_time_ordering() {
        assert!(SqlValue::Time("09:00:00".to_string()) < SqlValue::Time("17:00:00".to_string()));
        assert!(SqlValue::Time("17:00:00".to_string()) > SqlValue::Time("09:00:00".to_string()));
    }

    #[test]
    fn test_timestamp_ordering() {
        assert!(
            SqlValue::Timestamp("2024-01-01 09:00:00".to_string())
                < SqlValue::Timestamp("2024-01-01 17:00:00".to_string())
        );
    }

    #[test]
    fn test_null_is_incomparable() {
        // NULL compared to anything (including NULL) returns None
        assert_eq!(SqlValue::Null.partial_cmp(&SqlValue::Integer(1)), None);
        assert_eq!(SqlValue::Integer(1).partial_cmp(&SqlValue::Null), None);
        assert_eq!(SqlValue::Null.partial_cmp(&SqlValue::Null), None);
    }

    #[test]
    fn test_type_mismatch_is_incomparable() {
        // Different types cannot be compared
        assert_eq!(SqlValue::Integer(1).partial_cmp(&SqlValue::Varchar("1".to_string())), None);
        assert_eq!(SqlValue::Float(1.0).partial_cmp(&SqlValue::Integer(1)), None);
        assert_eq!(SqlValue::Boolean(true).partial_cmp(&SqlValue::Integer(1)), None);
    }

    #[test]
    fn test_can_use_comparison_operators() {
        // Test that Rust's comparison operators work with PartialOrd
        let a = SqlValue::Integer(1);
        let b = SqlValue::Integer(2);

        assert!(a < b);
        assert!(b > a);
        assert!(a <= b);
        assert!(b >= a);
        assert!(a == a);
        assert!(a != b);
    }

    // ============================================================================
    // Additional Type Compatibility Tests
    // ============================================================================

    #[test]
    fn test_boolean_compatible_with_boolean() {
        assert!(DataType::Boolean.is_compatible_with(&DataType::Boolean));
    }

    #[test]
    fn test_date_compatible_with_date() {
        assert!(DataType::Date.is_compatible_with(&DataType::Date));
    }

    // ============================================================================
    // Additional get_type() Tests
    // ============================================================================

    #[test]
    fn test_smallint_value_has_smallint_type() {
        let value = SqlValue::Smallint(42);
        assert_eq!(value.get_type(), DataType::Smallint);
    }

    #[test]
    fn test_bigint_value_has_bigint_type() {
        let value = SqlValue::Bigint(1000);
        assert_eq!(value.get_type(), DataType::Bigint);
    }

    #[test]
    fn test_numeric_value_has_numeric_type() {
        let value = SqlValue::Numeric("123.45".to_string());
        match value.get_type() {
            DataType::Numeric { .. } => {} // Success
            _ => panic!("Expected Numeric type"),
        }
    }

    #[test]
    fn test_float_value_has_float_type() {
        let value = SqlValue::Float(2.5);
        assert_eq!(value.get_type(), DataType::Float { precision: 53 });
    }

    #[test]
    fn test_real_value_has_real_type() {
        let value = SqlValue::Real(2.71);
        assert_eq!(value.get_type(), DataType::Real);
    }

    #[test]
    fn test_double_value_has_double_type() {
        let value = SqlValue::Double(123.456);
        assert_eq!(value.get_type(), DataType::DoublePrecision);
    }

    #[test]
    fn test_character_value_has_character_type() {
        let value = SqlValue::Character("hello".to_string());
        match value.get_type() {
            DataType::Character { .. } => {} // Success
            _ => panic!("Expected Character type"),
        }
    }

    #[test]
    fn test_date_value_has_date_type() {
        let value = SqlValue::Date("2024-01-01".to_string());
        assert_eq!(value.get_type(), DataType::Date);
    }

    #[test]
    fn test_time_value_has_time_type() {
        let value = SqlValue::Time("12:30:00".to_string());
        match value.get_type() {
            DataType::Time { .. } => {} // Success
            _ => panic!("Expected Time type"),
        }
    }

    #[test]
    fn test_timestamp_value_has_timestamp_type() {
        let value = SqlValue::Timestamp("2024-01-01 12:30:00".to_string());
        match value.get_type() {
            DataType::Timestamp { .. } => {} // Success
            _ => panic!("Expected Timestamp type"),
        }
    }

    // ============================================================================
    // Additional Display Tests
    // ============================================================================

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

    // ============================================================================
    // Real Type Comparison Tests
    // ============================================================================

    #[test]
    fn test_real_ordering() {
        assert!(SqlValue::Real(1.5) < SqlValue::Real(2.5));
        assert!(SqlValue::Real(2.5) > SqlValue::Real(1.5));
    }

    #[test]
    fn test_real_nan_is_incomparable() {
        let nan = SqlValue::Real(f32::NAN);
        let one = SqlValue::Real(1.0);
        assert_eq!(nan.partial_cmp(&one), None);
        assert_eq!(one.partial_cmp(&nan), None);
    }

    // ============================================================================
    // Hash Implementation Tests (for DISTINCT operations)
    // ============================================================================

    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    fn calculate_hash<T: Hash>(value: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn test_integer_hash() {
        let v1 = SqlValue::Integer(42);
        let v2 = SqlValue::Integer(42);
        let v3 = SqlValue::Integer(43);
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
        assert_ne!(calculate_hash(&v1), calculate_hash(&v3));
    }

    #[test]
    fn test_smallint_hash() {
        let v1 = SqlValue::Smallint(10);
        let v2 = SqlValue::Smallint(10);
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    }

    #[test]
    fn test_bigint_hash() {
        let v1 = SqlValue::Bigint(1000);
        let v2 = SqlValue::Bigint(1000);
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    }

    #[test]
    fn test_numeric_hash() {
        let v1 = SqlValue::Numeric("123.45".to_string());
        let v2 = SqlValue::Numeric("123.45".to_string());
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    }

    #[test]
    fn test_float_hash() {
        let v1 = SqlValue::Float(2.5);
        let v2 = SqlValue::Float(2.5);
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    }

    #[test]
    fn test_float_nan_hash() {
        // NaN values should hash to the same value for DISTINCT operations
        let nan1 = SqlValue::Float(f32::NAN);
        let nan2 = SqlValue::Float(f32::NAN);
        assert_eq!(calculate_hash(&nan1), calculate_hash(&nan2));
    }

    #[test]
    fn test_real_hash() {
        let v1 = SqlValue::Real(2.71);
        let v2 = SqlValue::Real(2.71);
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    }

    #[test]
    fn test_real_nan_hash() {
        let nan1 = SqlValue::Real(f32::NAN);
        let nan2 = SqlValue::Real(f32::NAN);
        assert_eq!(calculate_hash(&nan1), calculate_hash(&nan2));
    }

    #[test]
    fn test_double_hash() {
        let v1 = SqlValue::Double(123.456);
        let v2 = SqlValue::Double(123.456);
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    }

    #[test]
    fn test_double_nan_hash() {
        let nan1 = SqlValue::Double(f64::NAN);
        let nan2 = SqlValue::Double(f64::NAN);
        assert_eq!(calculate_hash(&nan1), calculate_hash(&nan2));
    }

    #[test]
    fn test_varchar_hash() {
        let v1 = SqlValue::Varchar("hello".to_string());
        let v2 = SqlValue::Varchar("hello".to_string());
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    }

    #[test]
    fn test_character_hash() {
        let v1 = SqlValue::Character("test".to_string());
        let v2 = SqlValue::Character("test".to_string());
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    }

    #[test]
    fn test_boolean_hash() {
        let v1 = SqlValue::Boolean(true);
        let v2 = SqlValue::Boolean(true);
        let v3 = SqlValue::Boolean(false);
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
        assert_ne!(calculate_hash(&v1), calculate_hash(&v3));
    }

    #[test]
    fn test_date_hash() {
        let v1 = SqlValue::Date("2024-01-01".to_string());
        let v2 = SqlValue::Date("2024-01-01".to_string());
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    }

    #[test]
    fn test_time_hash() {
        let v1 = SqlValue::Time("12:30:00".to_string());
        let v2 = SqlValue::Time("12:30:00".to_string());
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    }

    #[test]
    fn test_timestamp_hash() {
        let v1 = SqlValue::Timestamp("2024-01-01 12:30:00".to_string());
        let v2 = SqlValue::Timestamp("2024-01-01 12:30:00".to_string());
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    }

    #[test]
    fn test_null_hash() {
        let v1 = SqlValue::Null;
        let v2 = SqlValue::Null;
        // NULL values should hash consistently
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    }

    // ============================================================================
    // Edge Case Tests
    // ============================================================================

    #[test]
    fn test_empty_string_varchar() {
        let value = SqlValue::Varchar("".to_string());
        assert_eq!(format!("{}", value), "");
        assert!(!value.is_null());
    }

    #[test]
    fn test_negative_integer() {
        let value = SqlValue::Integer(-42);
        assert_eq!(format!("{}", value), "-42");
    }

    #[test]
    fn test_very_large_bigint() {
        let value = SqlValue::Bigint(i64::MAX);
        assert_eq!(format!("{}", value), format!("{}", i64::MAX));
    }

    #[test]
    fn test_very_small_bigint() {
        let value = SqlValue::Bigint(i64::MIN);
        assert_eq!(format!("{}", value), format!("{}", i64::MIN));
    }

    #[test]
    fn test_special_characters_in_varchar() {
        let value = SqlValue::Varchar("Hello, 世界! 🌍".to_string());
        assert_eq!(format!("{}", value), "Hello, 世界! 🌍");
    }
