//! Edge case tests for numeric functions
//!
//! This module tests numeric functions with edge cases including:
//! - NULL handling
//! - Overflow/underflow conditions
//! - Domain errors (negative sqrt, log of zero/negative)
//! - Type coercion and precision issues
//! - Boundary values (MIN_INT, MAX_INT, etc.)

mod common;

use common::create_test_evaluator;
use types::SqlValue;

// ==================== BASIC FUNCTIONS EDGE CASES ====================

#[test]
fn test_abs_min_int() {
    let (evaluator, row) = create_test_evaluator();

    // Test ABS of i32::MIN (should overflow but let's see what happens)
    let expr = ast::Expression::Function {
        name: "ABS".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Integer(i32::MIN as i64))],
        character_unit: None,
    };

    // This might panic or error - depending on implementation
    let result = evaluator.eval(&expr, &row);
    match result {
        Ok(_) => println!("ABS(i32::MIN) succeeded"),
        Err(e) => println!("ABS(i32::MIN) failed: {:?}", e),
    }
}

#[test]
fn test_abs_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "ABS".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_sign_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "SIGN".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_sign_float_zero() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "SIGN".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Double(-0.0))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Double(0.0));
}

#[test]
fn test_mod_by_zero() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "MOD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(10)),
            ast::Expression::Literal(types::SqlValue::Integer(0)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err()); // Should return DivisionByZero error
}

#[test]
fn test_mod_with_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "MOD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(10)),
            ast::Expression::Literal(types::SqlValue::Null),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_mod_negative_divisor() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "MOD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(10)),
            ast::Expression::Literal(types::SqlValue::Integer(-3)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(10 % -3));
}

// ==================== ROUNDING FUNCTIONS EDGE CASES ====================

#[test]
fn test_round_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "ROUND".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_round_with_null_precision() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "ROUND".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Double(3.14159)),
            ast::Expression::Literal(types::SqlValue::Null),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_round_negative_precision() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "ROUND".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Double(123.456)),
            ast::Expression::Literal(types::SqlValue::Integer(-1)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    // 123.456 rounded to -1 decimal places = 120
    if let types::SqlValue::Double(val) = result {
        assert!((val - 120.0).abs() < 0.001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_floor_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "FLOOR".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_floor_negative() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "FLOOR".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Double(-1.5))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    if let types::SqlValue::Double(val) = result {
        assert!((val - (-2.0)).abs() < 0.001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_ceil_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "CEIL".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_ceil_negative() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "CEIL".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Double(-1.5))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    if let types::SqlValue::Double(val) = result {
        assert!((val - (-1.0)).abs() < 0.001);
    } else {
        panic!("Expected Double value");
    }
}

// ==================== EXPONENTIAL FUNCTIONS EDGE CASES ====================

#[test]
fn test_power_zero_to_zero() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "POWER".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(0)),
            ast::Expression::Literal(types::SqlValue::Integer(0)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    // 0^0 is typically defined as 1 in SQL
    if let types::SqlValue::Double(val) = result {
        assert!((val - 1.0).abs() < 0.001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_power_negative_base() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "POWER".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(-2)),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    if let types::SqlValue::Double(val) = result {
        assert!((val - (-8.0)).abs() < 0.001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_power_with_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "POWER".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(2)),
            ast::Expression::Literal(types::SqlValue::Null),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_sqrt_negative() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "SQRT".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Integer(-1))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err()); // Should return error for negative sqrt
}

#[test]
fn test_sqrt_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "SQRT".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_exp_overflow() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "EXP".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Double(1000.0))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    // Should return infinity
    if let types::SqlValue::Double(val) = result {
        assert!(val.is_infinite() && val.is_sign_positive());
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_exp_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "EXP".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_ln_zero() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "LN".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Integer(0))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err()); // LN of zero should error
}

#[test]
fn test_ln_negative() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "LN".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Integer(-1))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err()); // LN of negative should error
}

#[test]
fn test_ln_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "LN".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_log10_zero() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "LOG10".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Integer(0))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err()); // LOG10 of zero should error
}

#[test]
fn test_log10_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "LOG10".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

// ==================== TRIGONOMETRIC FUNCTIONS EDGE CASES ====================

#[test]
fn test_sin_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "SIN".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_asin_out_of_range() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "ASIN".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Double(2.0))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err()); // ASIN requires -1 <= x <= 1
}

#[test]
fn test_acos_out_of_range() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "ACOS".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Double(-2.0))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err()); // ACOS requires -1 <= x <= 1
}

#[test]
fn test_atan2_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "ATAN2".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Double(1.0)),
            ast::Expression::Literal(types::SqlValue::Null),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_radians_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "RADIANS".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_degrees_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DEGREES".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

// ==================== TYPE COERCION TESTS ====================

#[test]
fn test_abs_type_coercion() {
    let (evaluator, row) = create_test_evaluator();

    // Test ABS with different numeric types
    let types_to_test = vec![
        types::SqlValue::Smallint(42),
        types::SqlValue::Integer(42),
        types::SqlValue::Bigint(42),
        types::SqlValue::Float(42.0),
        types::SqlValue::Double(42.0),
        types::SqlValue::Real(42.0),
    ];

    for value in types_to_test {
        let expr = ast::Expression::Function {
            name: "ABS".to_string(),
            args: vec![ast::Expression::Literal(value.clone())],
            character_unit: None,
        };
        let result = evaluator.eval(&expr, &row).unwrap();
        // Result should be same type as input
        assert_eq!(result, value);
    }
}

#[test]
fn test_power_type_mixing() {
    let (evaluator, row) = create_test_evaluator();

    // Test POWER with mixed types
    let expr = ast::Expression::Function {
        name: "POWER".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(2)),
            ast::Expression::Literal(types::SqlValue::Double(3.0)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    if let types::SqlValue::Double(val) = result {
        assert!((val - 8.0).abs() < 0.001);
    } else {
        panic!("Expected Double value");
    }
}

// ==================== DOMAIN AND RANGE ERRORS ====================

#[test]
fn test_trig_domain_errors() {
    let (evaluator, row) = create_test_evaluator();

    // Test various domain errors
    let test_cases = vec![
        ("ASIN", vec![types::SqlValue::Double(1.1)]),
        ("ASIN", vec![types::SqlValue::Double(-1.1)]),
        ("ACOS", vec![types::SqlValue::Double(1.1)]),
        ("ACOS", vec![types::SqlValue::Double(-1.1)]),
    ];

    for (func_name, args) in test_cases {
        let expr = ast::Expression::Function {
            name: func_name.to_string(),
            args: args.into_iter().map(|v| ast::Expression::Literal(v)).collect(),
            character_unit: None,
        };
        let result = evaluator.eval(&expr, &row);
        assert!(result.is_err(), "Function {} should have domain error", func_name);
    }
}

// ==================== PRECISION AND ACCURACY TESTS ====================

#[test]
fn test_round_precision_edge_cases() {
    let (evaluator, row) = create_test_evaluator();

    // Test rounding at precision boundaries
    let test_cases = vec![
        (1.23456789, 2, 1.23),
        (1.23456789, 0, 1.0),
        (1.99999999, 2, 2.00),
        (-1.23456789, 2, -1.23),
    ];

    for (input, precision, expected) in test_cases {
        let expr = ast::Expression::Function {
            name: "ROUND".to_string(),
            args: vec![
                ast::Expression::Literal(types::SqlValue::Double(input)),
                ast::Expression::Literal(types::SqlValue::Integer(precision)),
            ],
            character_unit: None,
        };
        let result = evaluator.eval(&expr, &row).unwrap();

        if let types::SqlValue::Double(val) = result {
            assert!(
                (val - expected).abs() < 0.001,
                "ROUND({}, {}) = {} but expected {}",
                input,
                precision,
                val,
                expected
            );
        } else {
            panic!("Expected Double value");
        }
    }
}

#[test]
fn test_floating_point_precision() {
    let (evaluator, row) = create_test_evaluator();

    // Test operations that might lose precision
    let expr = ast::Expression::Function {
        name: "SQRT".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Double(2.0))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    if let types::SqlValue::Double(val) = result {
        let squared = val * val;
        // sqrt(2)^2 should be very close to 2
        assert!((squared - 2.0).abs() < 0.0000001);
    } else {
        panic!("Expected Double value");
    }
}
