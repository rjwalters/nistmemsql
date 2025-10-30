//! Tests for advanced scalar functions (math, trigonometry, and conditional functions)

// Allow approximate constants in tests - these are test data values, not mathematical constants
#![allow(clippy::approx_constant)]

mod common;

use common::create_test_evaluator;

// ==================== ADVANCED MATH FUNCTIONS ====================

#[test]
fn test_exp_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "EXP".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Integer(1))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    // e^1 ≈ 2.718281828
    if let types::SqlValue::Double(val) = result {
        assert!((val - 2.718281828).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_ln_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "LN".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Double(2.718281828))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    // ln(e) ≈ 1.0
    if let types::SqlValue::Double(val) = result {
        assert!((val - 1.0).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_log_alias() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "LOG".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Double(2.718281828))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    // LOG is alias for LN
    if let types::SqlValue::Double(val) = result {
        assert!((val - 1.0).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_log10_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "LOG10".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Integer(100))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Double(2.0)); // log10(100) = 2
}

#[test]
fn test_sign_positive() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "SIGN".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Integer(42))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(1));
}

#[test]
fn test_sign_negative() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "SIGN".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Integer(-42))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(-1));
}

#[test]
fn test_sign_zero() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "SIGN".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Integer(0))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(0));
}

#[test]
fn test_pi_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr =
        ast::Expression::Function { name: "PI".to_string(), args: vec![], character_unit: None };
    let result = evaluator.eval(&expr, &row).unwrap();

    if let types::SqlValue::Double(val) = result {
        assert!((val - 3.14159265).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}

// ==================== TRIGONOMETRIC FUNCTIONS ====================

#[test]
fn test_sin_function() {
    let (evaluator, row) = create_test_evaluator();

    // sin(0) = 0
    let expr = ast::Expression::Function {
        name: "SIN".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Integer(0))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Double(0.0));
}

#[test]
fn test_cos_function() {
    let (evaluator, row) = create_test_evaluator();

    // cos(0) = 1
    let expr = ast::Expression::Function {
        name: "COS".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Integer(0))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Double(1.0));
}

#[test]
fn test_tan_function() {
    let (evaluator, row) = create_test_evaluator();

    // tan(0) = 0
    let expr = ast::Expression::Function {
        name: "TAN".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Integer(0))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Double(0.0));
}

#[test]
fn test_asin_function() {
    let (evaluator, row) = create_test_evaluator();

    // asin(0.5) ≈ 0.5236 (30 degrees in radians)
    let expr = ast::Expression::Function {
        name: "ASIN".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Double(0.5))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    if let types::SqlValue::Double(val) = result {
        assert!((val - 0.5236).abs() < 0.001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_acos_function() {
    let (evaluator, row) = create_test_evaluator();

    // acos(0.5) ≈ 1.0472 (60 degrees in radians)
    let expr = ast::Expression::Function {
        name: "ACOS".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Double(0.5))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    if let types::SqlValue::Double(val) = result {
        assert!((val - 1.0472).abs() < 0.001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_atan_function() {
    let (evaluator, row) = create_test_evaluator();

    // atan(1) ≈ 0.7854 (45 degrees in radians)
    let expr = ast::Expression::Function {
        name: "ATAN".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Integer(1))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    if let types::SqlValue::Double(val) = result {
        assert!((val - 0.7854).abs() < 0.001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_atan2_function() {
    let (evaluator, row) = create_test_evaluator();

    // atan2(1, 1) ≈ 0.7854 (45 degrees)
    let expr = ast::Expression::Function {
        name: "ATAN2".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Integer(1)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    if let types::SqlValue::Double(val) = result {
        assert!((val - 0.7854).abs() < 0.001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_radians_function() {
    let (evaluator, row) = create_test_evaluator();

    // 180 degrees = π radians
    let expr = ast::Expression::Function {
        name: "RADIANS".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Integer(180))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    if let types::SqlValue::Double(val) = result {
        assert!((val - std::f64::consts::PI).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_degrees_function() {
    let (evaluator, row) = create_test_evaluator();

    // π radians = 180 degrees
    let expr = ast::Expression::Function {
        name: "DEGREES".to_string(),
        args: vec![ast::Expression::Literal(types::SqlValue::Double(std::f64::consts::PI))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    if let types::SqlValue::Double(val) = result {
        assert!((val - 180.0).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}

// ==================== CONDITIONAL FUNCTIONS ====================

#[test]
fn test_greatest_integers() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "GREATEST".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(5)),
            ast::Expression::Literal(types::SqlValue::Integer(10)),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
            ast::Expression::Literal(types::SqlValue::Integer(7)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(10));
}

#[test]
fn test_greatest_with_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "GREATEST".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(5)),
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Integer(10)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(10)); // NULL is ignored
}

#[test]
fn test_least_integers() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "LEAST".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(5)),
            ast::Expression::Literal(types::SqlValue::Integer(10)),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
            ast::Expression::Literal(types::SqlValue::Integer(7)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(3));
}

#[test]
fn test_least_with_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "LEAST".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(5)),
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(3)); // NULL is ignored
}

#[test]
fn test_if_true_condition() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "IF".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Boolean(true)),
            ast::Expression::Literal(types::SqlValue::Varchar("yes".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("no".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("yes".to_string()));
}

#[test]
fn test_if_false_condition() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "IF".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Boolean(false)),
            ast::Expression::Literal(types::SqlValue::Varchar("yes".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("no".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("no".to_string()));
}

#[test]
fn test_if_null_condition() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "IF".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Varchar("yes".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("no".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("no".to_string())); // NULL treated as false
}

// ==================== COMPLEX NESTED EXPRESSIONS ====================

#[test]
fn test_trig_with_pi() {
    let (evaluator, row) = create_test_evaluator();

    // SIN(PI()) should be approximately 0
    let expr = ast::Expression::Function {
        name: "SIN".to_string(),
        args: vec![ast::Expression::Function {
            name: "PI".to_string(),
            args: vec![],
            character_unit: None,
        }],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    if let types::SqlValue::Double(val) = result {
        assert!(val.abs() < 0.0001); // sin(π) ≈ 0
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_radians_degrees_roundtrip() {
    let (evaluator, row) = create_test_evaluator();

    // DEGREES(RADIANS(90)) should equal 90
    let expr = ast::Expression::Function {
        name: "DEGREES".to_string(),
        args: vec![ast::Expression::Function {
            name: "RADIANS".to_string(),
            args: vec![ast::Expression::Literal(types::SqlValue::Integer(90))],
            character_unit: None,
        }],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    if let types::SqlValue::Double(val) = result {
        assert!((val - 90.0).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_exp_ln_roundtrip() {
    let (evaluator, row) = create_test_evaluator();

    // LN(EXP(2)) should equal 2
    let expr = ast::Expression::Function {
        name: "LN".to_string(),
        args: vec![ast::Expression::Function {
            name: "EXP".to_string(),
            args: vec![ast::Expression::Literal(types::SqlValue::Integer(2))],
            character_unit: None,
        }],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    if let types::SqlValue::Double(val) = result {
        assert!((val - 2.0).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_greatest_with_abs() {
    let (evaluator, row) = create_test_evaluator();

    // GREATEST(ABS(-5), ABS(3)) should be 5
    let expr = ast::Expression::Function {
        name: "GREATEST".to_string(),
        args: vec![
            ast::Expression::Function {
                name: "ABS".to_string(),
                args: vec![ast::Expression::Literal(types::SqlValue::Integer(-5))],
                character_unit: None,
            },
            ast::Expression::Function {
                name: "ABS".to_string(),
                args: vec![ast::Expression::Literal(types::SqlValue::Integer(3))],
                character_unit: None,
            },
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(5));
}
