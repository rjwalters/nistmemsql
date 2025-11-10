//! End-to-end tests for numeric SQL data types.

use vibesql_catalog::ColumnSchema;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

use super::fixtures::*;

#[test]
fn test_e2e_smallint_type() {
    let schema = create_numbers_schema(vec![
        ColumnSchema::new("ID".to_string(), DataType::Integer, false),
        ColumnSchema::new("SMALL_VAL".to_string(), DataType::Smallint, false),
    ]);

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("NUMBERS", Row::new(vec![SqlValue::Integer(1), SqlValue::Smallint(100)]))
        .unwrap();
    db.insert_row("NUMBERS", Row::new(vec![SqlValue::Integer(2), SqlValue::Smallint(-50)]))
        .unwrap();

    let results = execute_select(&db, "SELECT small_val FROM numbers").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Smallint(100));
    assert_eq!(results[1].values[0], SqlValue::Smallint(-50));
}

#[test]
fn test_e2e_bigint_type() {
    let schema = create_numbers_schema(vec![
        ColumnSchema::new("ID".to_string(), DataType::Integer, false),
        ColumnSchema::new("BIG_VAL".to_string(), DataType::Bigint, false),
    ]);

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row(
        "NUMBERS",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Bigint(9_223_372_036_854_775_807)]),
    )
    .unwrap();
    db.insert_row(
        "NUMBERS",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Bigint(-9_223_372_036_854_775_808)]),
    )
    .unwrap();

    let results = execute_select(&db, "SELECT big_val FROM numbers").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Bigint(9_223_372_036_854_775_807));
    assert_eq!(results[1].values[0], SqlValue::Bigint(-9_223_372_036_854_775_808));
}

#[test]
fn test_e2e_float_type() {
    let schema = create_measurements_schema(vec![
        ColumnSchema::new("ID".to_string(), DataType::Integer, false),
        ColumnSchema::new("VALUE".to_string(), DataType::Float { precision: 53 }, false),
    ]);

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("MEASUREMENTS", Row::new(vec![SqlValue::Integer(1), SqlValue::Float(3.14)]))
        .unwrap();
    db.insert_row("MEASUREMENTS", Row::new(vec![SqlValue::Integer(2), SqlValue::Float(-2.71)]))
        .unwrap();

    let results = execute_select(&db, "SELECT value FROM measurements").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Float(3.14));
    assert_eq!(results[1].values[0], SqlValue::Float(-2.71));
}

#[test]
fn test_e2e_real_type() {
    let schema = create_measurements_schema(vec![
        ColumnSchema::new("ID".to_string(), DataType::Integer, false),
        ColumnSchema::new("VALUE".to_string(), DataType::Real, false),
    ]);

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("MEASUREMENTS", Row::new(vec![SqlValue::Integer(1), SqlValue::Real(1.23)]))
        .unwrap();
    db.insert_row("MEASUREMENTS", Row::new(vec![SqlValue::Integer(2), SqlValue::Real(4.56)]))
        .unwrap();

    let results = execute_select(&db, "SELECT value FROM measurements").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Real(1.23));
    assert_eq!(results[1].values[0], SqlValue::Real(4.56));
}

#[test]
fn test_e2e_double_precision_type() {
    let schema = create_measurements_schema(vec![
        ColumnSchema::new("ID".to_string(), DataType::Integer, false),
        ColumnSchema::new("VALUE".to_string(), DataType::DoublePrecision, false),
    ]);

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row(
        "MEASUREMENTS",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Double(3.141592653589793)]),
    )
    .unwrap();
    db.insert_row(
        "MEASUREMENTS",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Double(2.718281828459045)]),
    )
    .unwrap();

    let results = execute_select(&db, "SELECT value FROM measurements").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Double(3.141592653589793));
    assert_eq!(results[1].values[0], SqlValue::Double(2.718281828459045));
}

#[test]
fn test_e2e_numeric_type() {
    let schema = create_financials_schema();
    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("FINANCIALS", Row::new(vec![SqlValue::Integer(1), SqlValue::Numeric(123.45)]))
        .unwrap();
    db.insert_row("FINANCIALS", Row::new(vec![SqlValue::Integer(2), SqlValue::Numeric(999.99)]))
        .unwrap();

    let results = execute_select(&db, "SELECT amount FROM financials").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Numeric(123.45));
    assert_eq!(results[1].values[0], SqlValue::Numeric(999.99));
}

#[test]
fn test_e2e_decimal_type() {
    let schema = create_financials_schema();
    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("FINANCIALS", Row::new(vec![SqlValue::Integer(1), SqlValue::Numeric(19.99)]))
        .unwrap();
    db.insert_row("FINANCIALS", Row::new(vec![SqlValue::Integer(2), SqlValue::Numeric(49.95)]))
        .unwrap();

    let results = execute_select(&db, "SELECT amount FROM financials").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Numeric(19.99));
    assert_eq!(results[1].values[0], SqlValue::Numeric(49.95));
}

#[test]
fn test_e2e_all_numeric_types_together() {
    let schema = create_numbers_schema(vec![
        ColumnSchema::new("ID".to_string(), DataType::Integer, false),
        ColumnSchema::new("S".to_string(), DataType::Smallint, false),
        ColumnSchema::new("B".to_string(), DataType::Bigint, false),
        ColumnSchema::new("F".to_string(), DataType::Float { precision: 53 }, false),
        ColumnSchema::new("R".to_string(), DataType::Real, false),
        ColumnSchema::new("D".to_string(), DataType::DoublePrecision, false),
    ]);

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row(
        "NUMBERS",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Smallint(100),
            SqlValue::Bigint(1000000),
            SqlValue::Float(3.14),
            SqlValue::Real(2.71),
            SqlValue::Double(1.41),
        ]),
    )
    .unwrap();

    let results = execute_select(&db, "SELECT * FROM numbers").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values.len(), 6);
}

#[test]
fn test_e2e_numeric_comparison() {
    let schema = create_numbers_schema(vec![
        ColumnSchema::new("ID".to_string(), DataType::Integer, false),
        ColumnSchema::new("SMALL_VAL".to_string(), DataType::Smallint, false),
    ]);

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("NUMBERS", Row::new(vec![SqlValue::Integer(1), SqlValue::Smallint(10)])).unwrap();
    db.insert_row("NUMBERS", Row::new(vec![SqlValue::Integer(2), SqlValue::Smallint(20)])).unwrap();
    db.insert_row("NUMBERS", Row::new(vec![SqlValue::Integer(3), SqlValue::Smallint(30)])).unwrap();

    let results =
        execute_select(&db, "SELECT small_val FROM numbers WHERE small_val >= 20").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Smallint(20));
    assert_eq!(results[1].values[0], SqlValue::Smallint(30));
}
