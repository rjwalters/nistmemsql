//! Test for issue #947: Decimal formatting with .000
//!
//! SQLLogicTest expects arithmetic results to display with 3 decimal places (e.g., "92.000")
//! instead of as plain integers (e.g., "92").

use catalog::{ColumnSchema, TableSchema};
use executor::SelectExecutor;
use parser::Parser;
use storage::{Database, Row};
use types::{DataType, SqlValue};

/// Execute a SELECT query end-to-end
fn execute_select(db: &Database, sql: &str) -> Result<Vec<Row>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;
    let select_stmt = match stmt {
        ast::Statement::Select(s) => s,
        other => return Err(format!("Expected SELECT statement, got {:?}", other)),
    };

    let executor = SelectExecutor::new(db);
    executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))
}

#[test]
fn test_arithmetic_decimal_formatting() {
    let mut db = Database::new();

    // Setup test table
    let schema = TableSchema::new(
        "TAB1".to_string(),
        vec![ColumnSchema::new("ID".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "TAB1",
        Row::new(vec![SqlValue::Integer(1)]),
    ).unwrap();
    db.insert_row(
        "TAB1",
        Row::new(vec![SqlValue::Integer(2)]),
    ).unwrap();
    db.insert_row(
        "TAB1",
        Row::new(vec![SqlValue::Integer(3)]),
    ).unwrap();

    // Test 1: Complex unary operations - the original failing query
    let rows = execute_select(&db, "SELECT + ( + - ( - 92 ) ) FROM TAB1").unwrap();
    assert_eq!(rows.len(), 3, "Should return 3 rows");

    // Check that the values are formatted as "92.000" not "92"
    for row in &rows {
        let value_str = format!("{}", row.values[0]);
        assert_eq!(value_str, "92.000", "Expected '92.000' but got '{}'", value_str);
    }

    // Test 2: Simple arithmetic operations should also return DECIMAL format
    let test_cases = vec![
        ("SELECT 92 + 0 FROM TAB1", "92.000"),
        ("SELECT 100 - 8 FROM TAB1", "92.000"),
        ("SELECT 46 * 2 FROM TAB1", "92.000"),
        ("SELECT +(92) FROM TAB1", "92.000"),
        ("SELECT -(-(92)) FROM TAB1", "92.000"),
        ("SELECT 5 + 3 FROM TAB1", "8.000"),
        ("SELECT 10 - 2 FROM TAB1", "8.000"),
        ("SELECT 4 * 2 FROM TAB1", "8.000"),
    ];

    for (query, expected) in test_cases {
        let rows = execute_select(&db, query).unwrap();
        assert_eq!(rows.len(), 3, "Query '{}' should return 3 rows", query);

        for row in &rows {
            let value_str = format!("{}", row.values[0]);
            assert_eq!(value_str, expected,
                "Query '{}': Expected '{}' but got '{}'", query, expected, value_str);
        }
    }
}

#[test]
fn test_numeric_display_format() {
    // Test that SqlValue::Numeric displays with 3 decimal places
    use types::SqlValue;

    let test_cases = vec![
        (SqlValue::Numeric(92.0), "92.000"),
        (SqlValue::Numeric(0.0), "0.000"),
        (SqlValue::Numeric(-92.0), "-92.000"),
        (SqlValue::Numeric(123.456), "123.456"),
        (SqlValue::Numeric(100.5), "100.500"),
        (SqlValue::Numeric(1.0), "1.000"),
    ];

    for (value, expected) in test_cases {
        let formatted = format!("{}", value);
        assert_eq!(formatted, expected,
            "Expected '{}' but got '{}' for {:?}", expected, formatted, value);
    }
}

#[test]
fn test_integer_arithmetic_values_display_as_decimal() {
    // Test that arithmetic operations on integers display with decimals
    // Note: Raw integer literals like "SELECT 92" remain as integers
    // Only arithmetic expressions produce DECIMAL format
    let db = Database::new();

    let test_cases = vec![
        ("SELECT +(92)", "92.000"),
        ("SELECT -(92)", "-92.000"),
        ("SELECT 5 + 3", "8.000"),
        ("SELECT 10 - 2", "8.000"),
        ("SELECT 2 * 3", "6.000"),
    ];

    for (query, expected) in test_cases {
        let rows = execute_select(&db, query).unwrap();
        assert_eq!(rows.len(), 1, "Query '{}' should return 1 row", query);

        let value_str = format!("{}", rows[0].values[0]);
        assert_eq!(value_str, expected,
            "Query '{}': Expected '{}' but got '{}'", query, expected, value_str);
    }
}
