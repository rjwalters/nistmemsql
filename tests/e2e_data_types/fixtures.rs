//! Shared fixtures for end-to-end data type tests.

use catalog::{ColumnSchema, TableSchema};
use executor::SelectExecutor;
use parser::Parser;
use storage::{Database, Row};
use types::{DataType, SqlValue};

// ========================================================================
// Shared Test Utilities
// ========================================================================

/// Execute a SELECT query end-to-end: parse SQL → execute → return results.
pub fn execute_select(db: &Database, sql: &str) -> Result<Vec<Row>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;
    let select_stmt = match stmt {
        ast::Statement::Select(s) => s,
        other => return Err(format!("Expected SELECT statement, got {:?}", other)),
    };

    let executor = SelectExecutor::new(db);
    executor
        .execute(&select_stmt)
        .map_err(|e| format!("Execution error: {:?}", e))
}

// ========================================================================
// Schema and Data Fixtures
// ========================================================================

pub fn create_users_schema() -> TableSchema {
    TableSchema::new(
        "USERS".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
            ColumnSchema::new("AGE".to_string(), DataType::Integer, false),
        ],
    )
}

pub fn insert_sample_users(db: &mut Database) {
    let rows = vec![
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Integer(25),
        ]),
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Integer(17),
        ]),
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Charlie".to_string()),
            SqlValue::Integer(30),
        ]),
        Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar("Diana".to_string()),
            SqlValue::Integer(22),
        ]),
        Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Varchar("Eve".to_string()),
            SqlValue::Integer(35),
        ]),
    ];

    for row in rows {
        db.insert_row("USERS", row).unwrap();
    }
}

pub fn create_numbers_schema(columns: Vec<ColumnSchema>) -> TableSchema {
    TableSchema::new("NUMBERS".to_string(), columns)
}

pub fn create_measurements_schema(columns: Vec<ColumnSchema>) -> TableSchema {
    TableSchema::new("MEASUREMENTS".to_string(), columns)
}

pub fn create_financials_schema() -> TableSchema {
    TableSchema::new(
        "FINANCIALS".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "AMOUNT".to_string(),
                DataType::Numeric { precision: 10, scale: 2 },
                false,
            ),
        ],
    )
}

pub fn create_codes_schema() -> TableSchema {
    TableSchema::new(
        "CODES".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "CODE".to_string(),
                DataType::Character { length: 5 },
                false,
            ),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Character { length: 10 },
                false,
            ),
        ],
    )
}
