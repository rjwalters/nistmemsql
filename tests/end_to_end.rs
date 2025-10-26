//! End-to-end integration tests for the SQL engine.
//!
//! These tests exercise the full pipeline: parse SQL → execute → verify results.

use catalog::{ColumnSchema, TableSchema};
use executor::SelectExecutor;
use parser::Parser;
use storage::{Database, Row};
use types::{DataType, SqlValue};

/// Execute a SELECT query end-to-end: parse SQL → execute → return results.
fn execute_select(db: &Database, sql: &str) -> Result<Vec<Row>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;
    let select_stmt = match stmt {
        ast::Statement::Select(s) => s,
        other => return Err(format!("Expected SELECT statement, got {:?}", other)),
    };

    let executor = SelectExecutor::new(db);
    executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))
}

fn create_users_schema() -> TableSchema {
    TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: 100 }, true),
            ColumnSchema::new("age".to_string(), DataType::Integer, false),
        ],
    )
}

fn insert_sample_users(db: &mut Database) {
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
    ];

    for row in rows {
        db.insert_row("users", row).unwrap();
    }
}

#[test]
fn test_e2e_select_star() {
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    let results = execute_select(&db, "SELECT * FROM users").unwrap();
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].values[0], SqlValue::Integer(1));
    assert_eq!(results[0].values[1], SqlValue::Varchar("Alice".to_string()));
    assert_eq!(results[0].values[2], SqlValue::Integer(25));
}

#[test]
fn test_e2e_select_with_where() {
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    let results = execute_select(&db, "SELECT * FROM users WHERE age >= 18").unwrap();
    assert_eq!(results.len(), 3);
    for row in &results {
        match row.values[2] {
            SqlValue::Integer(age) => assert!(age >= 18),
            _ => panic!("Expected integer age"),
        }
    }
}

#[test]
fn test_e2e_select_specific_columns() {
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    let results = execute_select(&db, "SELECT name, age FROM users").unwrap();
    assert_eq!(results.len(), 4);
    for row in &results {
        assert_eq!(row.values.len(), 2);
    }
    assert_eq!(results[0].values[0], SqlValue::Varchar("Alice".to_string()));
    assert_eq!(results[0].values[1], SqlValue::Integer(25));
}

#[test]
fn test_e2e_select_with_complex_where() {
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    let results =
        execute_select(&db, "SELECT name FROM users WHERE age > 20 AND age < 30").unwrap();
    let names: Vec<String> = results
        .iter()
        .map(|row| match &row.values[0] {
            SqlValue::Varchar(name) => name.clone(),
            _ => panic!("Expected varchar"),
        })
        .collect();

    assert_eq!(names.len(), 2);
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Diana".to_string()));
}

#[test]
fn test_e2e_group_by_count() {
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    let results =
        execute_select(&db, "SELECT age >= 21 AS adult, COUNT(*) FROM users GROUP BY age >= 21")
            .unwrap();

    assert_eq!(results.len(), 2);
    let mut summary = results
        .into_iter()
        .map(|row| match (&row.values[0], &row.values[1]) {
            (SqlValue::Boolean(flag), SqlValue::Integer(count)) => (*flag, *count),
            other => panic!("Unexpected row {:?}", other),
        })
        .collect::<Vec<_>>();
    summary.sort_by_key(|(flag, _)| if *flag { 1 } else { 0 });

    assert_eq!(summary[0], (false, 1));
    assert_eq!(summary[1], (true, 3));
}

#[test]
fn test_e2e_limit_offset() {
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    let results =
        execute_select(&db, "SELECT name FROM users ORDER BY id LIMIT 2 OFFSET 1").unwrap();
    assert_eq!(results.len(), 2);

    let names: Vec<String> = results
        .iter()
        .map(|row| match &row.values[0] {
            SqlValue::Varchar(name) => name.clone(),
            _ => panic!("Expected varchar"),
        })
        .collect();

    assert_eq!(names, vec!["Bob".to_string(), "Charlie".to_string()]);
}
