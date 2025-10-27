use catalog::{ColumnSchema, TableSchema};
use executor::SelectExecutor;
use parser::Parser;
use storage::{Database, Row};
use types::{DataType, SqlValue};

fn execute_query(db: &Database, query: &str) -> Result<Vec<Row>, String> {
    let stmt = Parser::parse_sql(query).map_err(|e| format!("Parse error: {:?}", e))?;
    let select_stmt = match stmt {
        ast::Statement::Select(s) => s,
        other => return Err(format!("Expected SELECT statement, got {:?}", other)),
    };

    let executor = SelectExecutor::new(db);
    executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))
}

// Helper to create a dummy table with one row for testing functions without FROM
fn create_dummy_table(db: &mut Database) {
    let schema = TableSchema::new(
        "dual".to_string(),
        vec![ColumnSchema::new(
            "dummy".to_string(),
            DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "dual",
        Row::new(vec![SqlValue::Integer(1)]),
    )
    .unwrap();
}

#[test]
fn test_upper_basic() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT UPPER('hello') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].values[0],
        SqlValue::Varchar("HELLO".to_string())
    );
}

#[test]
fn test_upper_already_uppercase() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT UPPER('WORLD') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].values[0],
        SqlValue::Varchar("WORLD".to_string())
    );
}

#[test]
fn test_upper_mixed_case() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT UPPER('HeLLo WoRLd') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].values[0],
        SqlValue::Varchar("HELLO WORLD".to_string())
    );
}

#[test]
fn test_upper_with_numbers() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT UPPER('test123') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].values[0],
        SqlValue::Varchar("TEST123".to_string())
    );
}

#[test]
fn test_lower_basic() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT LOWER('HELLO') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].values[0],
        SqlValue::Varchar("hello".to_string())
    );
}

#[test]
fn test_lower_already_lowercase() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT LOWER('world') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].values[0],
        SqlValue::Varchar("world".to_string())
    );
}

#[test]
fn test_lower_mixed_case() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT LOWER('HeLLo WoRLd') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].values[0],
        SqlValue::Varchar("hello world".to_string())
    );
}

#[test]
fn test_lower_with_numbers() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT LOWER('TEST123') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].values[0],
        SqlValue::Varchar("test123".to_string())
    );
}

#[test]
fn test_upper_null() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT UPPER(NULL) AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null);
}

#[test]
fn test_lower_null() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT LOWER(NULL) AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null);
}

#[test]
fn test_upper_with_table_data() {
    let mut db = Database::new();

    // Create a test table with string data
    let schema = TableSchema::new(
        "users".to_string(),
        vec![ColumnSchema::new(
            "name".to_string(),
            DataType::Varchar { max_length: 50 },
            false,
        )],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Varchar("alice".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Varchar("bob".to_string())]),
    )
    .unwrap();

    let results = execute_query(&db, "SELECT UPPER(name) AS upper_name FROM users;").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0].values[0],
        SqlValue::Varchar("ALICE".to_string())
    );
    assert_eq!(
        results[1].values[0],
        SqlValue::Varchar("BOB".to_string())
    );
}

#[test]
fn test_case_insensitive_function_names() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    // Test that function names are case-insensitive
    let results1 = execute_query(&db, "SELECT upper('test') AS result FROM dual;").unwrap();
    let results2 = execute_query(&db, "SELECT Upper('test') AS result FROM dual;").unwrap();
    let results3 = execute_query(&db, "SELECT UPPER('test') AS result FROM dual;").unwrap();

    assert_eq!(results1[0].values[0], results2[0].values[0]);
    assert_eq!(results2[0].values[0], results3[0].values[0]);
    assert_eq!(
        results1[0].values[0],
        SqlValue::Varchar("TEST".to_string())
    );
}
