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
        vec![ColumnSchema::new("dummy".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("dual", Row::new(vec![SqlValue::Integer(1)])).unwrap();
}

// Test SUBSTR (alias for SUBSTRING)
#[test]
fn test_substr_basic() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results =
        execute_query(&db, "SELECT SUBSTR('hello world', 1, 5) AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar("hello".to_string()));
}

// Test INSTR
#[test]
fn test_instr_found() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results =
        execute_query(&db, "SELECT INSTR('hello world', 'world') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(7));
}

#[test]
fn test_instr_not_found() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results =
        execute_query(&db, "SELECT INSTR('hello world', 'xyz') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(0));
}

#[test]
fn test_instr_null() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT INSTR(NULL, 'world') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null);
}

// Test LOCATE
#[test]
fn test_locate_found() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results =
        execute_query(&db, "SELECT LOCATE('world', 'hello world') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(7));
}

#[test]
fn test_locate_with_start() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    // Start searching from position 7, won't find at position 7
    let results =
        execute_query(&db, "SELECT LOCATE('world', 'hello world', 7) AS result FROM dual;")
            .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(7));
}

#[test]
fn test_locate_not_found() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results =
        execute_query(&db, "SELECT LOCATE('xyz', 'hello world') AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(0));
}

// Test FORMAT
#[test]
fn test_format_with_decimals() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT FORMAT(1234567.89, 2) AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar("1,234,567.89".to_string()));
}

#[test]
fn test_format_no_decimals() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT FORMAT(1000000, 0) AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar("1,000,000".to_string()));
}

#[test]
fn test_format_negative() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    // Test with integer subtraction to get negative number
    let results = execute_query(&db, "SELECT FORMAT(0 - 1234, 2) AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar("-1,234.00".to_string()));
}

// Test VERSION
#[test]
fn test_version() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT VERSION() AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    match &results[0].values[0] {
        SqlValue::Varchar(s) => {
            assert!(s.starts_with("NistMemSQL "));
        }
        _ => panic!("Expected VARCHAR result from VERSION()"),
    }
}

// Test DATABASE/SCHEMA
#[test]
fn test_database() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT DATABASE() AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar("default".to_string()));
}

#[test]
fn test_schema() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT SCHEMA() AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar("default".to_string()));
}

// Test USER/CURRENT_USER
#[test]
fn test_user() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT USER() AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar("anonymous".to_string()));
}

#[test]
fn test_current_user() {
    let mut db = Database::new();
    create_dummy_table(&mut db);

    let results = execute_query(&db, "SELECT CURRENT_USER() AS result FROM dual;").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar("anonymous".to_string()));
}
