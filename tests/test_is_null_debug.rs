use catalog::{ColumnSchema, TableSchema};
use executor::SelectExecutor;
use parser::Parser;
use storage::{Database, Row};
use types::{DataType, SqlValue};

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
fn test_is_null_debug() {
    let schema = TableSchema::new(
        "TAB0".to_string(),
        vec![
            ColumnSchema::new("COL0".to_string(), DataType::Integer, false),
            ColumnSchema::new("COL1".to_string(), DataType::Integer, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(20)]))
        .unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(30), SqlValue::Integer(30)]))
        .unwrap();

    // First, let's just select the expression value itself
    println!("Test 1: SELECT col0, col1, col0 - col1 FROM tab0");
    let results = execute_select(&db, "SELECT col0, col1, col0 - col1 FROM tab0")
        .expect("Query should succeed");
    for row in &results {
        println!("  Row: {:?}", row.values);
    }
    
    // Now test IS NULL
    println!("\nTest 2: SELECT col0 FROM tab0 WHERE col0 - col1 IS NULL");
    let results = execute_select(&db, "SELECT col0 FROM tab0 WHERE col0 - col1 IS NULL")
        .expect("Query should succeed");
    println!("  Returned {} rows", results.len());
    for row in &results {
        println!("  Row: {:?}", row.values);
    }

    // Now with unary operators
    println!("\nTest 3: SELECT col0 FROM tab0 WHERE + col0 - col1 IS NULL");
    let results = execute_select(&db, "SELECT col0 FROM tab0 WHERE + col0 - col1 IS NULL")
        .expect("Query should succeed");
    println!("  Returned {} rows", results.len());
    for row in &results {
        println!("  Row: {:?}", row.values);
    }
}
