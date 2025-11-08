use executor::database::Database;
use executor::types::DataType;
use catalog::ColumnSchema;

fn main() {
    let mut db = Database::new();
    
    // Create table with lowercase columns (as in SQLLogicTest)
    db.create_table(
        "tab0".to_string(),
        vec![
            ColumnSchema::new("col0".to_string(), DataType::Integer, false),
            ColumnSchema::new("col1".to_string(), DataType::Integer, false),
            ColumnSchema::new("col2".to_string(), DataType::Integer, false),
        ],
    ).unwrap();

    // Insert a row
    db.insert("tab0".to_string(), vec![
        types::Value::Integer(1),
        types::Value::Integer(2),
        types::Value::Integer(3),
    ]).unwrap();

    // Test query: SELECT cor0.col1 FROM tab0 AS cor0
    // This should work but might fail due to case sensitivity
    let query = "SELECT cor0.col1 FROM tab0 AS cor0";
    println!("Testing: {}", query);
}
