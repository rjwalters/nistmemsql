use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_storage::{parse_sql_statements, read_sql_dump, Database};
use vibesql_types::DataType;

#[test]
fn test_database_save_and_load_roundtrip() {
    let temp_file = "/tmp/test_db_roundtrip.sql";

    // Clean up any existing test file
    let _ = std::fs::remove_file(temp_file);

    // Step 1: Create database with some tables and data
    let mut db = Database::new();

    // Create a schema
    let schema = TableSchema::new(
        "test_users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new("age".to_string(), DataType::Integer, true),
        ],
    );

    db.create_table(schema.clone()).unwrap();

    // Insert some rows using the table directly
    let table = db.get_table_mut("test_users").unwrap();
    table
        .insert(vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar("Alice".to_string()),
            vibesql_types::SqlValue::Integer(30),
        ]))
        .unwrap();

    table
        .insert(vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar("Bob".to_string()),
            vibesql_types::SqlValue::Null,
        ]))
        .unwrap();

    // Step 2: Save database to SQL dump
    db.save_sql_dump(temp_file).unwrap();

    // Step 3: Verify file was created and has content
    assert!(std::path::Path::new(temp_file).exists(), "SQL dump file should exist");

    let content = std::fs::read_to_string(temp_file).unwrap();
    assert!(content.contains("CREATE TABLE test_users"), "SQL dump should contain CREATE TABLE");
    assert!(content.contains("Alice"), "SQL dump should contain inserted data");
    assert!(content.contains("Bob"), "SQL dump should contain inserted data");

    // Step 4: Load database from SQL dump using the load utilities
    let sql_content = read_sql_dump(temp_file).unwrap();
    let statements = parse_sql_statements(&sql_content).unwrap();

    // Verify we got the expected statements
    assert!(!statements.is_empty(), "Should have parsed statements from SQL dump");

    // Step 5: Verify we can parse the statements
    for (idx, stmt_sql) in statements.iter().enumerate() {
        let trimmed = stmt_sql.trim();
        if trimmed.is_empty() || trimmed.starts_with("--") {
            continue;
        }

        // Just verify it parses - we don't execute in this test
        let result = vibesql_parser::Parser::parse_sql(trimmed);
        assert!(
            result.is_ok(),
            "Statement {} should parse successfully: {}\nError: {:?}",
            idx,
            trimmed,
            result.err()
        );
    }

    // Clean up
    std::fs::remove_file(temp_file).unwrap();
}

#[test]
fn test_binary_format_roundtrip() {
    let temp_file = "/tmp/test_db_binary_roundtrip.vbsql";

    // Clean up any existing test file
    let _ = std::fs::remove_file(temp_file);

    // Step 1: Create database with schemas, tables, indexes, and data
    let mut db = Database::new();

    // Create a custom schema
    db.catalog.create_schema("test_schema".to_string()).unwrap();

    // Create first table with various data types
    let users_schema = TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new("age".to_string(), DataType::Integer, true),
            ColumnSchema::new("active".to_string(), DataType::Boolean, false),
        ],
    );
    db.create_table(users_schema).unwrap();

    // Create second table
    let products_schema = TableSchema::new(
        "products".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "product_name".to_string(),
                DataType::Varchar { max_length: Some(200) },
                false,
            ),
            ColumnSchema::new("price".to_string(), DataType::DoublePrecision, false),
        ],
    );
    db.create_table(products_schema).unwrap();

    // Insert data into users table
    let users_table = db.get_table_mut("users").unwrap();
    users_table
        .insert(vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar("Alice".to_string()),
            vibesql_types::SqlValue::Integer(30),
            vibesql_types::SqlValue::Boolean(true),
        ]))
        .unwrap();
    users_table
        .insert(vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar("Bob".to_string()),
            vibesql_types::SqlValue::Null,
            vibesql_types::SqlValue::Boolean(false),
        ]))
        .unwrap();
    users_table
        .insert(vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar("Charlie".to_string()),
            vibesql_types::SqlValue::Integer(25),
            vibesql_types::SqlValue::Boolean(true),
        ]))
        .unwrap();

    // Insert data into products table
    let products_table = db.get_table_mut("products").unwrap();
    products_table
        .insert(vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar("Widget".to_string()),
            vibesql_types::SqlValue::Double(19.99),
        ]))
        .unwrap();
    products_table
        .insert(vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar("Gadget".to_string()),
            vibesql_types::SqlValue::Double(29.99),
        ]))
        .unwrap();

    // Step 2: Save database to binary format
    db.save_binary(temp_file).unwrap();

    // Step 3: Verify file was created and has content
    assert!(std::path::Path::new(temp_file).exists(), "Binary file should exist");

    let metadata = std::fs::metadata(temp_file).unwrap();
    assert!(metadata.len() > 100, "Binary file should have substantial content");

    // Step 4: Load database from binary format
    let db2 = Database::load_binary(temp_file).unwrap();

    // Step 5: Verify all data was preserved

    // Check schema exists
    assert!(db2.catalog.list_schemas().contains(&"test_schema".to_string()));

    // Check tables exist
    let table_names = db2.catalog.list_tables();
    assert!(table_names.contains(&"users".to_string()));
    assert!(table_names.contains(&"products".to_string()));

    // Check users table structure and data
    let users_table2 = db2.get_table("users").unwrap();
    assert_eq!(users_table2.schema.columns.len(), 4);
    assert_eq!(users_table2.schema.columns[0].name, "id");
    assert_eq!(users_table2.schema.columns[1].name, "name");
    assert_eq!(users_table2.schema.columns[2].name, "age");
    assert_eq!(users_table2.schema.columns[3].name, "active");
    assert_eq!(users_table2.row_count(), 3);

    // Verify specific data values
    let users_rows = users_table2.scan();
    assert_eq!(users_rows[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(users_rows[0].values[1], vibesql_types::SqlValue::Varchar("Alice".to_string()));
    assert_eq!(users_rows[0].values[2], vibesql_types::SqlValue::Integer(30));
    assert_eq!(users_rows[0].values[3], vibesql_types::SqlValue::Boolean(true));

    // Verify NULL handling
    assert_eq!(users_rows[1].values[2], vibesql_types::SqlValue::Null);

    // Check products table structure and data
    let products_table2 = db2.get_table("products").unwrap();
    assert_eq!(products_table2.schema.columns.len(), 3);
    assert_eq!(products_table2.row_count(), 2);

    let products_rows = products_table2.scan();
    assert_eq!(products_rows[0].values[1], vibesql_types::SqlValue::Varchar("Widget".to_string()));
    assert_eq!(products_rows[0].values[2], vibesql_types::SqlValue::Double(19.99));

    // Step 6: Test auto-detection via Database::load()
    let db3 = Database::load(temp_file).unwrap();
    let users_table3 = db3.get_table("users").unwrap();
    assert_eq!(users_table3.row_count(), 3);

    // Clean up
    std::fs::remove_file(temp_file).unwrap();
}

#[test]
fn test_parse_sql_statements_with_comments() {
    let content = r#"
-- This is a comment
CREATE TABLE users (id INTEGER);

-- Another comment
INSERT INTO users VALUES (1);
INSERT INTO users VALUES (2);
    "#;

    let statements = parse_sql_statements(content).unwrap();

    // Should have 3 statements (CREATE TABLE, 2 INSERTs)
    // Line comments (--) are filtered out
    assert_eq!(statements.len(), 3, "Should parse 3 SQL statements");
}

#[test]
fn test_parse_sql_statements_with_string_literals() {
    let content = r#"
INSERT INTO users VALUES (1, 'Alice; Bob');
INSERT INTO users VALUES (2, "Charlie; Dave");
    "#;

    let statements = parse_sql_statements(content).unwrap();

    assert_eq!(statements.len(), 2, "Should parse 2 INSERT statements");
    assert!(statements[0].contains("Alice; Bob"), "Should preserve semicolons in string literals");
    assert!(statements[1].contains("Charlie; Dave"), "Should handle double-quoted strings");
}

#[test]
fn test_parse_multiline_create_table() {
    let content = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200)
);
    "#;

    let statements = parse_sql_statements(content).unwrap();

    assert_eq!(statements.len(), 1, "Should parse 1 CREATE TABLE statement");
    let stmt = &statements[0];
    assert!(stmt.contains("id INTEGER"), "Should preserve column definitions");
    assert!(stmt.contains("name VARCHAR"), "Should preserve all columns");
    assert!(stmt.contains("email VARCHAR"), "Should preserve all columns");
}
