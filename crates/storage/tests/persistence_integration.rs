use catalog::{ColumnSchema, TableSchema};
use storage::{parse_sql_statements, read_sql_dump, Database};
use types::DataType;

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
        .insert(storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("Alice".to_string()),
            types::SqlValue::Integer(30),
        ]))
        .unwrap();

    table
        .insert(storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Varchar("Bob".to_string()),
            types::SqlValue::Null,
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
        let result = parser::Parser::parse_sql(trimmed);
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
