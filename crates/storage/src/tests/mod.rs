use super::*;

#[test]
fn test_row_creation() {
    let row = Row::new(vec![
        types::SqlValue::Integer(1),
        types::SqlValue::Varchar("Alice".to_string()),
    ]);
    assert_eq!(row.len(), 2);
    assert_eq!(row.get(0), Some(&types::SqlValue::Integer(1)));
}

#[test]
fn test_table_creation() {
    let schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    let table = Table::new(schema);
    assert_eq!(table.row_count(), 0);
    assert_eq!(table.schema.name, "users");
}

#[test]
fn test_table_insert() {
    let schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    let mut table = Table::new(schema);

    let row = Row::new(vec![
        types::SqlValue::Integer(1),
        types::SqlValue::Varchar("Alice".to_string()),
    ]);

    let result = table.insert(row);
    assert!(result.is_ok());
    assert_eq!(table.row_count(), 1);
}

#[test]
fn test_table_insert_wrong_column_count() {
    let schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    let mut table = Table::new(schema);

    // Only 1 value when table expects 2
    let row = Row::new(vec![types::SqlValue::Integer(1)]);

    let result = table.insert(row);
    assert!(result.is_err());
    match result.unwrap_err() {
        StorageError::ColumnCountMismatch { expected, actual } => {
            assert_eq!(expected, 2);
            assert_eq!(actual, 1);
        }
        _ => panic!("Expected ColumnCountMismatch error"),
    }
}

#[test]
fn test_table_scan() {
    let schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    );
    let mut table = Table::new(schema);

    table.insert(Row::new(vec![types::SqlValue::Integer(1)])).unwrap();
    table.insert(Row::new(vec![types::SqlValue::Integer(2)])).unwrap();
    table.insert(Row::new(vec![types::SqlValue::Integer(3)])).unwrap();

    let rows = table.scan();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get(0), Some(&types::SqlValue::Integer(1)));
    assert_eq!(rows[1].get(0), Some(&types::SqlValue::Integer(2)));
    assert_eq!(rows[2].get(0), Some(&types::SqlValue::Integer(3)));
}

#[test]
fn test_database_create_table() {
    let mut db = Database::new();
    let schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    );

    let result = db.create_table(schema);
    assert!(result.is_ok());
    assert!(db.catalog.table_exists("users"));
    assert!(db.get_table("users").is_some());
}

#[test]
fn test_database_insert_row() {
    let mut db = Database::new();
    let schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    let row = Row::new(vec![types::SqlValue::Integer(1)]);
    let result = db.insert_row("users", row);
    assert!(result.is_ok());

    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn test_database_insert_into_nonexistent_table() {
    let mut db = Database::new();
    let row = Row::new(vec![types::SqlValue::Integer(1)]);
    let result = db.insert_row("missing", row);

    assert!(result.is_err());
    match result.unwrap_err() {
        StorageError::TableNotFound(name) => assert_eq!(name, "missing"),
        _ => panic!("Expected TableNotFound error"),
    }
}

#[test]
fn test_database_drop_table() {
    let mut db = Database::new();
    let schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    assert!(db.catalog.table_exists("users"));

    let result = db.drop_table("users");
    assert!(result.is_ok());
    assert!(!db.catalog.table_exists("users"));
    assert!(db.get_table("users").is_none());
}

#[test]
fn test_database_multiple_tables() {
    let mut db = Database::new();

    // Create users table
    let users_schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(users_schema).unwrap();

    // Create orders table
    let orders_schema = catalog::TableSchema::new(
        "orders".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(orders_schema).unwrap();

    // Insert into both
    db.insert_row("users", Row::new(vec![types::SqlValue::Integer(1)])).unwrap();
    db.insert_row("orders", Row::new(vec![types::SqlValue::Integer(100)])).unwrap();

    assert_eq!(db.get_table("users").unwrap().row_count(), 1);
    assert_eq!(db.get_table("orders").unwrap().row_count(), 1);

    let tables = db.list_tables();
    assert_eq!(tables.len(), 2);
}

// ========================================================================
// Diagnostic Tools Tests
// ========================================================================

#[test]
fn test_debug_info() {
    let mut db = Database::new();
    let schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "users",
        Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("Alice".to_string()),
        ]),
    )
    .unwrap();

    let debug = db.debug_info();
    assert!(debug.contains("Database Debug Info"));
    assert!(debug.contains("Tables: 1"));
    assert!(debug.contains("users"));
    assert!(debug.contains("1 rows"));
    assert!(debug.contains("2 columns"));
}

#[test]
fn test_dump_table() {
    let mut db = Database::new();
    let schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "users",
        Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("Alice".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Varchar("Bob".to_string()),
        ]),
    )
    .unwrap();

    let dump = db.dump_table("users").unwrap();
    assert!(dump.contains("Table: users"));
    assert!(dump.contains("id | name"));
    assert!(dump.contains("1 | Alice"));
    assert!(dump.contains("2 | Bob"));
    assert!(dump.contains("(2 rows)"));
}

#[test]
fn test_dump_table_not_found() {
    let db = Database::new();
    let result = db.dump_table("missing");
    assert!(result.is_err());
    match result.unwrap_err() {
        StorageError::TableNotFound(name) => assert_eq!(name, "missing"),
        _ => panic!("Expected TableNotFound error"),
    }
}

#[test]
fn test_dump_tables() {
    let mut db = Database::new();

    // Create and populate users table
    let users_schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(users_schema).unwrap();
    db.insert_row("users", Row::new(vec![types::SqlValue::Integer(1)])).unwrap();

    // Create and populate orders table
    let orders_schema = catalog::TableSchema::new(
        "orders".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(orders_schema).unwrap();
    db.insert_row("orders", Row::new(vec![types::SqlValue::Integer(100)])).unwrap();

    let dump = db.dump_tables();
    assert!(dump.contains("Table: users") || dump.contains("Table: orders"));
    assert!(dump.contains("1") || dump.contains("100"));
}
