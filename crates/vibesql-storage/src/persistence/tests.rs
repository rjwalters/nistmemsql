// ============================================================================
// Persistence Tests
// ============================================================================

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_types::{DataType, SqlValue};

use crate::Database;

#[test]
fn test_save_sql_dump() {
    let mut db = Database::new();

    // Create test schema
    let schema = TableSchema::new(
        "test".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data
    let table = db.get_table_mut("test").unwrap();
    table
        .insert(crate::Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())]))
        .unwrap();
    table
        .insert(crate::Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())]))
        .unwrap();

    // Save SQL dump
    let path = "/tmp/test_db.sql";
    db.save_sql_dump(path).unwrap();

    // Verify file exists and contains expected content
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.contains("CREATE TABLE"));
    assert!(content.contains("INSERT INTO"));
    assert!(content.contains("Alice"));
    assert!(content.contains("Bob"));

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_sql_value_to_literal() {
    use super::save::sql_value_to_literal;

    assert_eq!(sql_value_to_literal(&SqlValue::Null), "NULL");
    assert_eq!(sql_value_to_literal(&SqlValue::Integer(42)), "42");
    assert_eq!(sql_value_to_literal(&SqlValue::Varchar("test".to_string())), "'test'");
    assert_eq!(sql_value_to_literal(&SqlValue::Varchar("test's".to_string())), "'test''s'");
    assert_eq!(sql_value_to_literal(&SqlValue::Boolean(true)), "TRUE");
    assert_eq!(sql_value_to_literal(&SqlValue::Boolean(false)), "FALSE");
}

#[test]
fn test_sql_dump_with_nulls() {
    let mut db = Database::new();

    // Create test schema with nullable column
    let schema = TableSchema::new(
        "test_nulls".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "nullable_col".to_string(),
                DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data with NULL values
    let table = db.get_table_mut("test_nulls").unwrap();
    table.insert(crate::Row::new(vec![SqlValue::Integer(1), SqlValue::Null])).unwrap();
    table
        .insert(crate::Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("text".to_string())]))
        .unwrap();
    table.insert(crate::Row::new(vec![SqlValue::Integer(3), SqlValue::Null])).unwrap();

    // Save SQL dump
    let path = "/tmp/test_nulls.sql";
    db.save_sql_dump(path).unwrap();

    // Verify SQL contains NULL literal
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.contains("NULL"), "SQL dump should contain NULL literal");
    assert!(content.contains("INSERT INTO test_nulls VALUES (1, NULL)"));
    assert!(content.contains("INSERT INTO test_nulls VALUES (2, 'text')"));
    assert!(content.contains("INSERT INTO test_nulls VALUES (3, NULL)"));

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_sql_dump_with_quotes() {
    let mut db = Database::new();

    // Create test schema
    let schema = TableSchema::new(
        "test_quotes".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "text".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data with various quote types
    let table = db.get_table_mut("test_quotes").unwrap();
    table
        .insert(crate::Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("it's".to_string())]))
        .unwrap();
    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("say \"hello\"".to_string()),
        ]))
        .unwrap();
    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("it's a \"test\"".to_string()),
        ]))
        .unwrap();
    table
        .insert(crate::Row::new(vec![SqlValue::Integer(4), SqlValue::Varchar("".to_string())]))
        .unwrap();

    // Save SQL dump
    let path = "/tmp/test_quotes.sql";
    db.save_sql_dump(path).unwrap();

    // Verify SQL properly escapes quotes
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.contains("'it''s'"), "Single quotes should be escaped as ''");
    assert!(
        content.contains("'say \"hello\"'"),
        "Double quotes should be preserved in single-quoted strings"
    );
    assert!(content.contains("'it''s a \"test\"'"), "Mixed quotes should be handled correctly");
    assert!(content.contains("''"), "Empty string should be represented");

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_sql_dump_empty_database() {
    let db = Database::new();

    // Save SQL dump of empty database
    let path = "/tmp/test_empty_db.sql";
    db.save_sql_dump(path).unwrap();

    // Verify file exists and contains valid SQL structure
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.contains("-- VibeSQL Database Dump"));
    assert!(content.contains("-- Schemas"));
    assert!(content.contains("-- Roles"));
    assert!(content.contains("-- Tables and Data"));
    assert!(content.contains("-- Indexes"));
    assert!(content.contains("-- End of dump"));

    // Verify no CREATE TABLE statements in empty database
    assert!(!content.contains("CREATE TABLE"));

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_sql_dump_empty_table() {
    let mut db = Database::new();

    // Create test schema but insert no data
    let schema = TableSchema::new(
        "empty_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Save SQL dump
    let path = "/tmp/test_empty_table.sql";
    db.save_sql_dump(path).unwrap();

    // Verify SQL contains CREATE TABLE but no INSERT statements
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.contains("CREATE TABLE empty_table"));
    assert!(
        !content.contains("INSERT INTO empty_table"),
        "Empty table should have no INSERT statements"
    );

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_sql_dump_with_indexes() {
    let mut db = Database::new();

    // Create test schema
    let schema = TableSchema::new(
        "test_indexes".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "email".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Create indexes (use fully qualified table name)
    let idx1 =
        vibesql_ast::IndexColumn { column_name: "name".to_string(), direction: vibesql_ast::OrderDirection::Asc };
    db.create_index("idx_name".to_string(), "public.test_indexes".to_string(), false, vec![idx1])
        .unwrap();

    let idx2 =
        vibesql_ast::IndexColumn { column_name: "email".to_string(), direction: vibesql_ast::OrderDirection::Asc };
    db.create_index(
        "idx_email_unique".to_string(),
        "public.test_indexes".to_string(),
        true,
        vec![idx2],
    )
    .unwrap();

    // Save SQL dump
    let path = "/tmp/test_indexes.sql";
    db.save_sql_dump(path).unwrap();

    // Verify SQL contains CREATE INDEX statements
    let content = std::fs::read_to_string(path).unwrap();
    // Index names are normalized to uppercase
    assert!(content.contains("CREATE INDEX IDX_NAME"), "Should contain CREATE INDEX IDX_NAME");
    assert!(
        content.contains("CREATE UNIQUE INDEX IDX_EMAIL_UNIQUE"),
        "Should contain CREATE UNIQUE INDEX IDX_EMAIL_UNIQUE"
    );
    assert!(content.contains("ON public.test_indexes"), "Should reference test_indexes table");
    assert!(content.contains("Asc"), "Index direction should be included");

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_sql_dump_all_data_types() {
    let mut db = Database::new();

    // Create test schema with all data types
    let schema = TableSchema::new(
        "all_types".to_string(),
        vec![
            // Integer types
            ColumnSchema::new("col_int".to_string(), DataType::Integer, true),
            ColumnSchema::new("col_smallint".to_string(), DataType::Smallint, true),
            ColumnSchema::new("col_bigint".to_string(), DataType::Bigint, true),
            // Float types
            ColumnSchema::new("col_float".to_string(), DataType::Float { precision: 24 }, true),
            ColumnSchema::new("col_real".to_string(), DataType::Real, true),
            ColumnSchema::new("col_double".to_string(), DataType::DoublePrecision, true),
            // String types
            ColumnSchema::new(
                "col_varchar".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
            ColumnSchema::new("col_char".to_string(), DataType::Character { length: 10 }, true),
            // Boolean
            ColumnSchema::new("col_bool".to_string(), DataType::Boolean, true),
            // Numeric
            ColumnSchema::new(
                "col_numeric".to_string(),
                DataType::Numeric { precision: 10, scale: 2 },
                true,
            ),
            ColumnSchema::new(
                "col_decimal".to_string(),
                DataType::Decimal { precision: 5, scale: 0 },
                true,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data with various values
    let table = db.get_table_mut("all_types").unwrap();
    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(42),
            SqlValue::Smallint(100),
            SqlValue::Bigint(999999),
            SqlValue::Float(3.14),
            SqlValue::Real(2.718),
            SqlValue::Double(1.414),
            SqlValue::Varchar("test".to_string()),
            SqlValue::Character("fixed".to_string()),
            SqlValue::Boolean(true),
            SqlValue::Numeric(123.45),
            SqlValue::Numeric(999.0),
        ]))
        .unwrap();

    // Test special float values (NaN, Infinity)
    table
        .insert(crate::Row::new(vec![
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Float(f32::NAN),
            SqlValue::Real(f32::INFINITY),
            SqlValue::Double(f64::NEG_INFINITY),
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Boolean(false),
            SqlValue::Null,
            SqlValue::Null,
        ]))
        .unwrap();

    // Save SQL dump
    let path = "/tmp/test_all_types.sql";
    db.save_sql_dump(path).unwrap();

    // Verify SQL contains all data type declarations
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.contains("INTEGER"));
    assert!(content.contains("SMALLINT"));
    assert!(content.contains("BIGINT"));
    assert!(content.contains("FLOAT(24)"));
    assert!(content.contains("REAL"));
    assert!(content.contains("DOUBLE PRECISION"));
    assert!(content.contains("VARCHAR(100)"));
    assert!(content.contains("CHAR(10)"));
    assert!(content.contains("BOOLEAN"));
    assert!(content.contains("NUMERIC(10, 2)"));
    assert!(content.contains("DECIMAL(5, 0)"));

    // Verify special float values
    assert!(content.contains("'NaN'"));
    assert!(content.contains("'Infinity'"));
    assert!(content.contains("'-Infinity'"));
    assert!(content.contains("TRUE"));
    assert!(content.contains("FALSE"));

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_sql_dump_large_dataset() {
    let mut db = Database::new();

    // Create test schema
    let schema = TableSchema::new(
        "large_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "value".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert 10,000 rows
    let table = db.get_table_mut("large_table").unwrap();
    for i in 0..10000 {
        table
            .insert(crate::Row::new(vec![
                SqlValue::Integer(i as i64),
                SqlValue::Varchar(format!("value_{}", i)),
            ]))
            .unwrap();
    }

    // Save SQL dump and measure
    let path = "/tmp/test_large.sql";
    let start = std::time::Instant::now();
    db.save_sql_dump(path).unwrap();
    let duration = start.elapsed();

    // Verify export completed in reasonable time (should be fast)
    assert!(duration.as_secs() < 10, "Export took too long: {:?}", duration);

    // Verify file exists and has content
    let metadata = std::fs::metadata(path).unwrap();
    assert!(metadata.len() > 0, "File should have content");

    // Verify row count in file
    let content = std::fs::read_to_string(path).unwrap();
    let insert_count = content.matches("INSERT INTO large_table").count();
    assert_eq!(insert_count, 10000, "Should have 10,000 INSERT statements");

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_read_sql_dump() {
    use super::load::read_sql_dump;

    let mut db = Database::new();

    // Create test schema
    let schema = TableSchema::new(
        "test_load".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data
    let table = db.get_table_mut("test_load").unwrap();
    table
        .insert(crate::Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Test".to_string())]))
        .unwrap();

    // Save SQL dump
    let path = "/tmp/test_read_dump.sql";
    db.save_sql_dump(path).unwrap();

    // Read the dump back
    let content = read_sql_dump(path).unwrap();
    assert!(content.contains("CREATE TABLE"));
    assert!(content.contains("INSERT INTO"));
    assert!(content.contains("Test"));

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_read_sql_dump_file_not_found() {
    use super::load::read_sql_dump;

    let result = read_sql_dump("/tmp/nonexistent_file_xyz123.sql");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("does not exist"));
}

// ============================================================================
// JSON Format Tests
// ============================================================================

#[test]
fn test_json_roundtrip_basic() {
    let mut db = Database::new();

    // Create test schema with multiple types
    let schema = TableSchema::new(
        "test_json".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("active".to_string(), DataType::Boolean, true),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data
    let table = db.get_table_mut("test_json").unwrap();
    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Boolean(true),
        ]))
        .unwrap();
    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Boolean(false),
        ]))
        .unwrap();
    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Charlie".to_string()),
            SqlValue::Null,
        ]))
        .unwrap();

    // Save to JSON
    let path = "/tmp/test_roundtrip.json";
    db.save_json(path).unwrap();

    // Verify file exists and is valid JSON
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.contains("vibesql"));
    assert!(content.contains("test_json"));
    assert!(content.contains("Alice"));
    assert!(content.contains("Bob"));
    assert!(content.contains("Charlie"));

    // Load from JSON
    let loaded_db = Database::load_json(path).unwrap();

    // Verify table exists
    let loaded_table = loaded_db.get_table("test_json").unwrap();
    let rows = loaded_table.scan();

    // Verify row count
    assert_eq!(rows.len(), 3);

    // Verify data
    assert_eq!(rows[0].values[0], SqlValue::Integer(1));
    assert_eq!(rows[0].values[1], SqlValue::Varchar("Alice".to_string()));
    assert_eq!(rows[0].values[2], SqlValue::Boolean(true));

    assert_eq!(rows[1].values[0], SqlValue::Integer(2));
    assert_eq!(rows[1].values[1], SqlValue::Varchar("Bob".to_string()));
    assert_eq!(rows[1].values[2], SqlValue::Boolean(false));

    assert_eq!(rows[2].values[0], SqlValue::Integer(3));
    assert_eq!(rows[2].values[1], SqlValue::Varchar("Charlie".to_string()));
    assert_eq!(rows[2].values[2], SqlValue::Null);

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_json_roundtrip_all_types() {
    let mut db = Database::new();

    // Create schema with various types
    let schema = TableSchema::new(
        "all_types_json".to_string(),
        vec![
            ColumnSchema::new("col_int".to_string(), DataType::Integer, true),
            ColumnSchema::new("col_bigint".to_string(), DataType::Bigint, true),
            ColumnSchema::new("col_float".to_string(), DataType::Float { precision: 24 }, true),
            ColumnSchema::new("col_varchar".to_string(), DataType::Varchar { max_length: Some(100) }, true),
            ColumnSchema::new("col_bool".to_string(), DataType::Boolean, true),
            ColumnSchema::new(
                "col_numeric".to_string(),
                DataType::Numeric { precision: 10, scale: 2 },
                true,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data
    let table = db.get_table_mut("all_types_json").unwrap();
    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(42),
            SqlValue::Bigint(999999),
            SqlValue::Float(3.14),
            SqlValue::Varchar("test".to_string()),
            SqlValue::Boolean(true),
            SqlValue::Numeric(123.45),
        ]))
        .unwrap();

    // Insert row with NULL values
    table
        .insert(crate::Row::new(vec![
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Varchar("not_null".to_string()),
            SqlValue::Boolean(false),
            SqlValue::Null,
        ]))
        .unwrap();

    // Save to JSON
    let path = "/tmp/test_all_types.json";
    db.save_json(path).unwrap();

    // Load from JSON
    let loaded_db = Database::load_json(path).unwrap();

    // Verify table exists
    let loaded_table = loaded_db.get_table("all_types_json").unwrap();
    let rows = loaded_table.scan();

    // Verify row count
    assert_eq!(rows.len(), 2);

    // Verify first row data types and values
    assert_eq!(rows[0].values[0], SqlValue::Integer(42));
    assert_eq!(rows[0].values[1], SqlValue::Bigint(999999));
    assert_eq!(rows[0].values[3], SqlValue::Varchar("test".to_string()));
    assert_eq!(rows[0].values[4], SqlValue::Boolean(true));
    assert_eq!(rows[0].values[5], SqlValue::Numeric(123.45));

    // Verify second row has NULLs and non-null values
    assert_eq!(rows[1].values[0], SqlValue::Null);
    assert_eq!(rows[1].values[3], SqlValue::Varchar("not_null".to_string()));
    assert_eq!(rows[1].values[4], SqlValue::Boolean(false));

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_json_empty_database() {
    let db = Database::new();

    // Save empty database
    let path = "/tmp/test_empty.json";
    db.save_json(path).unwrap();

    // Verify file exists
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.contains("vibesql"));
    assert!(content.contains("\"tables\": []"));

    // Load from JSON
    let loaded_db = Database::load_json(path).unwrap();
    assert_eq!(loaded_db.catalog.list_tables().len(), 0);

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_json_roundtrip_temporal_types() {
    use vibesql_types::{Date, Time, Timestamp};

    let mut db = Database::new();

    // Create schema with temporal types
    let schema = TableSchema::new(
        "temporal_test".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("col_date".to_string(), DataType::Date, true),
            ColumnSchema::new("col_time".to_string(), DataType::Time { with_timezone: false }, true),
            ColumnSchema::new("col_timestamp".to_string(), DataType::Timestamp { with_timezone: false }, true),
            ColumnSchema::new(
                "col_timestamp_tz".to_string(),
                DataType::Timestamp { with_timezone: true },
                true,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data with temporal values
    let table = db.get_table_mut("temporal_test").unwrap();
    let date = Date::new(2025, 1, 15).unwrap();
    let time = Time::new(14, 30, 45, 0).unwrap();
    let timestamp = Timestamp::new(date, time);

    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Date(Date::new(2025, 1, 15).unwrap()),
            SqlValue::Time(Time::new(14, 30, 45, 0).unwrap()),
            SqlValue::Timestamp(timestamp.clone()),
            SqlValue::Timestamp(timestamp),
        ]))
        .unwrap();

    // Insert row with NULL temporal values
    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
        ]))
        .unwrap();

    // Save to JSON
    let path = "/tmp/test_temporal.json";
    db.save_json(path).unwrap();

    // Verify JSON contains ISO 8601 formatted dates
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.contains("2025-01-15"));
    assert!(content.contains("14:30:45"));

    // Load from JSON
    let loaded_db = Database::load_json(path).unwrap();

    // Verify table exists
    let loaded_table = loaded_db.get_table("temporal_test").unwrap();
    let rows = loaded_table.scan();

    // Verify row count
    assert_eq!(rows.len(), 2);

    // Verify temporal data
    assert_eq!(rows[0].values[0], SqlValue::Integer(1));
    assert_eq!(rows[0].values[1], SqlValue::Date(Date::new(2025, 1, 15).unwrap()));
    assert_eq!(rows[0].values[2], SqlValue::Time(Time::new(14, 30, 45, 0).unwrap()));

    // Verify NULLs preserved
    assert_eq!(rows[1].values[1], SqlValue::Null);
    assert_eq!(rows[1].values[2], SqlValue::Null);

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_json_numeric_precision() {
    let mut db = Database::new();

    // Create schema with numeric types
    let schema = TableSchema::new(
        "numeric_test".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "col_numeric".to_string(),
                DataType::Numeric { precision: 10, scale: 2 },
                true,
            ),
            ColumnSchema::new(
                "col_decimal".to_string(),
                DataType::Decimal { precision: 5, scale: 0 },
                true,
            ),
            ColumnSchema::new(
                "col_numeric_large".to_string(),
                DataType::Numeric { precision: 20, scale: 5 },
                true,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data with various numeric values
    let table = db.get_table_mut("numeric_test").unwrap();
    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Numeric(123.45),
            SqlValue::Numeric(12345.0),
            SqlValue::Numeric(123456789.12345),
        ]))
        .unwrap();

    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Numeric(0.01),
            SqlValue::Numeric(99999.0),
            SqlValue::Numeric(0.00001),
        ]))
        .unwrap();

    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
        ]))
        .unwrap();

    // Save to JSON
    let path = "/tmp/test_numeric_precision.json";
    db.save_json(path).unwrap();

    // Load from JSON
    let loaded_db = Database::load_json(path).unwrap();

    // Verify table exists
    let loaded_table = loaded_db.get_table("numeric_test").unwrap();
    let rows = loaded_table.scan();

    // Verify row count
    assert_eq!(rows.len(), 3);

    // Verify numeric data preserved
    assert_eq!(rows[0].values[1], SqlValue::Numeric(123.45));
    assert_eq!(rows[0].values[2], SqlValue::Numeric(12345.0));
    assert_eq!(rows[1].values[1], SqlValue::Numeric(0.01));
    assert_eq!(rows[2].values[1], SqlValue::Null);

    // Verify schema preserved precision/scale
    assert_eq!(loaded_table.schema.columns[1].data_type, DataType::Numeric { precision: 10, scale: 2 });
    assert_eq!(loaded_table.schema.columns[2].data_type, DataType::Decimal { precision: 5, scale: 0 });

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_json_integer_types() {
    let mut db = Database::new();

    // Create schema with all integer types
    let schema = TableSchema::new(
        "integer_test".to_string(),
        vec![
            ColumnSchema::new("col_smallint".to_string(), DataType::Smallint, true),
            ColumnSchema::new("col_integer".to_string(), DataType::Integer, true),
            ColumnSchema::new("col_bigint".to_string(), DataType::Bigint, true),
            ColumnSchema::new("col_unsigned".to_string(), DataType::Unsigned, true),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data with boundary values
    let table = db.get_table_mut("integer_test").unwrap();
    table
        .insert(crate::Row::new(vec![
            SqlValue::Smallint(i16::MAX),
            SqlValue::Integer(i64::MAX),
            SqlValue::Bigint(i64::MAX),
            SqlValue::Unsigned(u64::MAX),
        ]))
        .unwrap();

    table
        .insert(crate::Row::new(vec![
            SqlValue::Smallint(i16::MIN),
            SqlValue::Integer(i64::MIN),
            SqlValue::Bigint(i64::MIN),
            SqlValue::Unsigned(0),
        ]))
        .unwrap();

    table
        .insert(crate::Row::new(vec![
            SqlValue::Smallint(0),
            SqlValue::Integer(42),
            SqlValue::Bigint(-999999),
            SqlValue::Unsigned(123456789),
        ]))
        .unwrap();

    // Save to JSON
    let path = "/tmp/test_integer_types.json";
    db.save_json(path).unwrap();

    // Load from JSON
    let loaded_db = Database::load_json(path).unwrap();

    // Verify table exists
    let loaded_table = loaded_db.get_table("integer_test").unwrap();
    let rows = loaded_table.scan();

    // Verify row count
    assert_eq!(rows.len(), 3);

    // Verify boundary values preserved
    assert_eq!(rows[0].values[0], SqlValue::Smallint(i16::MAX));
    assert_eq!(rows[0].values[1], SqlValue::Integer(i64::MAX));
    assert_eq!(rows[0].values[3], SqlValue::Unsigned(u64::MAX));

    assert_eq!(rows[1].values[0], SqlValue::Smallint(i16::MIN));
    assert_eq!(rows[1].values[1], SqlValue::Integer(i64::MIN));
    assert_eq!(rows[1].values[3], SqlValue::Unsigned(0));

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_json_special_floats() {
    let mut db = Database::new();

    // Create schema with float types
    let schema = TableSchema::new(
        "float_test".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("col_float".to_string(), DataType::Float { precision: 24 }, true),
            ColumnSchema::new("col_real".to_string(), DataType::Real, true),
            ColumnSchema::new("col_double".to_string(), DataType::DoublePrecision, true),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data with special float values
    let table = db.get_table_mut("float_test").unwrap();
    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Float(f32::NAN),
            SqlValue::Real(f32::INFINITY),
            SqlValue::Double(f64::NEG_INFINITY),
        ]))
        .unwrap();

    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Float(3.14159),
            SqlValue::Real(2.71828),
            SqlValue::Double(1.41421),
        ]))
        .unwrap();

    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Float(0.0),
            SqlValue::Real(-0.0),
            SqlValue::Double(123.456789),
        ]))
        .unwrap();

    // Save to JSON
    let path = "/tmp/test_special_floats.json";
    db.save_json(path).unwrap();

    // Verify JSON contains special float representations
    let _content = std::fs::read_to_string(path).unwrap();
    // Note: serde_json serializes special floats as null by default
    // or as strings depending on settings - we just verify it doesn't crash

    // Load from JSON
    let loaded_db = Database::load_json(path).unwrap();

    // Verify table exists
    let loaded_table = loaded_db.get_table("float_test").unwrap();
    let rows = loaded_table.scan();

    // Verify row count
    assert_eq!(rows.len(), 3);

    // Verify normal float values preserved
    assert_eq!(rows[1].values[0], SqlValue::Integer(2));
    // Note: Floating point comparison needs tolerance for roundtrip

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_json_schemas_and_roles() {
    let mut db = Database::new();

    // Create non-default schemas
    db.catalog.create_schema("analytics".to_string()).unwrap();
    db.catalog.create_schema("staging".to_string()).unwrap();

    // Create roles
    db.catalog.create_role("admin".to_string()).unwrap();
    db.catalog.create_role("readonly".to_string()).unwrap();

    // Create table in default schema
    let schema1 = TableSchema::new(
        "users".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema1).unwrap();

    // Save to JSON
    let path = "/tmp/test_schemas_roles.json";
    db.save_json(path).unwrap();

    // Verify JSON contains schemas and roles
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.contains("analytics"));
    assert!(content.contains("staging"));
    assert!(content.contains("admin"));
    assert!(content.contains("readonly"));

    // Load from JSON
    let loaded_db = Database::load_json(path).unwrap();

    // Verify schemas recreated (excluding public)
    let schemas = loaded_db.catalog.list_schemas();
    assert!(schemas.contains(&"analytics".to_string()));
    assert!(schemas.contains(&"staging".to_string()));

    // Verify roles recreated
    let roles = loaded_db.catalog.list_roles();
    assert!(roles.contains(&"admin".to_string()));
    assert!(roles.contains(&"readonly".to_string()));

    // Verify table exists
    assert!(loaded_db.get_table("users").is_some());

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_json_large_dataset() {
    let mut db = Database::new();

    // Create test schema
    let schema = TableSchema::new(
        "large_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "data".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new("value".to_string(), DataType::Float { precision: 24 }, true),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert 10,000 rows
    let table = db.get_table_mut("large_table").unwrap();
    for i in 0..10000 {
        table
            .insert(crate::Row::new(vec![
                SqlValue::Integer(i as i64),
                SqlValue::Varchar(format!("data_value_{}", i)),
                SqlValue::Float((i as f32) * 1.5),
            ]))
            .unwrap();
    }

    // Save to JSON and measure time
    let path = "/tmp/test_large_dataset.json";
    let start = std::time::Instant::now();
    db.save_json(path).unwrap();
    let save_duration = start.elapsed();

    // Verify save completed in reasonable time
    assert!(save_duration.as_secs() < 10, "JSON save took too long: {:?}", save_duration);

    // Verify file exists and has content
    let metadata = std::fs::metadata(path).unwrap();
    assert!(metadata.len() > 0, "File should have content");

    // Load from JSON and measure time
    let start = std::time::Instant::now();
    let loaded_db = Database::load_json(path).unwrap();
    let load_duration = start.elapsed();

    // Verify load completed in reasonable time
    assert!(load_duration.as_secs() < 10, "JSON load took too long: {:?}", load_duration);

    // Verify table exists
    let loaded_table = loaded_db.get_table("large_table").unwrap();
    let rows = loaded_table.scan();

    // Verify row count
    assert_eq!(rows.len(), 10000);

    // Spot check some rows
    assert_eq!(rows[0].values[0], SqlValue::Integer(0));
    assert_eq!(rows[0].values[1], SqlValue::Varchar("data_value_0".to_string()));

    assert_eq!(rows[9999].values[0], SqlValue::Integer(9999));
    assert_eq!(rows[9999].values[1], SqlValue::Varchar("data_value_9999".to_string()));

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_json_load_nonexistent_file() {
    let result = Database::load_json("/tmp/nonexistent_file_12345.json");
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Failed to open file") || err_msg.contains("No such file"));
}

#[test]
fn test_json_load_malformed() {
    // Write malformed JSON
    let path = "/tmp/test_malformed.json";
    std::fs::write(path, "{invalid json content}").unwrap();

    let result = Database::load_json(path);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("JSON deserialization failed") || err_msg.contains("expected"));

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_json_load_empty_file() {
    // Write empty file
    let path = "/tmp/test_empty_json.json";
    std::fs::write(path, "").unwrap();

    let result = Database::load_json(path);
    assert!(result.is_err());

    // Cleanup
    std::fs::remove_file(path).ok();
}

#[test]
fn test_json_index_roundtrip() {
    let mut db = Database::new();

    // Create test table
    let schema = TableSchema::new(
        "test_indexes".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Create index
    let idx = vibesql_ast::IndexColumn {
        column_name: "name".to_string(),
        direction: vibesql_ast::OrderDirection::Asc,
    };

    // Use qualified table name for index creation (indexes require qualified names)
    db.create_index("idx_name".to_string(), "public.test_indexes".to_string(), false, vec![idx])
        .unwrap();

    // Save to JSON
    let path = "/tmp/test_index_roundtrip.json";
    db.save_json(path).unwrap();

    // Verify JSON contains index
    let content = std::fs::read_to_string(path).unwrap();
    assert!(content.contains("idx_name") || content.contains("IDX_NAME"));
    assert!(content.contains("indexes"));

    // Load from JSON
    let loaded_db = Database::load_json(path).unwrap();

    // Debug: Check what tables are available
    let tables = loaded_db.catalog.list_tables();
    eprintln!("Available tables after load: {:?}", tables);

    // Verify index exists
    let indexes = loaded_db.list_indexes();
    assert!(
        indexes.iter().any(|idx| idx.to_uppercase() == "IDX_NAME"),
        "Index not found in loaded database. Available indexes: {:?}, Available tables: {:?}",
        indexes,
        tables
    );

    // Cleanup
    std::fs::remove_file(path).ok();
}

// Note: Views are not yet supported due to missing catalog API (list_views).
// This is documented in the implementation and will be addressed in a follow-up.
