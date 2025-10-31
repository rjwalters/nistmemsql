//! Comprehensive error handling tests
//!
//! This test suite verifies error paths and error message formatting across
//! catalog, executor, and storage layers. Each test triggers a specific error
//! condition and validates the error variant and message.

use ast::Statement;
use catalog::CatalogError;
use executor::{
    AlterTableExecutor, CreateTableExecutor, ExecutorError, InsertExecutor,
    SchemaExecutor, SelectExecutor,
};
use executor::advanced_objects::{execute_create_view, execute_drop_view};
use parser::Parser;
use storage::Database;

// ============================================================================
// Catalog Error Tests
// ============================================================================

#[test]
fn test_table_already_exists_error() {
    let mut db = Database::new();

    // Create table first time
    let sql = "CREATE TABLE users (id INTEGER)";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateTable(create_stmt) = stmt {
        CreateTableExecutor::execute(&create_stmt, &mut db).expect("First create should succeed");
    }

    // Try to create same table again
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateTable(create_stmt) = stmt {
        let result = CreateTableExecutor::execute(&create_stmt, &mut db);
        assert!(result.is_err(), "Should fail with TableAlreadyExists");

        if let Err(ExecutorError::TableAlreadyExists(name)) = result {
            assert!(name == "USERS" || name == "public.USERS", "Expected USERS or public.USERS, got: {}", name);
            let error_msg = format!("{}", ExecutorError::TableAlreadyExists(name));
            assert!(error_msg.contains("already exists"));
        } else {
            panic!("Expected TableAlreadyExists error, got: {:?}", result);
        }
    }
}

#[test]
fn test_table_not_found_error() {
    let db = Database::new();

    // Try to select from non-existent table
    let sql = "SELECT * FROM nonexistent_table";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::Select(select_stmt) = stmt {
        let result = SelectExecutor::new(&db).execute(&select_stmt);
        assert!(result.is_err(), "Should fail with TableNotFound");

        if let Err(ExecutorError::TableNotFound(name)) = result {
            assert_eq!(name, "NONEXISTENT_TABLE");
            let error_msg = format!("{}", ExecutorError::TableNotFound(name));
            assert!(error_msg.contains("not found"));
        } else {
            panic!("Expected TableNotFound error, got: {:?}", result);
        }
    }
}

#[test]
#[ignore] // Column validation not fully implemented for SELECT
fn test_column_not_found_error() {
    let mut db = Database::new();

    // Create table
    let sql = "CREATE TABLE products (id INTEGER, name VARCHAR(50))";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateTable(create_stmt) = stmt {
        CreateTableExecutor::execute(&create_stmt, &mut db).expect("Create should succeed");
    }

    // Try to select non-existent column
    let sql = "SELECT invalid_column FROM products";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::Select(select_stmt) = stmt {
        let result = SelectExecutor::new(&db).execute(&select_stmt);
        assert!(result.is_err(), "Should fail with ColumnNotFound");

        match result {
            Err(ExecutorError::ColumnNotFound(name)) => {
                let error_msg = format!("{}", ExecutorError::ColumnNotFound(name.clone()));
                assert!(error_msg.contains("not found"));
            }
            other => panic!("Expected ColumnNotFound error, got: {:?}", other),
        }
    }
}

#[test]
fn test_column_already_exists_error() {
    let mut db = Database::new();

    // Create table
    let sql = "CREATE TABLE items (id INTEGER, name VARCHAR(50))";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateTable(create_stmt) = stmt {
        CreateTableExecutor::execute(&create_stmt, &mut db).expect("Create should succeed");
    }

    // Try to add column that already exists
    let sql = "ALTER TABLE items ADD COLUMN id INTEGER";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::AlterTable(alter_stmt) = stmt {
        let result = AlterTableExecutor::execute(&alter_stmt, &mut db);
        assert!(result.is_err(), "Should fail with ColumnAlreadyExists");

        match result {
            Err(ExecutorError::ColumnAlreadyExists(name)) => {
                let error_msg = format!("{}", ExecutorError::ColumnAlreadyExists(name.clone()));
                assert!(error_msg.contains("already exists"));
            }
            other => panic!("Expected ColumnAlreadyExists error, got: {:?}", other),
        }
    }
}

#[test]
fn test_schema_already_exists_error() {
    let mut db = Database::new();

    // Create schema first time
    let sql = "CREATE SCHEMA myschema";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateSchema(create_stmt) = stmt {
        SchemaExecutor::execute_create_schema(&create_stmt, &mut db).expect("First create should succeed");
    }

    // Try to create same schema again
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateSchema(create_stmt) = stmt {
        let result = SchemaExecutor::execute_create_schema(&create_stmt, &mut db);
        assert!(result.is_err(), "Should fail with SchemaAlreadyExists");

        match result {
            Err(ExecutorError::SchemaAlreadyExists(name)) => {
                let error_msg = format!("{}", ExecutorError::SchemaAlreadyExists(name.clone()));
                assert!(error_msg.contains("already exists"));
            }
            Err(ExecutorError::StorageError(msg)) if msg.contains("SchemaAlreadyExists") => {
                assert!(msg.contains("MYSCHEMA"));
            }
            other => panic!("Expected SchemaAlreadyExists error, got: {:?}", other),
        }
    }
}

#[test]
fn test_schema_not_found_error() {
    let mut db = Database::new();

    // Try to drop non-existent schema
    let sql = "DROP SCHEMA nonexistent_schema";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::DropSchema(drop_stmt) = stmt {
        let result = SchemaExecutor::execute_drop_schema(&drop_stmt, &mut db);
        assert!(result.is_err(), "Should fail with SchemaNotFound");

        match result {
            Err(ExecutorError::SchemaNotFound(name)) => {
                let error_msg = format!("{}", ExecutorError::SchemaNotFound(name.clone()));
                assert!(error_msg.contains("not found"));
            }
            Err(ExecutorError::StorageError(msg)) if msg.contains("SchemaNotFound") => {
                assert!(msg.contains("NONEXISTENT_SCHEMA"));
            }
            other => panic!("Expected SchemaNotFound error, got: {:?}", other),
        }
    }
}

#[test]
fn test_schema_not_empty_error() {
    let mut db = Database::new();

    // Create schema
    let sql = "CREATE SCHEMA testschema";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateSchema(create_stmt) = stmt {
        SchemaExecutor::execute_create_schema(&create_stmt, &mut db).expect("Create should succeed");
    }

    // Create table in schema
    let sql = "CREATE TABLE testschema.items (id INTEGER)";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateTable(create_stmt) = stmt {
        CreateTableExecutor::execute(&create_stmt, &mut db).expect("Create table should succeed");
    }

    // Try to drop non-empty schema
    let sql = "DROP SCHEMA testschema";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::DropSchema(drop_stmt) = stmt {
        let result = SchemaExecutor::execute_drop_schema(&drop_stmt, &mut db);
        assert!(result.is_err(), "Should fail with SchemaNotEmpty");

        match result {
            Err(ExecutorError::SchemaNotEmpty(name)) => {
                let error_msg = format!("{}", ExecutorError::SchemaNotEmpty(name.clone()));
                assert!(error_msg.contains("not empty"));
            }
            Err(ExecutorError::StorageError(msg)) if msg.contains("SchemaNotEmpty") => {
                assert!(msg.contains("TESTSCHEMA"));
            }
            other => panic!("Expected SchemaNotEmpty error, got: {:?}", other),
        }
    }
}

#[test]
fn test_view_already_exists_error() {
    let mut db = Database::new();

    // Create table for view
    let sql = "CREATE TABLE base_table (id INTEGER)";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateTable(create_stmt) = stmt {
        CreateTableExecutor::execute(&create_stmt, &mut db).expect("Create should succeed");
    }

    // Create view first time
    let sql = "CREATE VIEW myview AS SELECT * FROM base_table";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateView(create_stmt) = stmt {
        execute_create_view(&create_stmt, &mut db).expect("First create should succeed");
    }

    // Try to create same view again
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateView(create_stmt) = stmt {
        let result = execute_create_view(&create_stmt, &mut db);
        assert!(result.is_err(), "Should fail with ViewAlreadyExists");

        // Error is wrapped in ExecutorError::Other
        match result {
            Err(ExecutorError::Other(msg)) if msg.contains("already exists") => {
                assert!(msg.contains("View"));
            }
            other => panic!("Expected ViewAlreadyExists error, got: {:?}", other),
        }
    }
}

#[test]
fn test_view_not_found_error() {
    let mut db = Database::new();

    // Try to drop non-existent view
    let sql = "DROP VIEW nonexistent_view";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::DropView(drop_stmt) = stmt {
        let result = execute_drop_view(&drop_stmt, &mut db);
        assert!(result.is_err(), "Should fail with ViewNotFound");

        match result {
            Err(ExecutorError::Other(msg)) if msg.contains("not found") => {
                assert!(msg.contains("View"));
            }
            other => panic!("Expected ViewNotFound error, got: {:?}", other),
        }
    }
}

// ============================================================================
// Executor Error Tests
// ============================================================================

#[test]
fn test_division_by_zero_error() {
    let mut db = Database::new();

    // Create table with numeric data
    let sql = "CREATE TABLE numbers (value INTEGER)";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateTable(create_stmt) = stmt {
        CreateTableExecutor::execute(&create_stmt, &mut db).expect("Create should succeed");
    }

    // Insert test data
    let sql = "INSERT INTO numbers VALUES (10)";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::Insert(insert_stmt) = stmt {
        InsertExecutor::execute(&mut db, &insert_stmt).expect("Insert should succeed");
    }

    // Try division by zero
    let sql = "SELECT value / 0 FROM numbers";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::Select(select_stmt) = stmt {
        let result = SelectExecutor::new(&db).execute(&select_stmt);
        assert!(result.is_err(), "Should fail with DivisionByZero");

        match result {
            Err(ExecutorError::DivisionByZero) => {
                let error_msg = format!("{}", ExecutorError::DivisionByZero);
                assert!(error_msg.contains("Division by zero"));
            }
            other => panic!("Expected DivisionByZero error, got: {:?}", other),
        }
    }
}

#[test]
#[ignore] // Subquery execution not fully implemented
fn test_subquery_returned_multiple_rows_error() {
    let mut db = Database::new();

    // Create and populate table
    let sql = "CREATE TABLE items (id INTEGER, name VARCHAR(50))";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateTable(create_stmt) = stmt {
        CreateTableExecutor::execute(&create_stmt, &mut db).expect("Create should succeed");
    }

    // Insert multiple rows
    let sql = "INSERT INTO items VALUES (1, 'Item1'), (2, 'Item2')";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::Insert(insert_stmt) = stmt {
        InsertExecutor::execute(&mut db, &insert_stmt).expect("Insert should succeed");
    }

    // Scalar subquery returning multiple rows
    let sql = "SELECT (SELECT id FROM items) AS single_value";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::Select(select_stmt) = stmt {
        let result = SelectExecutor::new(&db).execute(&select_stmt);
        assert!(result.is_err(), "Should fail with SubqueryReturnedMultipleRows");

        match result {
            Err(ExecutorError::SubqueryReturnedMultipleRows { expected, actual }) => {
                assert_eq!(expected, 1);
                assert_eq!(actual, 2);
                let error_msg = format!("{}", ExecutorError::SubqueryReturnedMultipleRows { expected, actual });
                assert!(error_msg.contains("returned") && error_msg.contains("rows"));
            }
            other => panic!("Expected SubqueryReturnedMultipleRows error, got: {:?}", other),
        }
    }
}

#[test]
fn test_type_mismatch_error() {
    let mut db = Database::new();

    // Create table
    let sql = "CREATE TABLE data (num INTEGER, text VARCHAR(50))";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateTable(create_stmt) = stmt {
        CreateTableExecutor::execute(&create_stmt, &mut db).expect("Create should succeed");
    }

    // Insert test data
    let sql = "INSERT INTO data VALUES (42, 'hello')";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::Insert(insert_stmt) = stmt {
        InsertExecutor::execute(&mut db, &insert_stmt).expect("Insert should succeed");
    }

    // Try to add number and string (type mismatch)
    let sql = "SELECT num + text FROM data";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::Select(select_stmt) = stmt {
        let result = SelectExecutor::new(&db).execute(&select_stmt);
        assert!(result.is_err(), "Should fail with TypeMismatch");

        match result {
            Err(ExecutorError::TypeMismatch { .. }) => {
                // TypeMismatch error successfully triggered
            }
            other => panic!("Expected TypeMismatch error, got: {:?}", other),
        }
    }
}

#[test]
fn test_constraint_violation_error() {
    let mut db = Database::new();

    // Create table with NOT NULL constraint
    let sql = "CREATE TABLE users (id INTEGER NOT NULL, name VARCHAR(50))";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateTable(create_stmt) = stmt {
        CreateTableExecutor::execute(&create_stmt, &mut db).expect("Create should succeed");
    }

    // Try to insert NULL into NOT NULL column
    let sql = "INSERT INTO users (id, name) VALUES (NULL, 'John')";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::Insert(insert_stmt) = stmt {
        let result = InsertExecutor::execute(&mut db, &insert_stmt);
        assert!(result.is_err(), "Should fail with ConstraintViolation");

        match result {
            Err(ExecutorError::ConstraintViolation(msg)) => {
                let error_msg = format!("{}", ExecutorError::ConstraintViolation(msg.clone()));
                assert!(error_msg.contains("Constraint violation"));
            }
            other => panic!("Expected ConstraintViolation error, got: {:?}", other),
        }
    }
}

#[test]
#[ignore] // Dropping last column validation not implemented
fn test_cannot_drop_column_error() {
    let mut db = Database::new();

    // Create table with single column
    let sql = "CREATE TABLE minimal (id INTEGER)";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateTable(create_stmt) = stmt {
        CreateTableExecutor::execute(&create_stmt, &mut db).expect("Create should succeed");
    }

    // Try to drop the only column (should fail)
    let sql = "ALTER TABLE minimal DROP COLUMN id";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::AlterTable(alter_stmt) = stmt {
        let result = AlterTableExecutor::execute(&alter_stmt, &mut db);
        assert!(result.is_err(), "Should fail with CannotDropColumn");

        match result {
            Err(ExecutorError::CannotDropColumn(msg)) => {
                let error_msg = format!("{}", ExecutorError::CannotDropColumn(msg.clone()));
                assert!(error_msg.contains("Cannot drop column"));
            }
            other => panic!("Expected CannotDropColumn error, got: {:?}", other),
        }
    }
}

#[test]
fn test_cast_error() {
    // Test CastError display format
    let error = ExecutorError::CastError {
        from_type: "VARCHAR".to_string(),
        to_type: "INTEGER".to_string(),
    };

    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Cannot cast"));
    assert!(error_msg.contains("VARCHAR"));
    assert!(error_msg.contains("INTEGER"));
}

#[test]
fn test_column_index_out_of_bounds_error() {
    // Test ColumnIndexOutOfBounds display format
    let error = ExecutorError::ColumnIndexOutOfBounds { index: 99 };

    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Column index"));
    assert!(error_msg.contains("99"));
    assert!(error_msg.contains("out of bounds"));
}

#[test]
fn test_permission_denied_error() {
    // Test PermissionDenied display format
    let error = ExecutorError::PermissionDenied {
        role: "guest".to_string(),
        privilege: "DELETE".to_string(),
        object: "users".to_string(),
    };

    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Permission denied"));
    assert!(error_msg.contains("guest"));
    assert!(error_msg.contains("DELETE"));
    assert!(error_msg.contains("users"));
}

#[test]
fn test_unsupported_expression_error() {
    // Test UnsupportedExpression display format
    let error = ExecutorError::UnsupportedExpression("LATERAL JOIN".to_string());

    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Unsupported expression"));
    assert!(error_msg.contains("LATERAL JOIN"));
}

#[test]
fn test_unsupported_feature_error() {
    // Test UnsupportedFeature display format
    let error = ExecutorError::UnsupportedFeature("Recursive CTEs".to_string());

    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Unsupported feature"));
    assert!(error_msg.contains("Recursive CTEs"));
}

// ============================================================================
// Storage Error Tests
// ============================================================================

#[test]
fn test_storage_column_count_mismatch_error() {
    use storage::StorageError;

    // Test ColumnCountMismatch display format
    let error = StorageError::ColumnCountMismatch {
        expected: 3,
        actual: 5,
    };

    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Column count mismatch"));
    assert!(error_msg.contains("expected 3"));
    assert!(error_msg.contains("got 5"));
}

#[test]
fn test_storage_column_index_out_of_bounds_error() {
    use storage::StorageError;

    // Test ColumnIndexOutOfBounds display format
    let error = StorageError::ColumnIndexOutOfBounds { index: 42 };

    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Column index"));
    assert!(error_msg.contains("42"));
    assert!(error_msg.contains("out of bounds"));
}

#[test]
fn test_storage_row_not_found_error() {
    use storage::StorageError;

    // Test RowNotFound display format
    let error = StorageError::RowNotFound;

    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Row not found"));
}

#[test]
fn test_storage_catalog_error() {
    use storage::StorageError;

    // Test CatalogError display format
    let error = StorageError::CatalogError("Failed to update catalog".to_string());

    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Catalog error"));
    assert!(error_msg.contains("Failed to update catalog"));
}

#[test]
fn test_storage_transaction_error() {
    use storage::StorageError;

    // Test TransactionError display format
    let error = StorageError::TransactionError("Deadlock detected".to_string());

    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Transaction error"));
    assert!(error_msg.contains("Deadlock detected"));
}

// ============================================================================
// Catalog Error Display Tests
// ============================================================================

#[test]
fn test_catalog_error_display_formats() {
    // Test all CatalogError variants display correctly

    let errors = vec![
        (
            CatalogError::TableAlreadyExists("mytable".to_string()),
            vec!["Table", "mytable", "already exists"]
        ),
        (
            CatalogError::TableNotFound("missing".to_string()),
            vec!["Table", "missing", "not found"]
        ),
        (
            CatalogError::ColumnAlreadyExists("mycolumn".to_string()),
            vec!["Column", "mycolumn", "already exists"]
        ),
        (
            CatalogError::ColumnNotFound("badcol".to_string()),
            vec!["Column", "badcol", "not found"]
        ),
        (
            CatalogError::SchemaAlreadyExists("myschema".to_string()),
            vec!["Schema", "myschema", "already exists"]
        ),
        (
            CatalogError::SchemaNotFound("noschema".to_string()),
            vec!["Schema", "noschema", "not found"]
        ),
        (
            CatalogError::SchemaNotEmpty("fullschema".to_string()),
            vec!["Schema", "fullschema", "not empty"]
        ),
        (
            CatalogError::RoleAlreadyExists("admin".to_string()),
            vec!["Role", "admin", "already exists"]
        ),
        (
            CatalogError::RoleNotFound("norole".to_string()),
            vec!["Role", "norole", "not found"]
        ),
        (
            CatalogError::DomainAlreadyExists("mydomain".to_string()),
            vec!["Domain", "mydomain", "already exists"]
        ),
        (
            CatalogError::DomainNotFound("nodomain".to_string()),
            vec!["Domain", "nodomain", "not found"]
        ),
        (
            CatalogError::SequenceAlreadyExists("myseq".to_string()),
            vec!["Sequence", "myseq", "already exists"]
        ),
        (
            CatalogError::SequenceNotFound("noseq".to_string()),
            vec!["Sequence", "noseq", "not found"]
        ),
        (
            CatalogError::TypeAlreadyExists("mytype".to_string()),
            vec!["Type", "mytype", "already exists"]
        ),
        (
            CatalogError::TypeNotFound("notype".to_string()),
            vec!["Type", "notype", "not found"]
        ),
        (
            CatalogError::TypeInUse("busytype".to_string()),
            vec!["Type", "busytype", "still in use"]
        ),
        (
            CatalogError::CollationAlreadyExists("mycoll".to_string()),
            vec!["Collation", "mycoll", "already exists"]
        ),
        (
            CatalogError::CollationNotFound("nocoll".to_string()),
            vec!["Collation", "nocoll", "not found"]
        ),
        (
            CatalogError::CharacterSetAlreadyExists("mycharset".to_string()),
            vec!["Character set", "mycharset", "already exists"]
        ),
        (
            CatalogError::CharacterSetNotFound("nocharset".to_string()),
            vec!["Character set", "nocharset", "not found"]
        ),
        (
            CatalogError::TranslationAlreadyExists("mytrans".to_string()),
            vec!["Translation", "mytrans", "already exists"]
        ),
        (
            CatalogError::TranslationNotFound("notrans".to_string()),
            vec!["Translation", "notrans", "not found"]
        ),
        (
            CatalogError::ViewAlreadyExists("myview".to_string()),
            vec!["View", "myview", "already exists"]
        ),
        (
            CatalogError::ViewNotFound("noview".to_string()),
            vec!["View", "noview", "not found"]
        ),
    ];

    for (error, expected_parts) in errors {
        let error_msg = format!("{}", error);
        for part in expected_parts {
            assert!(
                error_msg.contains(part),
                "Error message '{}' should contain '{}'",
                error_msg,
                part
            );
        }
    }
}

// ============================================================================
// Error Conversion Tests
// ============================================================================

#[test]
fn test_catalog_to_executor_error_conversion() {
    // Test From<CatalogError> for ExecutorError conversion

    let catalog_err = CatalogError::TableNotFound("test".to_string());
    let executor_err: ExecutorError = catalog_err.into();

    match executor_err {
        ExecutorError::TableNotFound(name) => {
            assert_eq!(name, "test");
        }
        other => panic!("Expected TableNotFound, got: {:?}", other),
    }

    // Test advanced SQL:1999 objects conversion
    let catalog_err = CatalogError::DomainNotFound("mydomain".to_string());
    let executor_err: ExecutorError = catalog_err.into();

    match executor_err {
        ExecutorError::Other(msg) => {
            assert!(msg.contains("Domain"));
            assert!(msg.contains("mydomain"));
            assert!(msg.contains("not found"));
        }
        other => panic!("Expected Other error for DomainNotFound, got: {:?}", other),
    }
}

#[test]
fn test_error_implements_std_error_trait() {
    use std::error::Error;

    // Verify all error types implement std::error::Error trait
    let catalog_err: Box<dyn Error> = Box::new(CatalogError::TableNotFound("test".to_string()));
    assert!(catalog_err.to_string().contains("not found"));

    let executor_err: Box<dyn Error> = Box::new(ExecutorError::DivisionByZero);
    assert!(executor_err.to_string().contains("Division by zero"));

    let storage_err: Box<dyn Error> = Box::new(storage::StorageError::RowNotFound);
    assert!(storage_err.to_string().contains("Row not found"));
}
