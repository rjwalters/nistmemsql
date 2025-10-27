use crate::errors::ExecutorError;

/// Executor for INSERT statements
pub struct InsertExecutor;

impl InsertExecutor {
    /// Execute an INSERT statement
    /// Returns number of rows inserted
    pub fn execute(
        db: &mut storage::Database,
        stmt: &ast::InsertStmt,
    ) -> Result<usize, ExecutorError> {
        // Get table schema from catalog (clone to avoid borrow issues)
        let schema = db
            .catalog
            .get_table(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?
            .clone();

        // Determine target column indices and types
        let target_column_info: Vec<(usize, types::DataType)> = if stmt.columns.is_empty() {
            // No columns specified: INSERT INTO t VALUES (...)
            // Use all columns in schema order
            schema
                .columns
                .iter()
                .enumerate()
                .map(|(idx, col)| (idx, col.data_type.clone()))
                .collect()
        } else {
            // Columns specified: INSERT INTO t (col1, col2) VALUES (...)
            // Validate and resolve columns
            stmt.columns
                .iter()
                .map(|col_name| {
                    schema
                        .get_column_index(col_name)
                        .map(|idx| {
                            let col = &schema.columns[idx];
                            (idx, col.data_type.clone())
                        })
                        .ok_or_else(|| ExecutorError::ColumnNotFound(col_name.clone()))
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        // Validate each row has correct number of values
        for (row_idx, value_exprs) in stmt.values.iter().enumerate() {
            if value_exprs.len() != target_column_info.len() {
                return Err(ExecutorError::UnsupportedExpression(format!(
                    "INSERT row {} column count mismatch: expected {}, got {}",
                    row_idx + 1,
                    target_column_info.len(),
                    value_exprs.len()
                )));
            }
        }

        let mut rows_inserted = 0;

        for value_exprs in &stmt.values {
            // Build a complete row with values for all columns
            // Start with NULL for all columns, then fill in provided values
            let mut full_row_values = vec![types::SqlValue::Null; schema.columns.len()];

            for (expr, (col_idx, data_type)) in value_exprs.iter().zip(target_column_info.iter()) {
                // Evaluate expression (only literals supported initially)
                let value = evaluate_literal_expression(expr)?;

                // Type check: ensure value matches column type
                validate_type(&value, data_type)?;

                full_row_values[*col_idx] = value;
            }

            // Enforce NOT NULL constraints
            for (col_idx, col) in schema.columns.iter().enumerate() {
                if !col.nullable && full_row_values[col_idx] == types::SqlValue::Null {
                    return Err(ExecutorError::ConstraintViolation(format!(
                        "NOT NULL constraint violated for column '{}'",
                        col.name
                    )));
                }
            }

            // Insert the row
            let row = storage::Row::new(full_row_values);
            db.insert_row(&stmt.table_name, row).map_err(|e| {
                ExecutorError::UnsupportedExpression(format!("Storage error: {}", e))
            })?;
            rows_inserted += 1;
        }

        Ok(rows_inserted)
    }
}

/// Evaluate a literal expression to SqlValue
fn evaluate_literal_expression(expr: &ast::Expression) -> Result<types::SqlValue, ExecutorError> {
    match expr {
        ast::Expression::Literal(lit) => Ok(lit.clone()),
        _ => Err(ExecutorError::UnsupportedExpression(
            "INSERT only supports literal values".to_string(),
        )),
    }
}

/// Validate that a value matches the expected column type
fn validate_type(
    value: &types::SqlValue,
    expected_type: &types::DataType,
) -> Result<(), ExecutorError> {
    use types::{DataType, SqlValue};

    // NULL is valid for any type (NOT NULL constraint checked separately)
    if matches!(value, SqlValue::Null) {
        return Ok(());
    }

    // Check type compatibility
    match (value, expected_type) {
        (SqlValue::Integer(_), DataType::Integer) => Ok(()),
        (SqlValue::Varchar(_), DataType::Varchar { .. }) => Ok(()),
        (SqlValue::Boolean(_), DataType::Boolean) => Ok(()),
        _ => Err(ExecutorError::UnsupportedExpression(format!(
            "Type mismatch: expected {:?}, got {:?}",
            expected_type, value
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_test_table(db: &mut storage::Database) {
        // CREATE TABLE users (id INT, name VARCHAR(50))
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "name".to_string(),
                    types::DataType::Varchar { max_length: 50 },
                    false,
                ),
            ],
        );
        db.create_table(schema).unwrap();
    }

    #[test]
    fn test_basic_insert() {
        let mut db = storage::Database::new();
        setup_test_table(&mut db);

        // INSERT INTO users VALUES (1, 'Alice')
        let stmt = ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec![], // No columns specified
            values: vec![vec![
                ast::Expression::Literal(types::SqlValue::Integer(1)),
                ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
            ]],
        };

        let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
        assert_eq!(rows, 1);

        // Verify row was inserted
        let table = db.get_table("users").unwrap();
        assert_eq!(table.row_count(), 1);
    }

    #[test]
    fn test_multi_row_insert() {
        let mut db = storage::Database::new();
        setup_test_table(&mut db);

        // INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')
        let stmt = ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec![],
            values: vec![
                vec![
                    ast::Expression::Literal(types::SqlValue::Integer(1)),
                    ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
                ],
                vec![
                    ast::Expression::Literal(types::SqlValue::Integer(2)),
                    ast::Expression::Literal(types::SqlValue::Varchar("Bob".to_string())),
                ],
            ],
        };

        let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
        assert_eq!(rows, 2);

        let table = db.get_table("users").unwrap();
        assert_eq!(table.row_count(), 2);
    }

    #[test]
    fn test_insert_with_column_list() {
        let mut db = storage::Database::new();
        setup_test_table(&mut db);

        // INSERT INTO users (name, id) VALUES ('Alice', 1)
        let stmt = ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec!["name".to_string(), "id".to_string()],
            values: vec![vec![
                ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
                ast::Expression::Literal(types::SqlValue::Integer(1)),
            ]],
        };

        let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
        assert_eq!(rows, 1);

        let table = db.get_table("users").unwrap();
        assert_eq!(table.row_count(), 1);
    }

    #[test]
    fn test_insert_null_value() {
        let mut db = storage::Database::new();

        // CREATE TABLE users (id INT, name VARCHAR(50))
        // name is nullable
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "name".to_string(),
                    types::DataType::Varchar { max_length: 50 },
                    true, // nullable
                ),
            ],
        );
        db.create_table(schema).unwrap();

        // INSERT INTO users VALUES (1, NULL)
        let stmt = ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec![],
            values: vec![vec![
                ast::Expression::Literal(types::SqlValue::Integer(1)),
                ast::Expression::Literal(types::SqlValue::Null),
            ]],
        };

        let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
        assert_eq!(rows, 1);
    }

    #[test]
    fn test_insert_type_mismatch() {
        let mut db = storage::Database::new();
        setup_test_table(&mut db);

        // INSERT INTO users VALUES ('not_a_number', 'Alice')
        let stmt = ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec![],
            values: vec![vec![
                ast::Expression::Literal(types::SqlValue::Varchar("not_a_number".to_string())),
                ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
            ]],
        };

        let result = InsertExecutor::execute(&mut db, &stmt);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ExecutorError::UnsupportedExpression(_)));
    }

    #[test]
    fn test_insert_column_count_mismatch() {
        let mut db = storage::Database::new();
        setup_test_table(&mut db);

        // INSERT INTO users VALUES (1)  -- Missing name column
        let stmt = ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec![],
            values: vec![vec![ast::Expression::Literal(types::SqlValue::Integer(1))]],
        };

        let result = InsertExecutor::execute(&mut db, &stmt);
        assert!(result.is_err());
    }

    #[test]
    fn test_insert_table_not_found() {
        let mut db = storage::Database::new();

        // INSERT INTO nonexistent VALUES (1, 'Alice')
        let stmt = ast::InsertStmt {
            table_name: "nonexistent".to_string(),
            columns: vec![],
            values: vec![vec![
                ast::Expression::Literal(types::SqlValue::Integer(1)),
                ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
            ]],
        };

        let result = InsertExecutor::execute(&mut db, &stmt);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ExecutorError::TableNotFound(_)));
    }

    #[test]
    fn test_insert_column_not_found() {
        let mut db = storage::Database::new();
        setup_test_table(&mut db);

        // INSERT INTO users (id, invalid_col) VALUES (1, 'Alice')
        let stmt = ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec!["id".to_string(), "invalid_col".to_string()],
            values: vec![vec![
                ast::Expression::Literal(types::SqlValue::Integer(1)),
                ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
            ]],
        };

        let result = InsertExecutor::execute(&mut db, &stmt);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ExecutorError::ColumnNotFound(_)));
    }

    #[test]
    fn test_insert_not_null_constraint_violation() {
        let mut db = storage::Database::new();
        setup_test_table(&mut db);

        // INSERT INTO users VALUES (NULL, 'Alice')
        // id column is NOT NULL, so this should fail
        let stmt = ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec![],
            values: vec![vec![
                ast::Expression::Literal(types::SqlValue::Null),
                ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
            ]],
        };

        let result = InsertExecutor::execute(&mut db, &stmt);
        assert!(result.is_err());
        match result.unwrap_err() {
            ExecutorError::ConstraintViolation(msg) => {
                assert!(msg.contains("NOT NULL"));
                assert!(msg.contains("id"));
            }
            other => panic!("Expected ConstraintViolation, got {:?}", other),
        }
    }

    #[test]
    fn test_insert_not_null_constraint_with_column_list() {
        let mut db = storage::Database::new();

        // CREATE TABLE test (id INT NOT NULL, name VARCHAR(50) NOT NULL, age INT)
        let schema = catalog::TableSchema::new(
            "test".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "name".to_string(),
                    types::DataType::Varchar { max_length: 50 },
                    false,
                ),
                catalog::ColumnSchema::new("age".to_string(), types::DataType::Integer, true),
            ],
        );
        db.create_table(schema).unwrap();

        // INSERT INTO test (id, age) VALUES (1, 25)
        // name is NOT NULL but not provided, should fail
        let stmt = ast::InsertStmt {
            table_name: "test".to_string(),
            columns: vec!["id".to_string(), "age".to_string()],
            values: vec![vec![
                ast::Expression::Literal(types::SqlValue::Integer(1)),
                ast::Expression::Literal(types::SqlValue::Integer(25)),
            ]],
        };

        let result = InsertExecutor::execute(&mut db, &stmt);
        assert!(result.is_err());
        match result.unwrap_err() {
            ExecutorError::ConstraintViolation(msg) => {
                assert!(msg.contains("NOT NULL"));
                assert!(msg.contains("name"));
            }
            other => panic!("Expected ConstraintViolation, got {:?}", other),
        }
    }
}
