//! UPDATE statement execution

use ast::UpdateStmt;
use storage::Database;

use crate::errors::ExecutorError;
use crate::evaluator::ExpressionEvaluator;

/// Executor for UPDATE statements
pub struct UpdateExecutor;

impl UpdateExecutor {
    /// Execute an UPDATE statement
    ///
    /// # Arguments
    ///
    /// * `stmt` - The UPDATE statement AST node
    /// * `database` - The database to update
    ///
    /// # Returns
    ///
    /// Number of rows updated or error
    ///
    /// # Examples
    ///
    /// ```
    /// use ast::{UpdateStmt, Assignment, Expression};
    /// use types::SqlValue;
    /// use storage::Database;
    /// use catalog::{TableSchema, ColumnSchema};
    /// use types::DataType;
    /// use executor::UpdateExecutor;
    ///
    /// let mut db = Database::new();
    ///
    /// // Create table
    /// let schema = TableSchema::new(
    ///     "employees".to_string(),
    ///     vec![
    ///         ColumnSchema::new("id".to_string(), DataType::Integer, false),
    ///         ColumnSchema::new("salary".to_string(), DataType::Integer, false),
    ///     ],
    /// );
    /// db.create_table(schema).unwrap();
    ///
    /// // Insert a row
    /// db.insert_row("employees", storage::Row::new(vec![
    ///     SqlValue::Integer(1),
    ///     SqlValue::Integer(50000),
    /// ])).unwrap();
    ///
    /// // Update salary
    /// let stmt = UpdateStmt {
    ///     table_name: "employees".to_string(),
    ///     assignments: vec![
    ///         Assignment {
    ///             column: "salary".to_string(),
    ///             value: Expression::Literal(SqlValue::Integer(60000)),
    ///         },
    ///     ],
    ///     where_clause: None,
    ///};
    ///
    /// let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    /// assert_eq!(count, 1);
    /// ```
    pub fn execute(stmt: &UpdateStmt, database: &mut Database) -> Result<usize, ExecutorError> {
        // Step 1: Get table schema from catalog
        let schema = database
            .catalog
            .get_table(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Step 2: Get table from storage (for reading rows)
        let table = database
            .get_table(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Step 3: Create expression evaluator
        let evaluator = ExpressionEvaluator::new(schema);

        // Step 4: Build list of updates (two-phase execution for SQL semantics)
        let mut updates = Vec::new();

        for (row_index, row) in table.scan().iter().enumerate() {
            // Check WHERE clause
            let should_update = if let Some(ref where_expr) = stmt.where_clause {
                let result = evaluator.eval(where_expr, row)?;
                // SQL semantics: only TRUE (not NULL) causes update
                matches!(result, types::SqlValue::Boolean(true))
            } else {
                true // No WHERE clause = update all rows
            };

            if should_update {
                // Build updated row by cloning original and applying assignments
                let mut new_row = row.clone();

                // Apply each assignment
                for assignment in &stmt.assignments {
                    // Find column index
                    let col_index = schema
                        .get_column_index(&assignment.column)
                        .ok_or_else(|| ExecutorError::ColumnNotFound(assignment.column.clone()))?;

                    // Evaluate new value expression against ORIGINAL row
                    let new_value = evaluator.eval(&assignment.value, row)?;

                    // Update column in new row
                    new_row
                        .set(col_index, new_value)
                        .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
                }

                // Enforce NOT NULL constraints on the updated row
                for (col_idx, col) in schema.columns.iter().enumerate() {
                    let value = new_row.get(col_idx)
                        .ok_or_else(|| ExecutorError::ColumnIndexOutOfBounds { index: col_idx })?;

                    if !col.nullable && *value == types::SqlValue::Null {
                        return Err(ExecutorError::ConstraintViolation(format!(
                            "NOT NULL constraint violated for column '{}'",
                            col.name
                        )));
                    }
                }

                // Enforce PRIMARY KEY constraint (uniqueness)
                if let Some(pk_indices) = schema.get_primary_key_indices() {
                    // Extract primary key values from the updated row
                    let new_pk_values: Vec<&types::SqlValue> = pk_indices
                        .iter()
                        .filter_map(|&idx| new_row.get(idx))
                        .collect();

                    // Check if any OTHER row has the same primary key
                    for (other_idx, other_row) in table.scan().iter().enumerate() {
                        // Skip the row being updated
                        if other_idx == row_index {
                            continue;
                        }

                        let other_pk_values: Vec<&types::SqlValue> = pk_indices
                            .iter()
                            .filter_map(|&idx| other_row.get(idx))
                            .collect();

                        if new_pk_values == other_pk_values {
                            let pk_col_names: Vec<String> = schema.primary_key
                                .as_ref()
                                .unwrap()
                                .clone();
                            return Err(ExecutorError::ConstraintViolation(format!(
                                "PRIMARY KEY constraint violated: duplicate key value for ({})",
                                pk_col_names.join(", ")
                            )));
                        }
                    }
                }

                // Enforce UNIQUE constraints
                let unique_constraint_indices = schema.get_unique_constraint_indices();
                for (constraint_idx, unique_indices) in unique_constraint_indices.iter().enumerate() {
                    // Extract unique constraint values from the updated row
                    let new_unique_values: Vec<&types::SqlValue> = unique_indices
                        .iter()
                        .filter_map(|&idx| new_row.get(idx))
                        .collect();

                    // Skip if any value in the unique constraint is NULL
                    // (NULL != NULL in SQL, so multiple NULLs are allowed)
                    if new_unique_values.iter().any(|v| **v == types::SqlValue::Null) {
                        continue;
                    }

                    // Check if any OTHER row has the same unique constraint values
                    for (other_idx, other_row) in table.scan().iter().enumerate() {
                        // Skip the row being updated
                        if other_idx == row_index {
                            continue;
                        }

                        let other_unique_values: Vec<&types::SqlValue> = unique_indices
                            .iter()
                            .filter_map(|&idx| other_row.get(idx))
                            .collect();

                        // Skip if any existing value is NULL
                        if other_unique_values.iter().any(|v| **v == types::SqlValue::Null) {
                            continue;
                        }

                        if new_unique_values == other_unique_values {
                            let unique_col_names: Vec<String> = schema.unique_constraints[constraint_idx].clone();
                            return Err(ExecutorError::ConstraintViolation(format!(
                                "UNIQUE constraint violated: duplicate value for ({})",
                                unique_col_names.join(", ")
                            )));
                        }
                    }
                }

                // Enforce CHECK constraints
                if !schema.check_constraints.is_empty() {
                    let evaluator = crate::evaluator::ExpressionEvaluator::new(&schema);

                    for (constraint_name, check_expr) in &schema.check_constraints {
                        // Evaluate the CHECK expression against the updated row
                        let result = evaluator.eval(check_expr, &new_row)?;

                        // CHECK constraint passes if result is TRUE or NULL (UNKNOWN)
                        // CHECK constraint fails if result is FALSE
                        if result == types::SqlValue::Boolean(false) {
                            return Err(ExecutorError::ConstraintViolation(format!(
                                "CHECK constraint '{}' violated",
                                constraint_name
                            )));
                        }
                    }
                }

                updates.push((row_index, new_row));
            }
        }

        // Step 5: Apply all updates (after evaluation phase completes)
        let update_count = updates.len();

        // Get mutable table reference
        let table_mut = database
            .get_table_mut(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        for (index, new_row) in updates {
            table_mut
                .update_row(index, new_row)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
        }

        Ok(update_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::{Assignment, BinaryOperator, Expression};
    use catalog::{ColumnSchema, TableSchema};
    use storage::Row;
    use types::{DataType, SqlValue};

    fn setup_test_table(db: &mut Database) {
        // Create table schema
        let schema = TableSchema::new(
            "employees".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: 50 }, false),
                ColumnSchema::new("salary".to_string(), DataType::Integer, true),
                ColumnSchema::new(
                    "department".to_string(),
                    DataType::Varchar { max_length: 50 },
                    true,
                ),
            ],
        );

        db.create_table(schema).unwrap();

        // Insert test data
        db.insert_row(
            "employees",
            Row::new(vec![
                SqlValue::Integer(1),
                SqlValue::Varchar("Alice".to_string()),
                SqlValue::Integer(45000),
                SqlValue::Varchar("Engineering".to_string()),
            ]),
        )
        .unwrap();

        db.insert_row(
            "employees",
            Row::new(vec![
                SqlValue::Integer(2),
                SqlValue::Varchar("Bob".to_string()),
                SqlValue::Integer(48000),
                SqlValue::Varchar("Engineering".to_string()),
            ]),
        )
        .unwrap();

        db.insert_row(
            "employees",
            Row::new(vec![
                SqlValue::Integer(3),
                SqlValue::Varchar("Charlie".to_string()),
                SqlValue::Integer(42000),
                SqlValue::Varchar("Sales".to_string()),
            ]),
        )
        .unwrap();
    }

    #[test]
    fn test_update_all_rows() {
        let mut db = Database::new();
        setup_test_table(&mut db);

        let stmt = UpdateStmt {
            table_name: "employees".to_string(),
            assignments: vec![Assignment {
                column: "salary".to_string(),
                value: Expression::Literal(SqlValue::Integer(50000)),
            }],
            where_clause: None,
        };

        let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(count, 3);

        // Verify all salaries updated
        let table = db.get_table("employees").unwrap();
        for row in table.scan() {
            assert_eq!(row.get(2).unwrap(), &SqlValue::Integer(50000));
        }
    }

    #[test]
    fn test_update_with_where_clause() {
        let mut db = Database::new();
        setup_test_table(&mut db);

        let stmt = UpdateStmt {
            table_name: "employees".to_string(),
            assignments: vec![Assignment {
                column: "salary".to_string(),
                value: Expression::Literal(SqlValue::Integer(60000)),
            }],
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "department".to_string(),
                }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Varchar("Engineering".to_string()))),
            }),
        };

        let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(count, 2); // Alice and Bob

        // Verify only Engineering employees updated
        let table = db.get_table("employees").unwrap();
        let rows: Vec<&Row> = table.scan().iter().collect();

        // Alice and Bob should have new salary
        assert_eq!(rows[0].get(2).unwrap(), &SqlValue::Integer(60000));
        assert_eq!(rows[1].get(2).unwrap(), &SqlValue::Integer(60000));

        // Charlie should have original salary
        assert_eq!(rows[2].get(2).unwrap(), &SqlValue::Integer(42000));
    }

    #[test]
    fn test_update_multiple_columns() {
        let mut db = Database::new();
        setup_test_table(&mut db);

        let stmt = UpdateStmt {
            table_name: "employees".to_string(),
            assignments: vec![
                Assignment {
                    column: "salary".to_string(),
                    value: Expression::Literal(SqlValue::Integer(55000)),
                },
                Assignment {
                    column: "department".to_string(),
                    value: Expression::Literal(SqlValue::Varchar("Sales".to_string())),
                },
            ],
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
        };

        let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(count, 1);

        // Verify both columns updated for Alice
        let table = db.get_table("employees").unwrap();
        let row = &table.scan()[0];
        assert_eq!(row.get(2).unwrap(), &SqlValue::Integer(55000));
        assert_eq!(row.get(3).unwrap(), &SqlValue::Varchar("Sales".to_string()));
    }

    #[test]
    fn test_update_with_expression() {
        let mut db = Database::new();
        setup_test_table(&mut db);

        // Give everyone a 10% raise: salary = salary * 110 / 100
        let stmt = UpdateStmt {
            table_name: "employees".to_string(),
            assignments: vec![Assignment {
                column: "salary".to_string(),
                value: Expression::BinaryOp {
                    left: Box::new(Expression::BinaryOp {
                        left: Box::new(Expression::ColumnRef {
                            table: None,
                            column: "salary".to_string(),
                        }),
                        op: BinaryOperator::Multiply,
                        right: Box::new(Expression::Literal(SqlValue::Integer(110))),
                    }),
                    op: BinaryOperator::Divide,
                    right: Box::new(Expression::Literal(SqlValue::Integer(100))),
                },
            }],
            where_clause: None,
        };

        let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(count, 3);

        // Verify salaries increased (integer division, so exact values)
        let table = db.get_table("employees").unwrap();
        let rows: Vec<&Row> = table.scan().iter().collect();

        assert_eq!(rows[0].get(2).unwrap(), &SqlValue::Integer(49500)); // 45000 * 1.1
        assert_eq!(rows[1].get(2).unwrap(), &SqlValue::Integer(52800)); // 48000 * 1.1
        assert_eq!(rows[2].get(2).unwrap(), &SqlValue::Integer(46200)); // 42000 * 1.1
    }

    #[test]
    fn test_update_table_not_found() {
        let mut db = Database::new();

        let stmt = UpdateStmt {
            table_name: "nonexistent".to_string(),
            assignments: vec![],
            where_clause: None,
        };

        let result = UpdateExecutor::execute(&stmt, &mut db);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ExecutorError::TableNotFound(_)));
    }

    #[test]
    fn test_update_column_not_found() {
        let mut db = Database::new();
        setup_test_table(&mut db);

        let stmt = UpdateStmt {
            table_name: "employees".to_string(),
            assignments: vec![Assignment {
                column: "nonexistent_column".to_string(),
                value: Expression::Literal(SqlValue::Integer(123)),
            }],
            where_clause: None,
        };

        let result = UpdateExecutor::execute(&stmt, &mut db);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ExecutorError::ColumnNotFound(_)));
    }

    #[test]
    fn test_update_no_matching_rows() {
        let mut db = Database::new();
        setup_test_table(&mut db);

        let stmt = UpdateStmt {
            table_name: "employees".to_string(),
            assignments: vec![Assignment {
                column: "salary".to_string(),
                value: Expression::Literal(SqlValue::Integer(99999)),
            }],
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(999))),
            }),
        };

        let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(count, 0); // No rows matched
    }

    #[test]
    fn test_update_not_null_constraint_violation() {
        let mut db = Database::new();
        setup_test_table(&mut db);

        // Try to set name (NOT NULL column) to NULL
        let stmt = UpdateStmt {
            table_name: "employees".to_string(),
            assignments: vec![Assignment {
                column: "name".to_string(),
                value: Expression::Literal(SqlValue::Null),
            }],
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
        };

        let result = UpdateExecutor::execute(&stmt, &mut db);
        assert!(result.is_err());
        match result.unwrap_err() {
            ExecutorError::ConstraintViolation(msg) => {
                assert!(msg.contains("NOT NULL"));
                assert!(msg.contains("name"));
            }
            other => panic!("Expected ConstraintViolation, got {:?}", other),
        }
    }

    #[test]
    fn test_update_nullable_column_to_null() {
        let mut db = Database::new();
        setup_test_table(&mut db);

        // Set salary (nullable column) to NULL - should succeed
        let stmt = UpdateStmt {
            table_name: "employees".to_string(),
            assignments: vec![Assignment {
                column: "salary".to_string(),
                value: Expression::Literal(SqlValue::Null),
            }],
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
        };

        let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(count, 1);

        // Verify salary was set to NULL
        let table = db.get_table("employees").unwrap();
        let row = &table.scan()[0];
        assert_eq!(row.get(2).unwrap(), &SqlValue::Null);
    }

    #[test]
    fn test_update_primary_key_duplicate() {
        let mut db = Database::new();

        // CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))
        let schema = catalog::TableSchema::with_primary_key(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: 50 },
                    true,
                ),
            ],
            vec!["id".to_string()],
        );
        db.create_table(schema).unwrap();

        // Insert two rows
        db.insert_row(
            "users",
            Row::new(vec![
                SqlValue::Integer(1),
                SqlValue::Varchar("Alice".to_string()),
            ]),
        )
        .unwrap();
        db.insert_row(
            "users",
            Row::new(vec![
                SqlValue::Integer(2),
                SqlValue::Varchar("Bob".to_string()),
            ]),
        )
        .unwrap();

        // Try to update Bob's id to 1 (duplicate)
        let stmt = UpdateStmt {
            table_name: "users".to_string(),
            assignments: vec![Assignment {
                column: "id".to_string(),
                value: Expression::Literal(SqlValue::Integer(1)),
            }],
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            }),
        };

        let result = UpdateExecutor::execute(&stmt, &mut db);
        assert!(result.is_err());
        match result.unwrap_err() {
            ExecutorError::ConstraintViolation(msg) => {
                assert!(msg.contains("PRIMARY KEY"));
            }
            other => panic!("Expected ConstraintViolation, got {:?}", other),
        }
    }

    #[test]
    fn test_update_primary_key_to_unique_value() {
        let mut db = Database::new();

        // CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))
        let schema = catalog::TableSchema::with_primary_key(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: 50 },
                    true,
                ),
            ],
            vec!["id".to_string()],
        );
        db.create_table(schema).unwrap();

        // Insert two rows
        db.insert_row(
            "users",
            Row::new(vec![
                SqlValue::Integer(1),
                SqlValue::Varchar("Alice".to_string()),
            ]),
        )
        .unwrap();
        db.insert_row(
            "users",
            Row::new(vec![
                SqlValue::Integer(2),
                SqlValue::Varchar("Bob".to_string()),
            ]),
        )
        .unwrap();

        // Update Bob's id to 3 (unique) - should succeed
        let stmt = UpdateStmt {
            table_name: "users".to_string(),
            assignments: vec![Assignment {
                column: "id".to_string(),
                value: Expression::Literal(SqlValue::Integer(3)),
            }],
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            }),
        };

        let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(count, 1);

        // Verify the update
        let table = db.get_table("users").unwrap();
        let rows: Vec<&Row> = table.scan().iter().collect();
        assert_eq!(rows[1].get(0).unwrap(), &SqlValue::Integer(3));
    }

    #[test]
    fn test_update_unique_constraint_duplicate() {
        let mut db = storage::Database::new();

        // CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(50) UNIQUE)
        let schema = catalog::TableSchema::with_all_constraints(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "email".to_string(),
                    types::DataType::Varchar { max_length: 50 },
                    true,
                ),
            ],
            Some(vec!["id".to_string()]),
            vec![vec!["email".to_string()]],
        );
        db.create_table(schema).unwrap();

        // Insert two users
        db.insert_row(
            "users",
            Row::new(vec![
                SqlValue::Integer(1),
                SqlValue::Varchar("alice@example.com".to_string()),
            ]),
        )
        .unwrap();
        db.insert_row(
            "users",
            Row::new(vec![
                SqlValue::Integer(2),
                SqlValue::Varchar("bob@example.com".to_string()),
            ]),
        )
        .unwrap();

        // Try to update Bob's email to Alice's email (should fail - UNIQUE violation)
        let stmt = UpdateStmt {
            table_name: "users".to_string(),
            assignments: vec![Assignment {
                column: "email".to_string(),
                value: Expression::Literal(SqlValue::Varchar("alice@example.com".to_string())),
            }],
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            }),
        };

        let result = UpdateExecutor::execute(&stmt, &mut db);
        assert!(result.is_err());
        match result.unwrap_err() {
            ExecutorError::ConstraintViolation(msg) => {
                assert!(msg.contains("UNIQUE"));
                assert!(msg.contains("email"));
            }
            other => panic!("Expected ConstraintViolation, got {:?}", other),
        }
    }

    #[test]
    fn test_update_unique_constraint_to_unique_value() {
        let mut db = storage::Database::new();

        // CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(50) UNIQUE)
        let schema = catalog::TableSchema::with_all_constraints(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "email".to_string(),
                    types::DataType::Varchar { max_length: 50 },
                    true,
                ),
            ],
            Some(vec!["id".to_string()]),
            vec![vec!["email".to_string()]],
        );
        db.create_table(schema).unwrap();

        // Insert two users
        db.insert_row(
            "users",
            Row::new(vec![
                SqlValue::Integer(1),
                SqlValue::Varchar("alice@example.com".to_string()),
            ]),
        )
        .unwrap();
        db.insert_row(
            "users",
            Row::new(vec![
                SqlValue::Integer(2),
                SqlValue::Varchar("bob@example.com".to_string()),
            ]),
        )
        .unwrap();

        // Update Bob's email to a new unique value - should succeed
        let stmt = UpdateStmt {
            table_name: "users".to_string(),
            assignments: vec![Assignment {
                column: "email".to_string(),
                value: Expression::Literal(SqlValue::Varchar("robert@example.com".to_string())),
            }],
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            }),
        };

        let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(count, 1);

        // Verify the update
        let table = db.get_table("users").unwrap();
        let rows: Vec<&Row> = table.scan().iter().collect();
        assert_eq!(
            rows[1].get(1).unwrap(),
            &SqlValue::Varchar("robert@example.com".to_string())
        );
    }

    #[test]
    fn test_update_unique_constraint_allows_null() {
        let mut db = storage::Database::new();

        // CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(50) UNIQUE)
        let schema = catalog::TableSchema::with_all_constraints(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "email".to_string(),
                    types::DataType::Varchar { max_length: 50 },
                    true, // nullable
                ),
            ],
            Some(vec!["id".to_string()]),
            vec![vec!["email".to_string()]],
        );
        db.create_table(schema).unwrap();

        // Insert two users with email
        db.insert_row(
            "users",
            Row::new(vec![
                SqlValue::Integer(1),
                SqlValue::Varchar("alice@example.com".to_string()),
            ]),
        )
        .unwrap();
        db.insert_row(
            "users",
            Row::new(vec![
                SqlValue::Integer(2),
                SqlValue::Varchar("bob@example.com".to_string()),
            ]),
        )
        .unwrap();

        // Update first user's email to NULL - should succeed
        let stmt = UpdateStmt {
            table_name: "users".to_string(),
            assignments: vec![Assignment {
                column: "email".to_string(),
                value: Expression::Literal(SqlValue::Null),
            }],
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
        };

        let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(count, 1);

        // Update second user's email to NULL too - should succeed (multiple NULLs allowed)
        let stmt2 = UpdateStmt {
            table_name: "users".to_string(),
            assignments: vec![Assignment {
                column: "email".to_string(),
                value: Expression::Literal(SqlValue::Null),
            }],
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            }),
        };

        let count2 = UpdateExecutor::execute(&stmt2, &mut db).unwrap();
        assert_eq!(count2, 1);

        // Verify both emails are NULL
        let table = db.get_table("users").unwrap();
        let rows: Vec<&Row> = table.scan().iter().collect();
        assert_eq!(rows[0].get(1).unwrap(), &SqlValue::Null);
        assert_eq!(rows[1].get(1).unwrap(), &SqlValue::Null);
    }

    #[test]
    fn test_update_unique_constraint_composite() {
        let mut db = storage::Database::new();

        // CREATE TABLE users (id INT PRIMARY KEY, first_name VARCHAR(50), last_name VARCHAR(50), UNIQUE(first_name, last_name))
        let schema = catalog::TableSchema::with_all_constraints(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "first_name".to_string(),
                    types::DataType::Varchar { max_length: 50 },
                    true,
                ),
                catalog::ColumnSchema::new(
                    "last_name".to_string(),
                    types::DataType::Varchar { max_length: 50 },
                    true,
                ),
            ],
            Some(vec!["id".to_string()]),
            vec![vec!["first_name".to_string(), "last_name".to_string()]],
        );
        db.create_table(schema).unwrap();

        // Insert two users
        db.insert_row(
            "users",
            Row::new(vec![
                SqlValue::Integer(1),
                SqlValue::Varchar("Alice".to_string()),
                SqlValue::Varchar("Smith".to_string()),
            ]),
        )
        .unwrap();
        db.insert_row(
            "users",
            Row::new(vec![
                SqlValue::Integer(2),
                SqlValue::Varchar("Bob".to_string()),
                SqlValue::Varchar("Jones".to_string()),
            ]),
        )
        .unwrap();

        // Try to update Bob to have the same first_name and last_name as Alice (should fail)
        let stmt = UpdateStmt {
            table_name: "users".to_string(),
            assignments: vec![
                Assignment {
                    column: "first_name".to_string(),
                    value: Expression::Literal(SqlValue::Varchar("Alice".to_string())),
                },
                Assignment {
                    column: "last_name".to_string(),
                    value: Expression::Literal(SqlValue::Varchar("Smith".to_string())),
                },
            ],
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            }),
        };

        let result = UpdateExecutor::execute(&stmt, &mut db);
        assert!(result.is_err());
        match result.unwrap_err() {
            ExecutorError::ConstraintViolation(msg) => {
                assert!(msg.contains("UNIQUE"));
            }
            other => panic!("Expected ConstraintViolation, got {:?}", other),
        }
    }

    #[test]
    fn test_update_check_constraint_passes() {
        let mut db = Database::new();

        // CREATE TABLE products (id INT, price INT CHECK (price >= 0))
        let schema = catalog::TableSchema::with_all_constraint_types(
            "products".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), DataType::Integer, false),
                catalog::ColumnSchema::new("price".to_string(), DataType::Integer, false),
            ],
            None,
            Vec::new(),
            vec![(
                "price_positive".to_string(),
                ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::ColumnRef {
                        table: None,
                        column: "price".to_string(),
                    }),
                    op: ast::BinaryOperator::GreaterThanOrEqual,
                    right: Box::new(ast::Expression::Literal(SqlValue::Integer(0))),
                },
            )],
        );
        db.create_table(schema).unwrap();

        // Insert a row
        db.insert_row(
            "products",
            Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(50)]),
        )
        .unwrap();

        // Update to valid price (should succeed)
        let stmt = UpdateStmt {
            table_name: "products".to_string(),
            assignments: vec![Assignment {
                column: "price".to_string(),
                value: Expression::Literal(SqlValue::Integer(100)),
            }],
            where_clause: None,
        };

        let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_update_check_constraint_violation() {
        let mut db = Database::new();

        // CREATE TABLE products (id INT, price INT CHECK (price >= 0))
        let schema = catalog::TableSchema::with_all_constraint_types(
            "products".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), DataType::Integer, false),
                catalog::ColumnSchema::new("price".to_string(), DataType::Integer, false),
            ],
            None,
            Vec::new(),
            vec![(
                "price_positive".to_string(),
                ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::ColumnRef {
                        table: None,
                        column: "price".to_string(),
                    }),
                    op: ast::BinaryOperator::GreaterThanOrEqual,
                    right: Box::new(ast::Expression::Literal(SqlValue::Integer(0))),
                },
            )],
        );
        db.create_table(schema).unwrap();

        // Insert a row
        db.insert_row(
            "products",
            Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(50)]),
        )
        .unwrap();

        // Try to update to negative price (should fail)
        let stmt = UpdateStmt {
            table_name: "products".to_string(),
            assignments: vec![Assignment {
                column: "price".to_string(),
                value: Expression::Literal(SqlValue::Integer(-10)),
            }],
            where_clause: None,
        };

        let result = UpdateExecutor::execute(&stmt, &mut db);
        assert!(result.is_err());
        match result.unwrap_err() {
            ExecutorError::ConstraintViolation(msg) => {
                assert!(msg.contains("CHECK"));
                assert!(msg.contains("price_positive"));
            }
            other => panic!("Expected ConstraintViolation, got {:?}", other),
        }
    }

    #[test]
    fn test_update_check_constraint_with_null() {
        let mut db = Database::new();

        // CREATE TABLE products (id INT, price INT CHECK (price >= 0))
        let schema = catalog::TableSchema::with_all_constraint_types(
            "products".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), DataType::Integer, false),
                catalog::ColumnSchema::new("price".to_string(), DataType::Integer, true), // nullable
            ],
            None,
            Vec::new(),
            vec![(
                "price_positive".to_string(),
                ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::ColumnRef {
                        table: None,
                        column: "price".to_string(),
                    }),
                    op: ast::BinaryOperator::GreaterThanOrEqual,
                    right: Box::new(ast::Expression::Literal(SqlValue::Integer(0))),
                },
            )],
        );
        db.create_table(schema).unwrap();

        // Insert a row
        db.insert_row(
            "products",
            Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(50)]),
        )
        .unwrap();

        // Update to NULL (should succeed - NULL is treated as UNKNOWN which passes CHECK)
        let stmt = UpdateStmt {
            table_name: "products".to_string(),
            assignments: vec![Assignment {
                column: "price".to_string(),
                value: Expression::Literal(SqlValue::Null),
            }],
            where_clause: None,
        };

        let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_update_check_constraint_with_expression() {
        let mut db = Database::new();

        // CREATE TABLE employees (id INT, salary INT, bonus INT, CHECK (bonus < salary))
        let schema = catalog::TableSchema::with_all_constraint_types(
            "employees".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), DataType::Integer, false),
                catalog::ColumnSchema::new("salary".to_string(), DataType::Integer, false),
                catalog::ColumnSchema::new("bonus".to_string(), DataType::Integer, false),
            ],
            None,
            Vec::new(),
            vec![(
                "bonus_less_than_salary".to_string(),
                ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::ColumnRef {
                        table: None,
                        column: "bonus".to_string(),
                    }),
                    op: ast::BinaryOperator::LessThan,
                    right: Box::new(ast::Expression::ColumnRef {
                        table: None,
                        column: "salary".to_string(),
                    }),
                },
            )],
        );
        db.create_table(schema).unwrap();

        // Insert a row
        db.insert_row(
            "employees",
            Row::new(vec![
                SqlValue::Integer(1),
                SqlValue::Integer(50000),
                SqlValue::Integer(10000),
            ]),
        )
        .unwrap();

        // Update bonus to still be less than salary (should succeed)
        let stmt1 = UpdateStmt {
            table_name: "employees".to_string(),
            assignments: vec![Assignment {
                column: "bonus".to_string(),
                value: Expression::Literal(SqlValue::Integer(15000)),
            }],
            where_clause: None,
        };
        let count = UpdateExecutor::execute(&stmt1, &mut db).unwrap();
        assert_eq!(count, 1);

        // Try to update bonus to be >= salary (should fail)
        let stmt2 = UpdateStmt {
            table_name: "employees".to_string(),
            assignments: vec![Assignment {
                column: "bonus".to_string(),
                value: Expression::Literal(SqlValue::Integer(60000)),
            }],
            where_clause: None,
        };

        let result = UpdateExecutor::execute(&stmt2, &mut db);
        assert!(result.is_err());
        match result.unwrap_err() {
            ExecutorError::ConstraintViolation(msg) => {
                assert!(msg.contains("CHECK"));
                assert!(msg.contains("bonus_less_than_salary"));
            }
            other => panic!("Expected ConstraintViolation, got {:?}", other),
        }
    }
}
