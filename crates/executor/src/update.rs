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
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(999))),
            }),
        };

        let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(count, 0); // No rows matched
    }
}
