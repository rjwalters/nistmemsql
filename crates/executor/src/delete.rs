//! DELETE statement execution

use ast::DeleteStmt;
use storage::Database;

use crate::errors::ExecutorError;
use crate::evaluator::ExpressionEvaluator;

/// Executor for DELETE statements
pub struct DeleteExecutor;

impl DeleteExecutor {
    /// Execute a DELETE statement
    ///
    /// # Arguments
    ///
    /// * `stmt` - The DELETE statement AST node
    /// * `database` - The database to delete from
    ///
    /// # Returns
    ///
    /// Number of rows deleted or error
    ///
    /// # Examples
    ///
    /// ```
    /// use ast::{DeleteStmt, Expression, BinaryOperator};
    /// use types::SqlValue;
    /// use storage::Database;
    /// use catalog::{TableSchema, ColumnSchema};
    /// use types::DataType;
    /// use executor::DeleteExecutor;
    ///
    /// let mut db = Database::new();
    ///
    /// // Create table
    /// let schema = TableSchema::new(
    ///     "users".to_string(),
    ///     vec![
    ///         ColumnSchema::new("id".to_string(), DataType::Integer, false),
    ///         ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: 50 }, false),
    ///     ],
    /// );
    /// db.create_table(schema).unwrap();
    ///
    /// // Insert rows
    /// db.insert_row("users", storage::Row::new(vec![
    ///     SqlValue::Integer(1),
    ///     SqlValue::Varchar("Alice".to_string()),
    /// ])).unwrap();
    /// db.insert_row("users", storage::Row::new(vec![
    ///     SqlValue::Integer(2),
    ///     SqlValue::Varchar("Bob".to_string()),
    /// ])).unwrap();
    ///
    /// // Delete specific row
    /// let stmt = DeleteStmt {
    ///     table_name: "users".to_string(),
    ///     where_clause: Some(Expression::BinaryOp {
    ///         left: Box::new(Expression::ColumnRef {
    ///             table: None,
    ///             column: "id".to_string(),
    ///         }),
    ///         op: BinaryOperator::Equal,
    ///         right: Box::new(Expression::Literal(SqlValue::Integer(1))),
    ///     }),
    /// };
    ///
    /// let count = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    /// assert_eq!(count, 1);
    /// ```
    pub fn execute(stmt: &DeleteStmt, database: &mut Database) -> Result<usize, ExecutorError> {
        // Check table exists
        if !database.catalog.table_exists(&stmt.table_name) {
            return Err(ExecutorError::TableNotFound(stmt.table_name.clone()));
        }

        // Get mutable table reference
        let table = database
            .get_table_mut(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Case 1: No WHERE clause - delete all rows
        if stmt.where_clause.is_none() {
            let deleted_count = table.row_count();
            table.clear();
            return Ok(deleted_count);
        }

        // Case 2: WHERE clause - filter and delete matching rows
        let where_expr = stmt.where_clause.as_ref().unwrap();

        // Clone schema to avoid borrow issues with delete_where closure
        let schema = table.schema.clone();
        let evaluator = ExpressionEvaluator::new(&schema);

        // Delete rows where WHERE clause evaluates to TRUE
        let deleted_count = table.delete_where(|row| {
            // Evaluate WHERE clause for this row
            match evaluator.eval(where_expr, row) {
                Ok(types::SqlValue::Boolean(true)) => true,  // Delete this row
                Ok(types::SqlValue::Boolean(false)) => false, // Keep this row
                Ok(types::SqlValue::Null) => false,          // NULL = false (SQL semantics)
                Ok(_) => false,                              // Non-boolean = keep row
                Err(_) => false,                             // Error = keep row (safe default)
            }
        });

        Ok(deleted_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::{BinaryOperator, Expression};
    use catalog::{ColumnSchema, TableSchema};
    use storage::Row;
    use types::{DataType, SqlValue};

    fn setup_test_table(db: &mut Database) {
        // Create table schema
        let schema = TableSchema::new(
            "users".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: 50 }, false),
                ColumnSchema::new("active".to_string(), DataType::Boolean, false),
            ],
        );

        db.create_table(schema).unwrap();

        // Insert test data
        db.insert_row(
            "users",
            Row::new(vec![
                SqlValue::Integer(1),
                SqlValue::Varchar("Alice".to_string()),
                SqlValue::Boolean(true),
            ]),
        )
        .unwrap();

        db.insert_row(
            "users",
            Row::new(vec![
                SqlValue::Integer(2),
                SqlValue::Varchar("Bob".to_string()),
                SqlValue::Boolean(false),
            ]),
        )
        .unwrap();

        db.insert_row(
            "users",
            Row::new(vec![
                SqlValue::Integer(3),
                SqlValue::Varchar("Charlie".to_string()),
                SqlValue::Boolean(true),
            ]),
        )
        .unwrap();
    }

    #[test]
    fn test_delete_all_rows() {
        let mut db = Database::new();
        setup_test_table(&mut db);

        // DELETE FROM users;
        let stmt = DeleteStmt {
            table_name: "users".to_string(),
            where_clause: None,
        };

        let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(deleted, 3);

        let table = db.get_table("users").unwrap();
        assert_eq!(table.row_count(), 0);
    }

    #[test]
    fn test_delete_with_simple_where() {
        let mut db = Database::new();
        setup_test_table(&mut db);

        // DELETE FROM users WHERE id = 2;
        let stmt = DeleteStmt {
            table_name: "users".to_string(),
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            }),
        };

        let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(deleted, 1);

        // Verify Bob is deleted, Alice and Charlie remain
        let table = db.get_table("users").unwrap();
        assert_eq!(table.row_count(), 2);

        let remaining: Vec<i64> = table
            .scan()
            .iter()
            .map(|row| {
                if let SqlValue::Integer(id) = row.get(0).unwrap() {
                    *id
                } else {
                    0
                }
            })
            .collect();

        assert!(remaining.contains(&1)); // Alice
        assert!(remaining.contains(&3)); // Charlie
        assert!(!remaining.contains(&2)); // Bob deleted
    }

    #[test]
    fn test_delete_with_boolean_where() {
        let mut db = Database::new();
        setup_test_table(&mut db);

        // DELETE FROM users WHERE active = TRUE;
        let stmt = DeleteStmt {
            table_name: "users".to_string(),
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "active".to_string(),
                }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Boolean(true))),
            }),
        };

        let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(deleted, 2); // Alice and Charlie

        // Verify only Bob remains
        let table = db.get_table("users").unwrap();
        assert_eq!(table.row_count(), 1);

        let remaining_id = if let SqlValue::Integer(id) = table.scan()[0].get(0).unwrap() {
            *id
        } else {
            0
        };
        assert_eq!(remaining_id, 2); // Bob
    }

    #[test]
    fn test_delete_multiple_rows() {
        let mut db = Database::new();
        setup_test_table(&mut db);

        // DELETE FROM users WHERE id > 1;
        let stmt = DeleteStmt {
            table_name: "users".to_string(),
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                }),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
        };

        let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(deleted, 2); // Bob and Charlie

        // Verify only Alice remains
        let table = db.get_table("users").unwrap();
        assert_eq!(table.row_count(), 1);

        let remaining_name =
            if let SqlValue::Varchar(name) = table.scan()[0].get(1).unwrap() {
                name.clone()
            } else {
                String::new()
            };
        assert_eq!(remaining_name, "Alice");
    }

    #[test]
    fn test_delete_table_not_found() {
        let mut db = Database::new();

        let stmt = DeleteStmt {
            table_name: "nonexistent".to_string(),
            where_clause: None,
        };

        let result = DeleteExecutor::execute(&stmt, &mut db);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ExecutorError::TableNotFound(_)));
    }

    #[test]
    fn test_delete_no_matching_rows() {
        let mut db = Database::new();
        setup_test_table(&mut db);

        // DELETE FROM users WHERE id = 999;
        let stmt = DeleteStmt {
            table_name: "users".to_string(),
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(999))),
            }),
        };

        let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(deleted, 0);

        // All rows should still exist
        let table = db.get_table("users").unwrap();
        assert_eq!(table.row_count(), 3);
    }

    #[test]
    fn test_delete_from_empty_table() {
        let mut db = Database::new();

        // Create empty table
        let schema = TableSchema::new(
            "empty_users".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
        );
        db.create_table(schema).unwrap();

        // DELETE FROM empty_users;
        let stmt = DeleteStmt {
            table_name: "empty_users".to_string(),
            where_clause: None,
        };

        let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(deleted, 0);
    }

    #[test]
    fn test_delete_column_not_found() {
        let mut db = Database::new();
        setup_test_table(&mut db);

        // DELETE FROM users WHERE nonexistent_column = 1;
        let stmt = DeleteStmt {
            table_name: "users".to_string(),
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "nonexistent_column".to_string(),
                }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
        };

        // Error should be caught during evaluation, rows kept (safe default)
        let result = DeleteExecutor::execute(&stmt, &mut db);

        // Should succeed with 0 deletions (errors kept rows safe)
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);

        // All rows should still exist
        let table = db.get_table("users").unwrap();
        assert_eq!(table.row_count(), 3);
    }
}
