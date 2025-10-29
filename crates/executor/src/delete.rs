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

        // Step 1: Get schema (clone to avoid borrow issues)
        let schema = database
            .catalog
            .get_table(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?
            .clone();

        // Step 2: Evaluate WHERE clause and collect rows to delete (two-phase execution)
        let rows_to_delete: Vec<storage::Row> = {
            let table = database
                .get_table(&stmt.table_name)
                .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

            // Create evaluator with database reference for subquery support
            let evaluator = ExpressionEvaluator::with_database(&schema, database);

            table
                .scan()
                .iter()
                .filter(|row| {
                    if let Some(ref where_expr) = stmt.where_clause {
                        match evaluator.eval(where_expr, row) {
                            Ok(types::SqlValue::Boolean(true)) => true,
                            _ => false,
                        }
                    } else {
                        true
                    }
                })
                .cloned()
                .collect()
        }; // evaluator dropped here, releasing immutable borrow

        // Step 3: Check referential integrity for each row to be deleted
        for row in &rows_to_delete {
            check_no_child_references(database, &stmt.table_name, row)?;
        }

        // Step 4: Actually delete the rows (now we can borrow mutably)
        let table_mut = database
            .get_table_mut(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Delete rows by matching against our collected list
        let deleted_count = table_mut.delete_where(|row| {
            rows_to_delete.iter().any(|to_delete| {
                // Compare all column values
                row.values == to_delete.values
            })
        });

        Ok(deleted_count)
    }
}

/// Check that no child tables reference a row that is about to be deleted or updated.
fn check_no_child_references(
    db: &storage::Database,
    parent_table_name: &str,
    parent_row: &storage::Row,
) -> Result<(), ExecutorError> {
    let parent_schema = db
        .catalog
        .get_table(parent_table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(parent_table_name.to_string()))?;

    // This check is only meaningful if the parent table has a primary key.
    let pk_indices = match parent_schema.get_primary_key_indices() {
        Some(indices) => indices,
        None => return Ok(()),
    };

    let parent_key_values: Vec<types::SqlValue> = pk_indices
        .iter()
        .map(|&idx| parent_row.values[idx].clone())
        .collect();

    // Scan all tables in the database to find foreign keys that reference this table.
    for table_name in db.catalog.list_tables() {
        let child_schema = db.catalog.get_table(&table_name).unwrap();

        for fk in &child_schema.foreign_keys {
            if fk.parent_table != parent_table_name {
                continue;
            }

            // Check if any row in the child table references the parent row.
            let child_table = db.get_table(&table_name).unwrap();
            let has_references = child_table.scan().iter().any(|child_row| {
                let child_fk_values: Vec<types::SqlValue> = fk
                    .column_indices
                    .iter()
                    .map(|&idx| child_row.values[idx].clone())
                    .collect();
                child_fk_values == parent_key_values
            });

            if has_references {
                return Err(ExecutorError::ConstraintViolation(format!(
                    "FOREIGN KEY constraint violation: cannot delete or update a parent row when a foreign key constraint exists. The conflict occurred in table \'{}\', constraint \'{}\'.",
                    table_name,
                    fk.name.as_deref().unwrap_or(""),
                )));
            }
        }
    }

    Ok(())
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
        let stmt = DeleteStmt { table_name: "users".to_string(), where_clause: None };

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
                left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
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
            .map(|row| if let SqlValue::Integer(id) = row.get(0).unwrap() { *id } else { 0 })
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
                left: Box::new(Expression::ColumnRef { table: None, column: "active".to_string() }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Boolean(true))),
            }),
        };

        let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(deleted, 2); // Alice and Charlie

        // Verify only Bob remains
        let table = db.get_table("users").unwrap();
        assert_eq!(table.row_count(), 1);

        let remaining_id =
            if let SqlValue::Integer(id) = table.scan()[0].get(0).unwrap() { *id } else { 0 };
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
                left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
        };

        let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(deleted, 2); // Bob and Charlie

        // Verify only Alice remains
        let table = db.get_table("users").unwrap();
        assert_eq!(table.row_count(), 1);

        let remaining_name = if let SqlValue::Varchar(name) = table.scan()[0].get(1).unwrap() {
            name.clone()
        } else {
            String::new()
        };
        assert_eq!(remaining_name, "Alice");
    }

    #[test]
    fn test_delete_table_not_found() {
        let mut db = Database::new();

        let stmt = DeleteStmt { table_name: "nonexistent".to_string(), where_clause: None };

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
                left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
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
        let stmt = DeleteStmt { table_name: "empty_users".to_string(), where_clause: None };

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
