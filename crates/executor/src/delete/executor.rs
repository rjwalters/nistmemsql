//! DELETE statement execution

use ast::DeleteStmt;
use storage::Database;

use crate::errors::ExecutorError;
use crate::evaluator::ExpressionEvaluator;

use super::integrity::check_no_child_references;

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
    ///         ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(50) }, false),
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
        // Get table for scanning
        let table = database
            .get_table(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Create evaluator with database reference for subquery support (EXISTS, NOT EXISTS, IN with subquery, etc.)
        let evaluator = ExpressionEvaluator::with_database(&schema, database);

        // Find rows to delete and their indices
        let mut rows_and_indices_to_delete: Vec<(usize, storage::Row)> = Vec::new();
        for (index, row) in table.scan().iter().enumerate() {
            let should_delete = if let Some(ref where_expr) = stmt.where_clause {
                match evaluator.eval(where_expr, row) {
                    Ok(types::SqlValue::Boolean(true)) => true,
                    _ => false,
                }
            } else {
                true
            };

            if should_delete {
                rows_and_indices_to_delete.push((index, row.clone()));
            }
        }

        // Step 3: Check referential integrity for each row to be deleted
        for (_, row) in &rows_and_indices_to_delete {
            check_no_child_references(database, &stmt.table_name, row)?;
        }

        // Extract just the indices
        let indices_to_delete: std::collections::HashSet<usize> =
            rows_and_indices_to_delete.iter().map(|(idx, _)| *idx).collect();

        // Drop evaluator to release database borrow
        drop(evaluator);

        // Step 4: Actually delete the rows (now we can borrow mutably)
        let table_mut = database
            .get_table_mut(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Delete rows using the pre-computed indices
        use std::cell::Cell;
        let current_index = Cell::new(0);
        let deleted_count = table_mut.delete_where(|_row| {
            let index = current_index.get();
            let should_delete = indices_to_delete.contains(&index);
            current_index.set(index + 1);
            should_delete
        });

        Ok(deleted_count)
    }
}
