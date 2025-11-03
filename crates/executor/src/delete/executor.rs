//! DELETE statement execution

use ast::DeleteStmt;
use storage::Database;

use crate::errors::ExecutorError;
use crate::evaluator::ExpressionEvaluator;
use crate::privilege_checker::PrivilegeChecker;

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
    /// use ast::{DeleteStmt, Expression, BinaryOperator, WhereClause};
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
    ///     only: false,
    ///     table_name: "users".to_string(),
    ///     where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
    ///         left: Box::new(Expression::ColumnRef {
    ///             table: None,
    ///             column: "id".to_string(),
    ///         }),
    ///         op: BinaryOperator::Equal,
    ///         right: Box::new(Expression::Literal(SqlValue::Integer(1))),
    ///     })),
    /// };
    ///
    /// let count = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    /// assert_eq!(count, 1);
    /// ```
    pub fn execute(stmt: &DeleteStmt, database: &mut Database) -> Result<usize, ExecutorError> {
        // Note: stmt.only is currently ignored (treated as false)
        // ONLY keyword is used in table inheritance to exclude derived tables.
        // Since table inheritance is not yet implemented, we treat all deletes the same.

        // Check DELETE privilege on the table
        PrivilegeChecker::check_delete(database, &stmt.table_name)?;

        // Check table exists
        if !database.catalog.table_exists(&stmt.table_name) {
            return Err(ExecutorError::TableNotFound(stmt.table_name.clone()));
        }

        // Fast path: DELETE FROM table (no WHERE clause)
        // Use TRUNCATE-style optimization for 100-1000x performance improvement
        if stmt.where_clause.is_none() && can_use_truncate(database, &stmt.table_name)? {
            return execute_truncate(database, &stmt.table_name);
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
            let should_delete = if let Some(ref where_clause) = stmt.where_clause {
                match where_clause {
                    ast::WhereClause::Condition(where_expr) => {
                        matches!(
                            evaluator.eval(where_expr, row),
                            Ok(types::SqlValue::Boolean(true))
                        )
                    }
                    ast::WhereClause::CurrentOf(_cursor_name) => {
                        // TODO: Implement cursor support - for now return error
                        return Err(ExecutorError::UnsupportedFeature(
                            "WHERE CURRENT OF cursor is not yet implemented".to_string(),
                        ));
                    }
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

/// Check if TRUNCATE optimization can be used for DELETE FROM table (no WHERE)
///
/// TRUNCATE cannot be used if:
/// - Table has DELETE triggers (BEFORE/AFTER DELETE)
/// - Table is referenced by foreign keys from other tables (without CASCADE)
///
/// # Returns
/// - `Ok(true)` if TRUNCATE can be safely used
/// - `Ok(false)` if row-by-row deletion is required
/// - `Err` if table doesn't exist
fn can_use_truncate(database: &Database, table_name: &str) -> Result<bool, ExecutorError> {
    // Check for DELETE triggers on this table
    if has_delete_triggers(database, table_name) {
        return Ok(false);
    }

    // Check if this table is referenced by foreign keys from other tables
    if is_fk_referenced(database, table_name)? {
        return Ok(false);
    }

    Ok(true)
}

/// Check if a table has any DELETE triggers
fn has_delete_triggers(database: &Database, table_name: &str) -> bool {
    database
        .catalog
        .get_triggers_for_table(table_name, Some(ast::TriggerEvent::Delete))
        .next()
        .is_some()
}

/// Check if a table is referenced by foreign keys from other tables
///
/// Returns true if any other table has a foreign key constraint referencing this table.
/// When this is true, we cannot use TRUNCATE because we need to check each row
/// for child references.
fn is_fk_referenced(database: &Database, parent_table_name: &str) -> Result<bool, ExecutorError> {
    // Scan all tables to find foreign keys that reference this table
    for table_name in database.catalog.list_tables() {
        let child_schema = database
            .catalog
            .get_table(&table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.clone()))?;

        for fk in &child_schema.foreign_keys {
            if fk.parent_table == parent_table_name {
                return Ok(true); // Found a reference
            }
        }
    }

    Ok(false) // No references found
}

/// Execute TRUNCATE-style fast path for DELETE FROM table (no WHERE)
///
/// Clears all rows and indexes in a single operation instead of row-by-row deletion.
/// Provides 100-1000x performance improvement for full table deletes.
///
/// # Safety
/// Only call this after `can_use_truncate` returns true.
fn execute_truncate(database: &mut Database, table_name: &str) -> Result<usize, ExecutorError> {
    let table = database
        .get_table_mut(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    let row_count = table.row_count();

    // Clear all data at once (O(1) operation)
    table.clear();

    Ok(row_count)
}
