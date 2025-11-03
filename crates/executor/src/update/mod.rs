//! UPDATE statement execution
//!
//! This module provides UPDATE statement execution with the following architecture:
//!
//! - `row_selector`: Handles WHERE clause evaluation and primary key index optimization
//! - `value_updater`: Applies assignment expressions to rows
//! - `constraints`: Validates NOT NULL, PRIMARY KEY, UNIQUE, and CHECK constraints
//! - `foreign_keys`: Validates foreign key constraints and child references
//!
//! The main `UpdateExecutor` orchestrates these components to implement SQL's two-phase
//! update semantics: first collect all updates evaluating against original rows, then
//! apply all updates atomically.

mod constraints;
mod foreign_keys;
mod row_selector;
mod value_updater;

use ast::UpdateStmt;
use storage::Database;

use crate::errors::ExecutorError;
use crate::evaluator::ExpressionEvaluator;
use crate::privilege_checker::PrivilegeChecker;

use constraints::ConstraintValidator;
use foreign_keys::ForeignKeyValidator;
use row_selector::RowSelector;
use value_updater::ValueUpdater;

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
        Self::execute_with_schema(stmt, database, None)
    }

    /// Execute an UPDATE statement with optional pre-fetched schema
    ///
    /// This method allows cursor-level schema caching to reduce redundant catalog lookups.
    /// If schema is provided, skips the catalog lookup step.
    ///
    /// # Arguments
    ///
    /// * `stmt` - The UPDATE statement AST node
    /// * `database` - The database to update
    /// * `schema` - Optional pre-fetched schema (from cursor cache)
    ///
    /// # Returns
    ///
    /// Number of rows updated or error
    pub fn execute_with_schema(
        stmt: &UpdateStmt,
        database: &mut Database,
        schema: Option<&catalog::TableSchema>,
    ) -> Result<usize, ExecutorError> {
        // Check UPDATE privilege on the table
        PrivilegeChecker::check_update(database, &stmt.table_name)?;

        // Step 1: Get table schema - use provided schema or fetch from catalog
        let schema = if let Some(s) = schema {
            s
        } else {
            database
                .catalog
                .get_table(&stmt.table_name)
                .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?
        };

        // Step 2: Get table from storage (for reading rows)
        let table = database
            .get_table(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Step 3: Create expression evaluator with database reference for subquery support
        let evaluator = ExpressionEvaluator::with_database(schema, database);

        // Step 4: Select rows to update using RowSelector
        let row_selector = RowSelector::new(schema, &evaluator);
        let candidate_rows = row_selector.select_rows(table, &stmt.where_clause)?;

        // Step 5: Create value updater
        let value_updater = ValueUpdater::new(schema, &evaluator, &stmt.table_name);

        // Step 6: Build list of updates (two-phase execution for SQL semantics)
        // Each update consists of: (row_index, new_row, changed_columns)
        let mut updates: Vec<(usize, storage::Row, std::collections::HashSet<usize>)> = Vec::new();

        for (row_index, row) in candidate_rows {
            // If the primary key is being updated, we need to check for child references
            if let Some(pk_indices) = schema.get_primary_key_indices() {
                let updates_pk = stmt.assignments.iter().any(|a| {
                    let col_index = schema.get_column_index(&a.column).unwrap();
                    pk_indices.contains(&col_index)
                });

                if updates_pk {
                    ForeignKeyValidator::check_no_child_references(database, &stmt.table_name, &row)?;
                }
            }

            // Apply assignments to build updated row
            let (new_row, changed_columns) = value_updater.apply_assignments(&row, &stmt.assignments)?;

            // Validate all constraints (NOT NULL, PRIMARY KEY, UNIQUE, CHECK)
            let constraint_validator = ConstraintValidator::new(schema);
            constraint_validator.validate_row(table, &stmt.table_name, row_index, &new_row, &row)?;

            // Enforce FOREIGN KEY constraints (child table)
            if !schema.foreign_keys.is_empty() {
                ForeignKeyValidator::validate_constraints(database, &stmt.table_name, &new_row.values)?;
            }

            updates.push((row_index, new_row, changed_columns));
        }

        // Step 5: Apply all updates (after evaluation phase completes)
        let update_count = updates.len();

        // Get mutable table reference
        let table_mut = database
            .get_table_mut(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        for (index, new_row, changed_columns) in updates {
            table_mut
                .update_row_selective(index, new_row, &changed_columns)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
        }

        Ok(update_count)
    }
}
