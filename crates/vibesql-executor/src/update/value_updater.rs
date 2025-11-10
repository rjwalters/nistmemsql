//! Value update logic for UPDATE operations

use std::collections::HashSet;

use vibesql_ast::Assignment;

use crate::{errors::ExecutorError, evaluator::ExpressionEvaluator};

/// Applies assignment expressions to rows
pub struct ValueUpdater<'a> {
    schema: &'a vibesql_catalog::TableSchema,
    evaluator: &'a ExpressionEvaluator<'a>,
    table_name: &'a str,
}

impl<'a> ValueUpdater<'a> {
    /// Create a new value updater
    pub fn new(
        schema: &'a vibesql_catalog::TableSchema,
        evaluator: &'a ExpressionEvaluator<'a>,
        table_name: &'a str,
    ) -> Self {
        Self { schema, evaluator, table_name }
    }

    /// Apply assignments to a row
    ///
    /// Returns the updated row and a set of changed column indices.
    /// Expressions are evaluated against the original row (two-phase semantics).
    pub fn apply_assignments(
        &self,
        original_row: &vibesql_storage::Row,
        assignments: &[Assignment],
    ) -> Result<(vibesql_storage::Row, HashSet<usize>), ExecutorError> {
        let mut new_row = original_row.clone();
        let mut changed_columns = HashSet::new();

        // Apply each assignment
        for assignment in assignments {
            // Find column index
            let col_index = self.schema.get_column_index(&assignment.column).ok_or_else(|| {
                ExecutorError::ColumnNotFound {
                    column_name: assignment.column.clone(),
                    table_name: self.table_name.to_string(),
                    searched_tables: vec![self.table_name.to_string()],
                    available_columns: self.schema.columns.iter().map(|c| c.name.clone()).collect(),
                }
            })?;

            // Evaluate new value expression
            // Handle DEFAULT specially before evaluating other expressions
            let new_value = match &assignment.value {
                vibesql_ast::Expression::Default => {
                    // Use column's default value, or NULL if no default is defined
                    let column = &self.schema.columns[col_index];
                    if let Some(default_expr) = &column.default_value {
                        // Evaluate the default expression (currently only supports literals)
                        match default_expr {
                            vibesql_ast::Expression::Literal(lit) => lit.clone(),
                            _ => {
                                return Err(ExecutorError::UnsupportedExpression(format!(
                                    "Complex default expressions not yet supported for column '{}'",
                                    column.name
                                )))
                            }
                        }
                    } else {
                        // No default value defined, use NULL
                        vibesql_types::SqlValue::Null
                    }
                }
                _ => {
                    // Evaluate other expressions against ORIGINAL row
                    self.evaluator.eval(&assignment.value, original_row)?
                }
            };

            // Update column in new row
            new_row
                .set(col_index, new_value)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

            // Track that this column changed
            changed_columns.insert(col_index);
        }

        Ok((new_row, changed_columns))
    }
}
