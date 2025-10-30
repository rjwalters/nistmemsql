///! Main evaluation entry point for combined expressions

use crate::errors::ExecutorError;
use crate::select::WindowFunctionKey;
use super::super::core::{CombinedExpressionEvaluator, ExpressionEvaluator};

impl<'a> CombinedExpressionEvaluator<'a> {
    /// Evaluate an expression in the context of a combined row
    /// This is the main entry point for expression evaluation
    pub(crate) fn eval(
        &self,
        expr: &ast::Expression,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        match expr {
            // Literals - just return the value
            ast::Expression::Literal(val) => Ok(val.clone()),

            // Column reference - look up column index (with optional table qualifier)
            ast::Expression::ColumnRef { table, column } => {
                eprintln!("DEBUG CombinedExpr ColumnRef: table={:?}, column={}, inner_schema_tables={:?}",
                         table, column, self.schema.table_schemas.keys().collect::<Vec<_>>());

                // Try to resolve in inner schema first
                if let Some(col_index) = self.schema.get_column_index(table.as_deref(), column) {
                    eprintln!("DEBUG CombinedExpr: Found in INNER schema at index {}", col_index);
                    return row.get(col_index)
                        .cloned()
                        .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
                }

                // If not found in inner schema and outer context exists, try outer schema
                if let (Some(outer_row), Some(outer_schema)) = (self.outer_row, self.outer_schema) {
                    eprintln!("DEBUG CombinedExpr: Not in inner, checking OUTER schema, outer_tables={:?}",
                             outer_schema.table_schemas.keys().collect::<Vec<_>>());
                    if let Some(col_index) = outer_schema.get_column_index(table.as_deref(), column) {
                        eprintln!("DEBUG CombinedExpr: Found in OUTER schema at index {}", col_index);
                        return outer_row.get(col_index)
                            .cloned()
                            .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
                    } else {
                        eprintln!("DEBUG CombinedExpr: NOT FOUND in outer schema");
                    }
                } else {
                    eprintln!("DEBUG CombinedExpr: No outer context available");
                }

                // Column not found in either schema
                eprintln!("DEBUG CombinedExpr: Column NOT FOUND in any schema");
                Err(ExecutorError::ColumnNotFound(column.clone()))
            }

            // Binary operations
            ast::Expression::BinaryOp { left, op, right } => {
                let left_val = self.eval(left, row)?;
                let right_val = self.eval(right, row)?;
                ExpressionEvaluator::eval_binary_op_static(&left_val, op, &right_val)
            }

            // CASE expression
            ast::Expression::Case { operand, when_clauses, else_result } => {
                self.eval_case(operand, when_clauses, else_result, row)
            }

            // IN operator with subquery
            ast::Expression::In { expr, subquery, negated } => {
                self.eval_in_subquery(expr, subquery, *negated, row)
            }

            // Scalar subquery - must return exactly one row and one column
            ast::Expression::ScalarSubquery(subquery) => {
                self.eval_scalar_subquery(subquery, row)
            }

            // BETWEEN predicate: expr BETWEEN low AND high
            ast::Expression::Between { expr, low, high, negated } => {
                self.eval_between(expr, low, high, *negated, row)
            }

            // CAST expression: CAST(expr AS data_type)
            ast::Expression::Cast { expr, data_type } => {
                self.eval_cast(expr, data_type, row)
            }

            // LIKE pattern matching: expr LIKE pattern
            ast::Expression::Like { expr, pattern, negated } => {
                self.eval_like(expr, pattern, *negated, row)
            }

            // IN operator with value list: expr IN (val1, val2, ...)
            ast::Expression::InList { expr, values, negated } => {
                self.eval_in_list(expr, values, *negated, row)
            }

            // EXISTS predicate: EXISTS (SELECT ...)
            ast::Expression::Exists { subquery, negated } => {
                self.eval_exists(subquery, *negated, row)
            }

            // Quantified comparison: expr op ALL/ANY/SOME (SELECT ...)
            ast::Expression::QuantifiedComparison { expr, op, quantifier, subquery } => {
                self.eval_quantified(expr, op, quantifier, subquery, row)
            }

            // IS NULL / IS NOT NULL
            ast::Expression::IsNull { expr, negated } => {
                self.eval_is_null(expr, *negated, row)
            }

            // Function expressions - handle scalar functions (not aggregates)
            ast::Expression::Function { name, args } => {
                self.eval_function(name, args, row)
            }

            // Unary operations (delegate to shared function)
            ast::Expression::UnaryOp { op, expr } => {
                self.eval_unary(op, expr, row)
            }

            // Window functions - look up pre-computed values
            ast::Expression::WindowFunction { function, over } => {
                if let Some(mapping) = self.window_mapping {
                    let key = WindowFunctionKey::from_expression(function, over);
                    if let Some(&col_idx) = mapping.get(&key) {
                        // Extract the pre-computed value from the appended column
                        let value = row.values
                            .get(col_idx)
                            .cloned()
                            .ok_or_else(|| ExecutorError::ColumnIndexOutOfBounds { index: col_idx })?;
                        Ok(value)
                    } else {
                        Err(ExecutorError::UnsupportedExpression(
                            format!("Window function not found in mapping: {:?}", expr),
                        ))
                    }
                } else {
                    Err(ExecutorError::UnsupportedExpression(
                        "Window functions require window mapping context".to_string(),
                    ))
                }
            }

            // TRIM expression - handle position and custom removal character
            ast::Expression::Trim { position, removal_char, string } => {
                let string_val = self.eval(string, row)?;
                let removal_val = if let Some(removal) = removal_char {
                    Some(self.eval(removal, row)?)
                } else {
                    None
                };
                super::super::functions::string::trim_advanced(string_val, position.clone(), removal_val)
            }

            // Unsupported expressions
            _ => Err(ExecutorError::UnsupportedExpression(format!("{:?}", expr))),
        }
    }
}
