//! Main evaluation entry point for combined expressions

use super::super::core::{CombinedExpressionEvaluator, ExpressionEvaluator};
use crate::errors::ExecutorError;
use crate::select::WindowFunctionKey;

impl CombinedExpressionEvaluator<'_> {
    /// Evaluate an expression in the context of a combined row
    /// This is the main entry point for expression evaluation
    pub(crate) fn eval(
        &self,
        expr: &ast::Expression,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        // Check depth limit to prevent stack overflow from deeply nested expressions
        if self.depth >= crate::limits::MAX_EXPRESSION_DEPTH {
            return Err(ExecutorError::ExpressionDepthExceeded {
                depth: self.depth,
                max_depth: crate::limits::MAX_EXPRESSION_DEPTH,
            });
        }

        // Increment depth for recursive calls
        self.with_incremented_depth(|evaluator| evaluator.eval_impl(expr, row))
    }

    /// Internal implementation of eval with depth already incremented
    fn eval_impl(
        &self,
        expr: &ast::Expression,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        match expr {
            // Literals - just return the value
            ast::Expression::Literal(val) => Ok(val.clone()),

            // DEFAULT keyword - not allowed in UPDATE/SELECT expressions
            // DEFAULT is only valid in INSERT VALUES and UPDATE SET
            // This evaluator is used for SELECT and WHERE clauses where DEFAULT is invalid
            ast::Expression::Default => Err(ExecutorError::UnsupportedExpression(
                "DEFAULT keyword is only valid in INSERT VALUES and UPDATE SET clauses".to_string(),
            )),

            // Column reference - look up column index (with optional table qualifier)
            ast::Expression::ColumnRef { table, column } => {
                // Try to resolve in inner schema first
                if let Some(col_index) = self.get_column_index_cached(table.as_deref(), column) {
                    return row
                        .get(col_index)
                        .cloned()
                        .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
                }

                // If not found in inner schema and outer context exists, try outer schema
                if let (Some(outer_row), Some(outer_schema)) = (self.outer_row, self.outer_schema) {
                    if let Some(col_index) = outer_schema.get_column_index(table.as_deref(), column)
                    {
                        return outer_row
                            .get(col_index)
                            .cloned()
                            .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
                    }
                }

                // Column not found in either schema - collect diagnostic info
                let searched_tables: Vec<String> = self.schema.table_schemas.keys().cloned().collect();
                let mut available_columns = Vec::new();
                for (_start, schema) in self.schema.table_schemas.values() {
                    available_columns.extend(schema.columns.iter().map(|c| c.name.clone()));
                }
                if let Some(outer_schema) = self.outer_schema {
                    for (_start, schema) in outer_schema.table_schemas.values() {
                        available_columns.extend(schema.columns.iter().map(|c| c.name.clone()));
                    }
                }

                Err(ExecutorError::ColumnNotFound {
                    column_name: column.clone(),
                    table_name: table.clone().unwrap_or_else(|| "unknown".to_string()),
                    searched_tables,
                    available_columns,
                })
            }

            // Binary operations
            ast::Expression::BinaryOp { left, op, right } => {
                use types::SqlValue;

                // Short-circuit evaluation for AND/OR operators
                match op {
                    ast::BinaryOperator::And => {
                        let left_val = self.eval(left, row)?;
                        // Short-circuit: if left is false, return false immediately
                        match left_val {
                            SqlValue::Boolean(false) => return Ok(SqlValue::Boolean(false)),
                            // For NULL and TRUE, must evaluate right side
                            // SQL three-valued logic:
                            // - NULL AND FALSE = FALSE (not NULL!)
                            // - NULL AND TRUE = NULL
                            // - TRUE AND x = x
                            _ => {
                                let right_val = self.eval(right, row)?;

                                // Special case: NULL AND FALSE = FALSE
                                if matches!(left_val, SqlValue::Null) && matches!(right_val, SqlValue::Boolean(false)) {
                                    return Ok(SqlValue::Boolean(false));
                                }

                                ExpressionEvaluator::eval_binary_op_static(&left_val, op, &right_val)
                            }
                        }
                    }
                    ast::BinaryOperator::Or => {
                        let left_val = self.eval(left, row)?;
                        // Short-circuit: if left is true, return true immediately
                        match left_val {
                            SqlValue::Boolean(true) => return Ok(SqlValue::Boolean(true)),
                            // For NULL and FALSE, must evaluate right side
                            // SQL three-valued logic:
                            // - NULL OR TRUE = TRUE (not NULL!)
                            // - NULL OR FALSE = NULL
                            // - FALSE OR x = x
                            _ => {
                                let right_val = self.eval(right, row)?;

                                // Special case: NULL OR TRUE = TRUE
                                if matches!(left_val, SqlValue::Null) && matches!(right_val, SqlValue::Boolean(true)) {
                                    return Ok(SqlValue::Boolean(true));
                                }

                                ExpressionEvaluator::eval_binary_op_static(&left_val, op, &right_val)
                            }
                        }
                    }
                    // For all other operators, evaluate both sides as before
                    _ => {
                        let left_val = self.eval(left, row)?;
                        let right_val = self.eval(right, row)?;
                        ExpressionEvaluator::eval_binary_op_static(&left_val, op, &right_val)
                    }
                }
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
            ast::Expression::ScalarSubquery(subquery) => self.eval_scalar_subquery(subquery, row),

            // BETWEEN predicate: expr BETWEEN low AND high
            ast::Expression::Between { expr, low, high, negated, symmetric } => {
                self.eval_between(expr, low, high, *negated, *symmetric, row)
            }

            // CAST expression: CAST(expr AS data_type)
            ast::Expression::Cast { expr, data_type } => self.eval_cast(expr, data_type, row),

            // POSITION expression: POSITION(substring IN string)
            ast::Expression::Position { substring, string, character_unit: _ } => {
                self.eval_position(substring, string, row)
            }

            // TRIM expression: TRIM([position] [removal_char FROM] string)
            ast::Expression::Trim { position, removal_char, string } => {
                self.eval_trim(position, removal_char, string, row)
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
            ast::Expression::IsNull { expr, negated } => self.eval_is_null(expr, *negated, row),

            // Function expressions - handle scalar functions (not aggregates)
            ast::Expression::Function { name, args, character_unit } => {
                self.eval_function(name, args, character_unit, row)
            }

            // Current date/time functions
            ast::Expression::CurrentDate => {
                super::super::functions::eval_scalar_function("CURRENT_DATE", &[], &None)
            }
            ast::Expression::CurrentTime { precision: _ } => {
                // For now, ignore precision and call existing function
                // Phase 2 will implement precision-aware formatting
                super::super::functions::eval_scalar_function("CURRENT_TIME", &[], &None)
            }
            ast::Expression::CurrentTimestamp { precision: _ } => {
                // For now, ignore precision and call existing function
                // Phase 2 will implement precision-aware formatting
                super::super::functions::eval_scalar_function("CURRENT_TIMESTAMP", &[], &None)
            }

            // Unary operations (delegate to shared function)
            ast::Expression::UnaryOp { op, expr } => self.eval_unary(op, expr, row),

            // Window functions - look up pre-computed values
            ast::Expression::WindowFunction { function, over } => {
                if let Some(mapping) = self.window_mapping {
                    let key = WindowFunctionKey::from_expression(function, over);
                    if let Some(&col_idx) = mapping.get(&key) {
                        // Extract the pre-computed value from the appended column
                        let value =
                            row.values.get(col_idx).cloned().ok_or({
                                ExecutorError::ColumnIndexOutOfBounds { index: col_idx }
                            })?;
                        Ok(value)
                    } else {
                        Err(ExecutorError::UnsupportedExpression(format!(
                            "Window function not found in mapping: {:?}",
                            expr
                        )))
                    }
                } else {
                    Err(ExecutorError::UnsupportedExpression(
                        "Window functions require window mapping context".to_string(),
                    ))
                }
            }

            // Unsupported expressions
            _ => Err(ExecutorError::UnsupportedExpression(format!("{:?}", expr))),
        }
    }
}
