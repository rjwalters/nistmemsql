//! Main evaluation entry point and basic expression types

use types::SqlValue;

use super::super::core::ExpressionEvaluator;
use crate::errors::ExecutorError;

impl ExpressionEvaluator<'_> {
    /// Evaluate an expression in the context of a row
    pub fn eval(
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

        // CSE: Check cache if enabled and expression is deterministic
        if self.enable_cse && super::super::expression_hash::ExpressionHasher::is_deterministic(expr) {
            let hash = super::super::expression_hash::ExpressionHasher::hash(expr);

            // Check cache
            if let Some(cached) = self.cse_cache.borrow().get(&hash) {
                return Ok(cached.clone());
            }

            // Evaluate with depth increment and cache result
            let result = self.with_incremented_depth(|evaluator| evaluator.eval_impl(expr, row))?;
            self.cse_cache.borrow_mut().insert(hash, result.clone());
            return Ok(result);
        }

        // Non-cached path: increment depth and evaluate
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

            // DEFAULT keyword - not allowed in SELECT/WHERE expressions
            ast::Expression::Default => Err(ExecutorError::UnsupportedExpression(
                "DEFAULT keyword is only valid in INSERT VALUES and UPDATE SET clauses".to_string(),
            )),

            // Column reference - look up column index and get value from row
            ast::Expression::ColumnRef { table, column } => {
                self.eval_column_ref(table.as_deref(), column, row)
            }

            // Binary operations
            ast::Expression::BinaryOp { left, op, right } => {
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
                                if matches!(left_val, SqlValue::Null)
                                    && matches!(right_val, SqlValue::Boolean(false))
                                {
                                    return Ok(SqlValue::Boolean(false));
                                }

                                self.eval_binary_op(&left_val, op, &right_val)
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
                                if matches!(left_val, SqlValue::Null)
                                    && matches!(right_val, SqlValue::Boolean(true))
                                {
                                    return Ok(SqlValue::Boolean(true));
                                }

                                self.eval_binary_op(&left_val, op, &right_val)
                            }
                        }
                    }
                    // For all other operators, evaluate both sides as before
                    _ => {
                        let left_val = self.eval(left, row)?;
                        let right_val = self.eval(right, row)?;
                        self.eval_binary_op(&left_val, op, &right_val)
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

            // Scalar subquery
            ast::Expression::ScalarSubquery(subquery) => self.eval_scalar_subquery(subquery, row),

            // BETWEEN predicate
            ast::Expression::Between { expr, low, high, negated, symmetric } => {
                self.eval_between(expr, low, high, *negated, *symmetric, row)
            }

            // CAST expression
            ast::Expression::Cast { expr, data_type } => self.eval_cast(expr, data_type, row),

            // POSITION expression
            ast::Expression::Position { substring, string, character_unit: _ } => {
                self.eval_position(substring, string, row)
            }

            // TRIM expression
            ast::Expression::Trim { position, removal_char, string } => {
                self.eval_trim(position, removal_char, string, row)
            }

            // LIKE pattern matching
            ast::Expression::Like { expr, pattern, negated } => {
                self.eval_like(expr, pattern, *negated, row)
            }

            // IN list (value list)
            ast::Expression::InList { expr, values, negated } => {
                self.eval_in_list(expr, values, *negated, row)
            }

            // EXISTS predicate
            ast::Expression::Exists { subquery, negated } => {
                self.eval_exists(subquery, *negated, row)
            }

            // Quantified comparison (ALL/ANY/SOME)
            ast::Expression::QuantifiedComparison { expr, op, quantifier, subquery } => {
                self.eval_quantified(expr, op, quantifier, subquery, row)
            }

            // Function call
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

            // Unsupported expressions
            ast::Expression::Wildcard => Err(ExecutorError::UnsupportedExpression(
                "Wildcard (*) not supported in expressions".to_string(),
            )),

            // Unary operations
            ast::Expression::UnaryOp { op, expr } => {
                let val = self.eval(expr, row)?;
                super::operators::eval_unary_op(op, &val)
            }

            ast::Expression::IsNull { expr, negated } => {
                let value = self.eval(expr, row)?;
                let is_null = matches!(value, SqlValue::Null);
                let result = if *negated { !is_null } else { is_null };
                Ok(SqlValue::Boolean(result))
            }

            ast::Expression::WindowFunction { .. } => Err(ExecutorError::UnsupportedExpression(
                "Window functions should be evaluated separately".to_string(),
            )),

            ast::Expression::AggregateFunction { .. } => Err(ExecutorError::UnsupportedExpression(
                "Aggregate functions should be evaluated in aggregation context".to_string(),
            )),

            // NEXT VALUE FOR sequence expression
            // TODO: Implement proper sequence evaluation
            //
            // Requirements for implementation:
            // 1. Sequence catalog objects (CREATE SEQUENCE, DROP SEQUENCE, etc.)
            // 2. Sequence state storage (current value, increment, min/max, cycle, etc.)
            // 3. Mutable access to catalog to advance sequences (architectural change)
            // 4. Thread-safe sequence value generation
            //
            // Current architecture has immutable database references in evaluator.
            // Possible solutions:
            // 1. Use RefCell<Sequence> or Arc<Mutex<Sequence>> for interior mutability
            // 2. Handle NEXT VALUE FOR at statement execution level (INSERT/SELECT)
            // 3. Change evaluator to accept mutable database reference
            // 4. Use a separate sequence manager with thread-safe state
            //
            // Note: Parser and AST support already exists (Expression::NextValue).
            // See SQL:1999 Section 6.13 for sequence expression specification.
            ast::Expression::NextValue { sequence_name } => {
                Err(ExecutorError::UnsupportedExpression(format!(
                    "NEXT VALUE FOR {} - Sequence expressions not yet implemented. \
                    Requires sequence catalog infrastructure (CREATE SEQUENCE support), \
                    sequence state management, and mutable catalog access. \
                    Use auto-incrementing primary keys or generate values in application code instead.",
                    sequence_name
                )))
            }
        }
    }

    /// Evaluate column reference
    fn eval_column_ref(
        &self,
        table_qualifier: Option<&str>,
        column: &str,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        // Track which tables we searched for better error messages
        let mut searched_tables = Vec::new();
        let mut available_columns = Vec::new();

        // If table qualifier is provided, validate it matches a known schema
        if let Some(qualifier) = table_qualifier {
            let qualifier_lower = qualifier.to_lowercase();
            let inner_name_lower = self.schema.name.to_lowercase();

            // Check if qualifier matches inner schema
            if qualifier_lower == inner_name_lower {
                // Qualifier matches inner schema - search only there
                searched_tables.push(self.schema.name.clone());
                if let Some(col_index) = self.schema.get_column_index(column) {
                    return row
                        .get(col_index)
                        .cloned()
                        .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
                }
            } else if let Some(outer_schema) = self.outer_schema {
                let outer_name_lower = outer_schema.name.to_lowercase();

                // Check if qualifier matches outer schema
                if qualifier_lower == outer_name_lower {
                    // Qualifier matches outer schema - search only there
                    if let Some(outer_row) = self.outer_row {
                        searched_tables.push(outer_schema.name.clone());
                        if let Some(col_index) = outer_schema.get_column_index(column) {
                            return outer_row
                                .get(col_index)
                                .cloned()
                                .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
                        }
                    }
                } else {
                    // Qualifier doesn't match any known schema
                    let mut known_tables = vec![self.schema.name.clone()];
                    known_tables.push(outer_schema.name.clone());

                    return Err(ExecutorError::InvalidTableQualifier {
                        qualifier: qualifier.to_string(),
                        column: column.to_string(),
                        available_tables: known_tables,
                    });
                }
            } else {
                // No outer schema and qualifier doesn't match inner schema
                return Err(ExecutorError::InvalidTableQualifier {
                    qualifier: qualifier.to_string(),
                    column: column.to_string(),
                    available_tables: vec![self.schema.name.clone()],
                });
            }

            // If we get here, qualifier was valid but column wasn't found
            available_columns.extend(self.schema.columns.iter().map(|c| c.name.clone()));
            if let Some(outer_schema) = self.outer_schema {
                available_columns.extend(outer_schema.columns.iter().map(|c| c.name.clone()));
            }

            return Err(ExecutorError::ColumnNotFound {
                column_name: column.to_string(),
                table_name: qualifier.to_string(),
                searched_tables,
                available_columns,
            });
        }

        // No qualifier provided - use original search logic (inner first, then outer)
        // Try to resolve in inner schema first
        searched_tables.push(self.schema.name.clone());
        if let Some(col_index) = self.schema.get_column_index(column) {
            return row
                .get(col_index)
                .cloned()
                .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
        }

        // If not found in inner schema and outer context exists, try outer schema
        if let (Some(outer_row), Some(outer_schema)) = (self.outer_row, self.outer_schema) {
            searched_tables.push(outer_schema.name.clone());
            if let Some(col_index) = outer_schema.get_column_index(column) {
                return outer_row
                    .get(col_index)
                    .cloned()
                    .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
            }
        }

        // Column not found - collect available columns for suggestions
        available_columns.extend(self.schema.columns.iter().map(|c| c.name.clone()));
        if let Some(outer_schema) = self.outer_schema {
            available_columns.extend(outer_schema.columns.iter().map(|c| c.name.clone()));
        }

        // Column not found in either schema
        Err(ExecutorError::ColumnNotFound {
            column_name: column.to_string(),
            table_name: table_qualifier.unwrap_or("unknown").to_string(),
            searched_tables,
            available_columns,
        })
    }
}
