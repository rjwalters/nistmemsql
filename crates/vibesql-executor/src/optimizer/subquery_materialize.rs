//! Uncorrelated IN subquery materialization
//!
//! This module pre-executes uncorrelated IN subqueries once and converts them to
//! IN (list of values) to avoid re-executing the subquery for every outer row.
//!
//! For Q4-style queries like:
//! ```sql
//! SELECT ... FROM orders WHERE o_orderkey IN (
//!   SELECT DISTINCT l_orderkey FROM lineitem WHERE l_commitdate < l_receiptdate
//! )
//! ```
//!
//! The subquery is executed once and converted to:
//! ```sql
//! SELECT ... FROM orders WHERE o_orderkey IN (1, 2, 3, ...)
//! ```
//!
//! This transforms O(n * m) execution (n outer rows × m subquery cost) into
//! O(m) + O(n) execution.

use std::collections::HashSet;

use vibesql_ast::{CaseWhen, Expression};
use vibesql_storage::Database;
use vibesql_types::SqlValue;

use crate::errors::ExecutorError;
use crate::optimizer::subquery_rewrite::correlation::is_correlated;
use crate::select::SelectExecutor;

/// Materialize uncorrelated IN subqueries in a WHERE clause
///
/// Walks the expression tree, finds IN (subquery) where the subquery is
/// uncorrelated, executes them once, and replaces with IN (list).
///
/// Returns the transformed expression.
pub fn materialize_uncorrelated_in_subqueries(
    expr: &Expression,
    database: &Database,
) -> Result<Expression, ExecutorError> {
    materialize_expr(expr, database)
}

fn materialize_expr(expr: &Expression, database: &Database) -> Result<Expression, ExecutorError> {
    match expr {
        // Handle IN (subquery)
        Expression::In {
            expr: in_expr,
            subquery,
            negated,
        } => {
            // Check if subquery is uncorrelated
            if !is_correlated(subquery) {
                // Execute subquery once
                let executor = SelectExecutor::new(database);
                let rows = executor.execute(subquery)?;

                // Single-column subquery validation
                if !rows.is_empty() && rows[0].values.len() != 1 {
                    return Err(ExecutorError::SubqueryColumnCountMismatch {
                        expected: 1,
                        actual: rows[0].values.len(),
                    });
                }

                // Collect values into a HashSet for deduplication
                let mut values_set: HashSet<SqlValue> = HashSet::new();
                for row in &rows {
                    if let Some(val) = row.values.first() {
                        values_set.insert(val.clone());
                    }
                }

                // Convert to InList expression
                let values: Vec<Expression> = values_set
                    .into_iter()
                    .map(Expression::Literal)
                    .collect();

                // If empty, return appropriate constant
                if values.is_empty() {
                    // Empty IN list: expr IN () is always false, NOT IN () is always true
                    return Ok(Expression::Literal(SqlValue::Boolean(*negated)));
                }

                // Recursively materialize the left expression (in case it has nested subqueries)
                let materialized_in_expr = materialize_expr(in_expr, database)?;

                Ok(Expression::InList {
                    expr: Box::new(materialized_in_expr),
                    values,
                    negated: *negated,
                })
            } else {
                // Correlated subquery - keep as-is, recursively process in_expr
                let materialized_in_expr = materialize_expr(in_expr, database)?;
                Ok(Expression::In {
                    expr: Box::new(materialized_in_expr),
                    subquery: subquery.clone(),
                    negated: *negated,
                })
            }
        }

        // Recursively process binary operations
        Expression::BinaryOp { op, left, right } => {
            let new_left = materialize_expr(left, database)?;
            let new_right = materialize_expr(right, database)?;
            Ok(Expression::BinaryOp {
                op: op.clone(),
                left: Box::new(new_left),
                right: Box::new(new_right),
            })
        }

        // Recursively process unary operations
        Expression::UnaryOp { op, expr: inner } => {
            let new_inner = materialize_expr(inner, database)?;
            Ok(Expression::UnaryOp {
                op: op.clone(),
                expr: Box::new(new_inner),
            })
        }

        // Handle EXISTS (though typically not uncorrelated)
        Expression::Exists { subquery, negated } => {
            if !is_correlated(subquery) {
                // Uncorrelated EXISTS can be evaluated once
                let executor = SelectExecutor::new(database);
                let rows = executor.execute(subquery)?;
                let exists = !rows.is_empty();
                let result = if *negated { !exists } else { exists };
                Ok(Expression::Literal(SqlValue::Boolean(result)))
            } else {
                Ok(expr.clone())
            }
        }

        // Handle InList (recursively process expressions in the list)
        Expression::InList {
            expr: in_expr,
            values,
            negated,
        } => {
            let new_expr = materialize_expr(in_expr, database)?;
            let new_values: Result<Vec<_>, _> = values
                .iter()
                .map(|v| materialize_expr(v, database))
                .collect();
            Ok(Expression::InList {
                expr: Box::new(new_expr),
                values: new_values?,
                negated: *negated,
            })
        }

        // Handle CASE expressions
        Expression::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            let new_operand = operand
                .as_ref()
                .map(|e| materialize_expr(e, database))
                .transpose()?;
            let new_clauses: Result<Vec<CaseWhen>, ExecutorError> = when_clauses
                .iter()
                .map(|clause| {
                    let new_conditions: Result<Vec<_>, ExecutorError> = clause
                        .conditions
                        .iter()
                        .map(|c| materialize_expr(c, database))
                        .collect();
                    Ok(CaseWhen {
                        conditions: new_conditions?,
                        result: materialize_expr(&clause.result, database)?,
                    })
                })
                .collect();
            let new_else = else_result
                .as_ref()
                .map(|e| materialize_expr(e, database))
                .transpose()?;
            Ok(Expression::Case {
                operand: new_operand.map(Box::new),
                when_clauses: new_clauses?,
                else_result: new_else.map(Box::new),
            })
        }

        // Handle scalar subqueries (could be optimized too)
        Expression::ScalarSubquery(subquery) => {
            if !is_correlated(subquery) {
                // Execute uncorrelated scalar subquery once
                let executor = SelectExecutor::new(database);
                let rows = executor.execute(subquery)?;

                // Scalar subquery must return exactly one row with one column
                if rows.len() > 1 {
                    return Err(ExecutorError::SubqueryReturnedMultipleRows {
                        expected: 1,
                        actual: rows.len(),
                    });
                }

                if rows.is_empty() {
                    return Ok(Expression::Literal(SqlValue::Null));
                }

                let row = &rows[0];
                if row.values.len() != 1 {
                    return Err(ExecutorError::SubqueryColumnCountMismatch {
                        expected: 1,
                        actual: row.values.len(),
                    });
                }

                Ok(Expression::Literal(row.values[0].clone()))
            } else {
                Ok(expr.clone())
            }
        }

        // Handle IsNull
        Expression::IsNull { expr: inner, negated } => {
            let new_inner = materialize_expr(inner, database)?;
            Ok(Expression::IsNull {
                expr: Box::new(new_inner),
                negated: *negated,
            })
        }

        // Handle Between
        Expression::Between {
            expr: inner,
            low,
            high,
            negated,
            symmetric,
        } => {
            let new_inner = materialize_expr(inner, database)?;
            let new_low = materialize_expr(low, database)?;
            let new_high = materialize_expr(high, database)?;
            Ok(Expression::Between {
                expr: Box::new(new_inner),
                low: Box::new(new_low),
                high: Box::new(new_high),
                negated: *negated,
                symmetric: *symmetric,
            })
        }

        // Handle functions
        Expression::Function { name, args, character_unit } => {
            let new_args: Result<Vec<_>, _> = args
                .iter()
                .map(|a| materialize_expr(a, database))
                .collect();
            Ok(Expression::Function {
                name: name.clone(),
                args: new_args?,
                character_unit: character_unit.clone(),
            })
        }

        Expression::AggregateFunction { name, args, distinct } => {
            let new_args: Result<Vec<_>, _> = args
                .iter()
                .map(|a| materialize_expr(a, database))
                .collect();
            Ok(Expression::AggregateFunction {
                name: name.clone(),
                args: new_args?,
                distinct: *distinct,
            })
        }

        // Handle Cast
        Expression::Cast { expr: inner, data_type } => {
            let new_inner = materialize_expr(inner, database)?;
            Ok(Expression::Cast {
                expr: Box::new(new_inner),
                data_type: data_type.clone(),
            })
        }

        // Handle Like
        Expression::Like {
            expr: inner,
            pattern,
            negated,
        } => {
            let new_inner = materialize_expr(inner, database)?;
            let new_pattern = materialize_expr(pattern, database)?;
            Ok(Expression::Like {
                expr: Box::new(new_inner),
                pattern: Box::new(new_pattern),
                negated: *negated,
            })
        }

        // All other expressions - return as-is
        _ => Ok(expr.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests would require a mock database, which is complex
    // For now, we rely on integration tests with TPC-H Q4
}
