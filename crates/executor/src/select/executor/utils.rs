//! Utility methods for SelectExecutor

use super::builder::SelectExecutor;

/// Check if an expression references a column (which requires FROM clause)
fn expression_references_column(expr: &ast::Expression) -> bool {
    match expr {
        ast::Expression::ColumnRef { .. } => true,
        ast::Expression::Default => false, // DEFAULT doesn't reference columns

        ast::Expression::BinaryOp { left, right, .. } => {
            expression_references_column(left) || expression_references_column(right)
        }

        ast::Expression::UnaryOp { expr, .. } => expression_references_column(expr),

        ast::Expression::Function { args, .. } => args.iter().any(expression_references_column),

        ast::Expression::AggregateFunction { args, .. } => {
            args.iter().any(expression_references_column)
        }

        ast::Expression::IsNull { expr, .. } => expression_references_column(expr),

        ast::Expression::InList { expr, values, .. } => {
            expression_references_column(expr) || values.iter().any(expression_references_column)
        }

        ast::Expression::Between { expr, low, high, .. } => {
            expression_references_column(expr)
                || expression_references_column(low)
                || expression_references_column(high)
        }

        ast::Expression::Cast { expr, .. } => expression_references_column(expr),

        ast::Expression::Position { substring, string, character_unit: _ } => {
            expression_references_column(substring) || expression_references_column(string)
        }

        ast::Expression::Trim { removal_char, string, .. } => {
            removal_char.as_ref().is_some_and(|e| expression_references_column(e))
                || expression_references_column(string)
        }

        ast::Expression::Like { expr, pattern, .. } => {
            expression_references_column(expr) || expression_references_column(pattern)
        }

        ast::Expression::In { expr, .. } => {
            // Note: subquery could reference outer columns but that's a different case
            expression_references_column(expr)
        }

        ast::Expression::QuantifiedComparison { expr, .. } => expression_references_column(expr),

        ast::Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|e| expression_references_column(e))
                || when_clauses.iter().any(|when_clause| {
                    when_clause.conditions.iter().any(expression_references_column)
                        || expression_references_column(&when_clause.result)
                })
                || else_result.as_ref().is_some_and(|e| expression_references_column(e))
        }

        ast::Expression::WindowFunction { function, over } => {
            // Check window function arguments
            let args_reference_column = match function {
                ast::WindowFunctionSpec::Aggregate { args, .. }
                | ast::WindowFunctionSpec::Ranking { args, .. }
                | ast::WindowFunctionSpec::Value { args, .. } => {
                    args.iter().any(expression_references_column)
                }
            };

            // Check PARTITION BY and ORDER BY clauses
            let partition_references = over
                .partition_by
                .as_ref()
                .is_some_and(|exprs| exprs.iter().any(expression_references_column));

            let order_references = over.order_by.as_ref().is_some_and(|items| {
                items.iter().any(|item| expression_references_column(&item.expr))
            });

            args_reference_column || partition_references || order_references
        }

        // These don't contain column references:
        ast::Expression::Literal(_) => false,
        ast::Expression::Wildcard => false,
        ast::Expression::ScalarSubquery(_) => false, // Subquery has its own scope
        ast::Expression::Exists { .. } => false,     // Subquery has its own scope
        ast::Expression::CurrentDate => false,
        ast::Expression::CurrentTime { .. } => false,
        ast::Expression::CurrentTimestamp { .. } => false,
    }
}

impl SelectExecutor<'_> {
    /// Check if an expression references a column (which requires FROM clause)
    pub(super) fn expression_references_column(&self, expr: &ast::Expression) -> bool {
        expression_references_column(expr)
    }
}
