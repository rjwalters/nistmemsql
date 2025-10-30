//! Utility methods for SelectExecutor

use super::builder::SelectExecutor;

impl SelectExecutor<'_> {
    /// Check if an expression references a column (which requires FROM clause)
    pub(super) fn expression_references_column(&self, expr: &ast::Expression) -> bool {
        match expr {
            ast::Expression::ColumnRef { .. } => true,
            ast::Expression::Default => false, // DEFAULT doesn't reference columns

            ast::Expression::BinaryOp { left, right, .. } => {
                self.expression_references_column(left) || self.expression_references_column(right)
            }

            ast::Expression::UnaryOp { expr, .. } => self.expression_references_column(expr),

            ast::Expression::Function { args, .. } => {
                args.iter().any(|arg| self.expression_references_column(arg))
            }

            ast::Expression::AggregateFunction { args, .. } => {
                args.iter().any(|arg| self.expression_references_column(arg))
            }

            ast::Expression::IsNull { expr, .. } => self.expression_references_column(expr),

            ast::Expression::InList { expr, values, .. } => {
                self.expression_references_column(expr)
                    || values.iter().any(|v| self.expression_references_column(v))
            }

            ast::Expression::Between { expr, low, high, .. } => {
                self.expression_references_column(expr)
                    || self.expression_references_column(low)
                    || self.expression_references_column(high)
            }

            ast::Expression::Cast { expr, .. } => self.expression_references_column(expr),

            ast::Expression::Position { substring, string, character_unit: _ } => {
                self.expression_references_column(substring)
                    || self.expression_references_column(string)
            }

            ast::Expression::Trim { removal_char, string, .. } => {
                removal_char.as_ref().is_some_and(|e| self.expression_references_column(e))
                    || self.expression_references_column(string)
            }

            ast::Expression::Like { expr, pattern, .. } => {
                self.expression_references_column(expr)
                    || self.expression_references_column(pattern)
            }

            ast::Expression::In { expr, .. } => {
                // Note: subquery could reference outer columns but that's a different case
                self.expression_references_column(expr)
            }

            ast::Expression::QuantifiedComparison { expr, .. } => {
                self.expression_references_column(expr)
            }

            ast::Expression::Case { operand, when_clauses, else_result } => {
                operand.as_ref().is_some_and(|e| self.expression_references_column(e))
                    || when_clauses.iter().any(|when_clause| {
                        when_clause
                            .conditions
                            .iter()
                            .any(|cond| self.expression_references_column(cond))
                            || self.expression_references_column(&when_clause.result)
                    })
                    || else_result.as_ref().is_some_and(|e| self.expression_references_column(e))
            }

            ast::Expression::WindowFunction { function, over } => {
                // Check window function arguments
                let args_reference_column = match function {
                    ast::WindowFunctionSpec::Aggregate { args, .. }
                    | ast::WindowFunctionSpec::Ranking { args, .. }
                    | ast::WindowFunctionSpec::Value { args, .. } => {
                        args.iter().any(|arg| self.expression_references_column(arg))
                    }
                };

                // Check PARTITION BY and ORDER BY clauses
                let partition_references = over.partition_by.as_ref().is_some_and(|exprs| {
                    exprs.iter().any(|e| self.expression_references_column(e))
                });

                let order_references = over.order_by.as_ref().is_some_and(|items| {
                    items.iter().any(|item| self.expression_references_column(&item.expr))
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
}
