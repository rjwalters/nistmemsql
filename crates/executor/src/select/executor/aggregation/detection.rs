//! Aggregate detection helpers for SelectExecutor

use super::super::builder::SelectExecutor;

impl SelectExecutor<'_> {
    /// Check if SELECT list contains aggregate functions
    pub(in crate::select::executor) fn has_aggregates(
        &self,
        select_list: &[ast::SelectItem],
    ) -> bool {
        select_list.iter().any(|item| match item {
            ast::SelectItem::Expression { expr, .. } => self.expression_has_aggregate(expr),
            _ => false,
        })
    }

    /// Check if an expression contains aggregate functions
    #[allow(clippy::only_used_in_recursion)]
    pub(in crate::select::executor) fn expression_has_aggregate(
        &self,
        expr: &ast::Expression,
    ) -> bool {
        match expr {
            // New AggregateFunction variant
            ast::Expression::AggregateFunction { .. } => true,
            // Old Function variant (backwards compatibility)
            ast::Expression::Function { name, .. } => {
                matches!(name.to_uppercase().as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
            }
            ast::Expression::BinaryOp { left, right, .. } => {
                self.expression_has_aggregate(left) || self.expression_has_aggregate(right)
            }
            _ => false,
        }
    }
}
