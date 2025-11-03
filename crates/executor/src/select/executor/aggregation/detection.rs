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

    /// Check if statement is a simple COUNT(*) query that can use fast path
    ///
    /// Fast path conditions:
    /// - Single SELECT item: COUNT(*)
    /// - No WHERE clause
    /// - No GROUP BY clause
    /// - No HAVING clause
    /// - No DISTINCT
    /// - No JOIN (single table reference)
    /// - No set operations (UNION, INTERSECT, EXCEPT)
    /// - FROM clause contains single table (not subquery/CTE)
    pub(in crate::select::executor) fn is_simple_count_star(
        &self,
        stmt: &ast::SelectStmt,
    ) -> Option<String> {
        // Must have exactly one select item
        if stmt.select_list.len() != 1 {
            return None;
        }

        // Check if it's COUNT(*)
        let is_count_star = match &stmt.select_list[0] {
            ast::SelectItem::Expression { expr, .. } => {
                match expr {
                    ast::Expression::AggregateFunction { name, distinct, args } => {
                        // Must be COUNT, not DISTINCT, with single wildcard argument
                        if name.to_uppercase() != "COUNT" || *distinct || args.len() != 1 {
                            return None;
                        }
                        matches!(args[0], ast::Expression::Wildcard)
                            || matches!(
                                &args[0],
                                ast::Expression::ColumnRef { table: None, column } if column == "*"
                            )
                    }
                    ast::Expression::Function { name, args, .. } => {
                        // Old Function variant (backwards compatibility)
                        if name.to_uppercase() != "COUNT" || args.len() != 1 {
                            return None;
                        }
                        matches!(args[0], ast::Expression::Wildcard)
                            || matches!(
                                &args[0],
                                ast::Expression::ColumnRef { table: None, column } if column == "*"
                            )
                    }
                    _ => false,
                }
            }
            _ => false,
        };

        if !is_count_star {
            return None;
        }

        // Must not have WHERE, GROUP BY, HAVING, DISTINCT, or set operations
        if stmt.where_clause.is_some()
            || stmt.group_by.is_some()
            || stmt.having.is_some()
            || stmt.distinct
            || stmt.set_operation.is_some()
        {
            return None;
        }

        // Must have a FROM clause with a single table
        let table_name = match &stmt.from {
            Some(ast::FromClause::Table { name, .. }) => name.clone(),
            Some(ast::FromClause::Join { .. }) => return None, // JOIN not allowed
            Some(ast::FromClause::Subquery { .. }) => return None, // Subquery not allowed
            None => return None,                               // No FROM clause
        };

        Some(table_name)
    }
}
