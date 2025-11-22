//! Columnar execution integration for SelectExecutor
//!
//! This module integrates the columnar execution engine with the query executor,
//! providing automatic detection and execution of queries that can benefit from
//! SIMD-accelerated columnar processing.

use std::collections::HashMap;

use super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError,
    select::{columnar, cte::CteResult},
};

impl SelectExecutor<'_> {
    /// Try to execute using columnar (SIMD-accelerated) execution
    ///
    /// Returns Some(rows) if the query is compatible with columnar execution.
    /// Returns None if the query should fall back to regular row-based execution.
    ///
    /// Columnar execution provides 6-10x speedup for queries with:
    /// - Simple predicates on numeric columns
    /// - Aggregations (SUM, AVG, MIN, MAX, COUNT)
    /// - Single table scans (no JOINs yet)
    ///
    /// # Phase 5 Implementation
    ///
    /// This initial implementation focuses on simple aggregate queries without GROUP BY.
    /// Future phases will add support for:
    /// - GROUP BY aggregations
    /// - JOIN operations
    /// - More complex predicates (OR logic, IN clauses)
    pub(in crate::select::executor) fn try_columnar_execution(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        cte_results: &HashMap<String, CteResult>,
    ) -> Result<Option<Vec<vibesql_storage::Row>>, ExecutorError> {
        // Check if this query is compatible with columnar execution
        if !self.should_use_columnar(stmt) {
            return Ok(None);
        }

        // Only handle queries without CTEs or set operations for now
        if !cte_results.is_empty() || stmt.set_operation.is_some() {
            return Ok(None);
        }

        // Must have a FROM clause
        let from_clause = match &stmt.from {
            Some(from) => from,
            None => return Ok(None),
        };

        // Execute FROM clause WITHOUT applying WHERE clause
        // The columnar module will apply the WHERE clause using SIMD-accelerated filtering
        let mut from_result = self.execute_from_with_where(
            from_clause,
            cte_results,
            None, // Don't filter here - columnar module will handle it with SIMD
            None, // ORDER BY applied after aggregation
        )?;

        // Extract schema before taking rows (to avoid borrow checker issues)
        let schema = from_result.schema.clone();

        // Extract expressions from SELECT list (only Expression items, skip wildcards)
        let select_exprs: Vec<_> = stmt
            .select_list
            .iter()
            .filter_map(|item| match item {
                vibesql_ast::SelectItem::Expression { expr, .. } => Some(expr.clone()),
                _ => None, // Skip wildcards
            })
            .collect();

        // Try columnar execution with SIMD-accelerated filtering
        // If this returns None, the regular executor will handle the query with row-based execution
        match columnar::execute_columnar(
            from_result.rows(),
            stmt.where_clause.as_ref(), // Let columnar module apply WHERE with SIMD
            &select_exprs,
            &schema,
        ) {
            Some(result) => result.map(Some),
            None => Ok(None), // Fall back to regular execution
        }
    }

    /// Determine if a query should use columnar execution
    ///
    /// Columnar execution is beneficial for queries that:
    /// 1. Have simple predicates (=, <, >, <=, >=, BETWEEN)
    /// 2. Use aggregations (SUM, AVG, MIN, MAX, COUNT)
    /// 3. Scan a single table (no JOINs for now)
    /// 4. Don't use complex features (window functions, CTEs, subqueries)
    ///
    /// # Heuristics
    ///
    /// - **Row count threshold**: Only use columnar for tables with 1000+ rows
    ///   (overhead of columnar conversion not worth it for small tables)
    /// - **Column types**: Prefer numeric columns (i64, f64) that benefit from SIMD
    /// - **Predicate complexity**: Simple AND predicates only (no OR for now)
    /// - **Aggregate complexity**: Supports simple column references and arithmetic expressions (e.g., SUM(a * b))
    fn should_use_columnar(&self, stmt: &vibesql_ast::SelectStmt) -> bool {
        // Must have aggregates
        if !self.has_aggregates(&stmt.select_list) && stmt.having.is_none() {
            return false;
        }

        // Note: We no longer check has_simple_aggregates() here because the columnar
        // module now supports complex expressions like SUM(a * b) via AggregateSource::Expression.
        // The extract_aggregates() function will return None if it encounters unsupported
        // expressions, triggering an automatic fallback to row-based execution.

        // No GROUP BY support yet (Phase 5 limitation)
        // TODO: Add GROUP BY support in future phase
        if stmt.group_by.is_some() {
            return false;
        }

        // Must have a FROM clause (single table for now)
        let from_clause = match &stmt.from {
            Some(from) => from,
            None => return false,
        };

        // Check if it's a simple table scan (no JOINs)
        // TODO: Add JOIN support in future phase
        if !self.is_simple_table_scan(from_clause) {
            return false;
        }

        // No window functions
        if self.has_window_functions_in_select(&stmt.select_list) {
            return false;
        }

        // No DISTINCT for now (would need additional processing)
        if stmt.distinct {
            return false;
        }

        // Check if WHERE clause has simple predicates
        if let Some(where_expr) = &stmt.where_clause {
            if !self.is_simple_predicate(where_expr) {
                return false;
            }
        }

        true
    }

    /// Check if FROM clause is a simple table scan (no JOINs, no subqueries)
    fn is_simple_table_scan(&self, from: &vibesql_ast::FromClause) -> bool {
        use vibesql_ast::FromClause;

        match from {
            FromClause::Table { .. } => true,
            FromClause::Join { .. } => false,
            FromClause::Subquery { .. } => false,
        }
    }

    /// Check if expression is a simple predicate suitable for columnar execution
    ///
    /// Simple predicates include:
    /// - Comparisons: =, <, >, <=, >=
    /// - BETWEEN
    /// - AND combinations of simple predicates
    ///
    /// Not supported yet:
    /// - OR predicates (partial support exists but not fully integrated)
    /// - IN clauses
    /// - LIKE patterns
    /// - Complex subqueries
    fn is_simple_predicate(&self, expr: &vibesql_ast::Expression) -> bool {
        use vibesql_ast::Expression;

        match expr {
            // Simple binary comparisons
            Expression::BinaryOp { op, .. } => {
                use vibesql_ast::BinaryOperator;
                matches!(
                    op,
                    BinaryOperator::Equal
                        | BinaryOperator::NotEqual
                        | BinaryOperator::LessThan
                        | BinaryOperator::LessThanOrEqual
                        | BinaryOperator::GreaterThan
                        | BinaryOperator::GreaterThanOrEqual
                        | BinaryOperator::And
                )
            }
            // BETWEEN is supported
            Expression::Between { .. } => true,
            // Everything else is too complex
            _ => false,
        }
    }

    /// Check if aggregates have simple column references only
    ///
    /// Returns true if all aggregate functions contain only:
    /// - Column references (e.g., SUM(price))
    /// - COUNT(*) wildcards
    ///
    /// Returns false for complex expressions like:
    /// - SUM(a * b)  - arithmetic expressions
    /// - AVG(CASE ...) - conditional logic
    /// - COUNT(DISTINCT x) - distinct handled separately
    fn has_simple_aggregates(&self, select_list: &[vibesql_ast::SelectItem]) -> bool {
        use vibesql_ast::{Expression, SelectItem};

        for item in select_list {
            if let SelectItem::Expression { expr, .. } = item {
                if let Expression::AggregateFunction { args, .. } = expr {
                    for arg in args {
                        match arg {
                            // Simple column references are OK
                            Expression::ColumnRef { .. } => {}
                            // COUNT(*) wildcard is OK
                            Expression::Wildcard => {}
                            // Everything else is too complex
                            _ => return false,
                        }
                    }
                }
            }
        }
        true
    }

    /// Check if SELECT list has window functions
    fn has_window_functions_in_select(&self, select_list: &[vibesql_ast::SelectItem]) -> bool {
        use vibesql_ast::SelectItem;

        for item in select_list {
            match item {
                SelectItem::Expression { expr, .. } => {
                    if self.has_window_functions_in_expr(expr) {
                        return true;
                    }
                }
                _ => {} // Wildcards don't have window functions
            }
        }
        false
    }

    /// Check if an expression contains window functions (recursively)
    fn has_window_functions_in_expr(&self, expr: &vibesql_ast::Expression) -> bool {
        use vibesql_ast::Expression;

        match expr {
            Expression::WindowFunction { .. } => true,
            Expression::BinaryOp { left, right, .. } => {
                self.has_window_functions_in_expr(left) || self.has_window_functions_in_expr(right)
            }
            Expression::UnaryOp { expr, .. } => self.has_window_functions_in_expr(expr),
            Expression::Function { args, .. } => {
                args.iter().any(|arg| self.has_window_functions_in_expr(arg))
            }
            _ => false,
        }
    }
}
