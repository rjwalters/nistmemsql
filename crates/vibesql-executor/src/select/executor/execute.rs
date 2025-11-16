//! Main execution methods for SelectExecutor

use std::collections::HashMap;

use super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError,
    select::{
        cte::{execute_ctes, CteResult},
        helpers::apply_limit_offset,
        join::FromResult,
        set_operations::apply_set_operation,
        SelectResult,
    },
};

impl SelectExecutor<'_> {
    /// Execute a SELECT statement
    pub fn execute(&self, stmt: &vibesql_ast::SelectStmt) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        // Check timeout before starting execution
        self.check_timeout()?;

        // Check subquery depth limit to prevent stack overflow
        if self.subquery_depth >= crate::limits::MAX_EXPRESSION_DEPTH {
            return Err(ExecutorError::ExpressionDepthExceeded {
                depth: self.subquery_depth,
                max_depth: crate::limits::MAX_EXPRESSION_DEPTH,
            });
        }

        // Execute CTEs if present
        let cte_results = if let Some(with_clause) = &stmt.with_clause {
            execute_ctes(with_clause, |query, cte_ctx| self.execute_with_ctes(query, cte_ctx))?
        } else {
            HashMap::new()
        };

        // Execute the main query with CTE context
        self.execute_with_ctes(stmt, &cte_results)
    }

    /// Execute a SELECT statement and return both columns and rows
    pub fn execute_with_columns(
        &self,
        stmt: &vibesql_ast::SelectStmt,
    ) -> Result<SelectResult, ExecutorError> {
        // First, get the FROM result to access the schema
        let from_result = if let Some(from_clause) = &stmt.from {
            let cte_results = if let Some(with_clause) = &stmt.with_clause {
                execute_ctes(with_clause, |query, cte_ctx| self.execute_with_ctes(query, cte_ctx))?
            } else {
                HashMap::new()
            };
            Some(self.execute_from(from_clause, &cte_results)?)
        } else {
            None
        };

        // Derive column names from the SELECT list
        let columns = self.derive_column_names(&stmt.select_list, from_result.as_ref())?;

        // Execute the query to get rows
        let rows = self.execute(stmt)?;

        Ok(SelectResult { columns, rows })
    }

    /// Execute SELECT statement with CTE context
    pub(super) fn execute_with_ctes(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        cte_results: &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        // Execute the left-hand side query
        let has_aggregates = self.has_aggregates(&stmt.select_list) || stmt.having.is_some();
        let has_group_by = stmt.group_by.is_some();

        let mut results = if has_aggregates || has_group_by {
            self.execute_with_aggregation(stmt, cte_results)?
        } else if let Some(from_clause) = &stmt.from {
            // Selective predicate pushdown disable for multi-column IN optimization (issue #1807)
            //
            // Multi-column IN optimization requires access to ALL table rows to correctly
            // use indexes. If predicate pushdown filters rows during the FROM scan, the index
            // optimization receives an already-filtered subset with incorrect row indices.
            //
            // We ONLY disable predicate pushdown when:
            // - WHERE clause contains an IN expression
            // - The column has a MULTI-column index
            //
            // All other queries (single-column indexes, binary ops, etc.) retain predicate pushdown.
            //
            // Example requiring disabled pushdown:
            //   SELECT * FROM test WHERE a IN (10)
            //   Index: (a, b) ← Multi-column!
            //
            // Example allowing predicate pushdown:
            //   SELECT * FROM test WHERE a IN (10)
            //   Index: (a) ← Single-column, pushdown is safe!
            //
            //   SELECT * FROM test WHERE a = 10
            //   ← Binary op, not IN, pushdown is safe!

            let where_clause_for_pushdown = if super::index_optimization::requires_predicate_pushdown_disable(
                self.database,
                stmt.where_clause.as_ref(),
                stmt.from.as_ref(),
            ) {
                None // Disable predicate pushdown for multi-column IN
            } else {
                stmt.where_clause.as_ref() // Enable predicate pushdown for everything else
            };

            // Pass WHERE and ORDER BY to execute_from for optimization
            let from_result =
                self.execute_from_with_where(from_clause, cte_results, where_clause_for_pushdown, stmt.order_by.as_deref())?;
            self.execute_without_aggregation(stmt, from_result)?
        } else {
            // SELECT without FROM - evaluate expressions as a single row
            self.execute_select_without_from(stmt)?
        };

        // Handle set operations (UNION, INTERSECT, EXCEPT)
        // Process operations left-to-right to ensure correct associativity
        if let Some(set_op) = &stmt.set_operation {
            results = self.execute_set_operations(results, set_op, cte_results)?;

            // Apply LIMIT/OFFSET to the final result (after all set operations)
            // For queries WITHOUT set operations, LIMIT/OFFSET is already applied
            // in execute_without_aggregation() or execute_with_aggregation()
            results = apply_limit_offset(results, stmt.limit, stmt.offset);
        }

        Ok(results)
    }

    /// Execute a chain of set operations left-to-right
    ///
    /// SQL set operations are left-associative, so:
    /// A EXCEPT B EXCEPT C should evaluate as (A EXCEPT B) EXCEPT C
    ///
    /// The parser creates a right-recursive AST structure, but we need to execute left-to-right.
    fn execute_set_operations(
        &self,
        mut left_results: Vec<vibesql_storage::Row>,
        set_op: &vibesql_ast::SetOperation,
        cte_results: &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        // Execute the immediate right query WITHOUT its set operations
        // This prevents right-recursive evaluation
        let right_stmt = &set_op.right;
        let has_aggregates = self.has_aggregates(&right_stmt.select_list) || right_stmt.having.is_some();
        let has_group_by = right_stmt.group_by.is_some();

        let right_results = if has_aggregates || has_group_by {
            self.execute_with_aggregation(right_stmt, cte_results)?
        } else if let Some(from_clause) = &right_stmt.from {
            let from_result =
                self.execute_from_with_where(from_clause, cte_results, right_stmt.where_clause.as_ref(), right_stmt.order_by.as_deref())?;
            self.execute_without_aggregation(right_stmt, from_result)?
        } else {
            self.execute_select_without_from(right_stmt)?
        };

        // Apply the current operation
        left_results = apply_set_operation(left_results, right_results, set_op)?;

        // If the right side has more set operations, continue processing them
        // This creates the left-to-right evaluation: ((A op B) op C) op D
        if let Some(next_set_op) = &right_stmt.set_operation {
            left_results = self.execute_set_operations(left_results, next_set_op, cte_results)?;
        }

        Ok(left_results)
    }

    /// Execute a FROM clause (table or join) and return combined schema and rows
    pub(super) fn execute_from(
        &self,
        from: &vibesql_ast::FromClause,
        cte_results: &HashMap<String, CteResult>,
    ) -> Result<FromResult, ExecutorError> {
        use crate::select::scan::execute_from_clause;
        execute_from_clause(from, cte_results, self.database, None, None, |query| self.execute_with_columns(query))
    }

    /// Execute a FROM clause with WHERE and ORDER BY for optimization
    pub(super) fn execute_from_with_where(
        &self,
        from: &vibesql_ast::FromClause,
        cte_results: &HashMap<String, CteResult>,
        where_clause: Option<&vibesql_ast::Expression>,
        order_by: Option<&[vibesql_ast::OrderByItem]>,
    ) -> Result<FromResult, ExecutorError> {
        use crate::select::scan::execute_from_clause;
        execute_from_clause(from, cte_results, self.database, where_clause, order_by, |query| {
            self.execute_with_columns(query)
        })
    }
}
