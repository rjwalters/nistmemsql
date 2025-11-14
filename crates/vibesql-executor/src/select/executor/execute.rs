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
            // Pass WHERE clause to execute_from for predicate pushdown optimization
            let from_result =
                self.execute_from_with_where(from_clause, cte_results, stmt.where_clause.as_ref())?;
            self.execute_without_aggregation(stmt, from_result)?
        } else {
            // SELECT without FROM - evaluate expressions as a single row
            self.execute_select_without_from(stmt)?
        };

        // Handle set operations (UNION, INTERSECT, EXCEPT)
        if let Some(set_op) = &stmt.set_operation {
            // Execute the right-hand side query
            let right_results = self.execute(&set_op.right)?;

            // Apply the set operation
            results = apply_set_operation(results, right_results, set_op)?;

            // Apply LIMIT/OFFSET to the final combined result
            results = apply_limit_offset(results, stmt.limit, stmt.offset);
        }

        Ok(results)
    }

    /// Execute a FROM clause (table or join) and return combined schema and rows
    pub(super) fn execute_from(
        &self,
        from: &vibesql_ast::FromClause,
        cte_results: &HashMap<String, CteResult>,
    ) -> Result<FromResult, ExecutorError> {
        use crate::select::scan::execute_from_clause;
        execute_from_clause(from, cte_results, self.database, None, |query| self.execute_with_columns(query))
    }

    /// Execute a FROM clause with WHERE clause for predicate pushdown
    pub(super) fn execute_from_with_where(
        &self,
        from: &vibesql_ast::FromClause,
        cte_results: &HashMap<String, CteResult>,
        where_clause: Option<&vibesql_ast::Expression>,
    ) -> Result<FromResult, ExecutorError> {
        use crate::select::scan::execute_from_clause;
        execute_from_clause(from, cte_results, self.database, where_clause, |query| {
            self.execute_with_columns(query)
        })
    }
}
