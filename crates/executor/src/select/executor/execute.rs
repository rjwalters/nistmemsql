//! Main execution methods for SelectExecutor

use super::builder::SelectExecutor;
use crate::errors::ExecutorError;
use crate::optimizer::PredicateDecomposition;
use crate::select::cte::{execute_ctes, CteResult};
use crate::select::helpers::apply_limit_offset;
use crate::select::join::FromResult;
use crate::select::set_operations::apply_set_operation;
use crate::select::SelectResult;
use std::collections::HashMap;

impl SelectExecutor<'_> {
    /// Execute a SELECT statement
    pub fn execute(&self, stmt: &ast::SelectStmt) -> Result<Vec<storage::Row>, ExecutorError> {
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
        stmt: &ast::SelectStmt,
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
        stmt: &ast::SelectStmt,
        cte_results: &HashMap<String, CteResult>,
    ) -> Result<Vec<storage::Row>, ExecutorError> {
        // Execute the left-hand side query
        let has_aggregates = self.has_aggregates(&stmt.select_list) || stmt.having.is_some();
        let has_group_by = stmt.group_by.is_some();

        let mut results = if has_aggregates || has_group_by {
            self.execute_with_aggregation(stmt, cte_results)?
        } else if let Some(from_clause) = &stmt.from {
            // For non-aggregated queries, decompose WHERE predicates to enable pushdown optimization
            let predicates = if let Some(where_expr) = &stmt.where_clause {
                use crate::optimizer::decompose_where_clause;
                // Create a dummy schema for initial predicate decomposition
                // The real schema will be built during FROM execution
                let dummy_schema = crate::schema::CombinedSchema {
                    table_schemas: HashMap::new(),
                    total_columns: 0,
                };
                match decompose_where_clause(Some(where_expr), &dummy_schema) {
                    Ok(decomp) => Some(decomp),
                    Err(_) => None, // If decomposition fails, continue without optimization
                }
            } else {
                None
            };
            
            let from_result = if let Some(ref pred) = predicates {
                self.execute_from_with_predicates(from_clause, &cte_results, Some(pred))?
            } else {
                self.execute_from(from_clause, &cte_results)?
            };
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
        from: &ast::FromClause,
        cte_results: &HashMap<String, CteResult>,
    ) -> Result<FromResult, ExecutorError> {
        self.execute_from_with_predicates(from, cte_results, None)
    }

    /// Execute a FROM clause with optional WHERE predicate pushdown
    pub(super) fn execute_from_with_predicates(
        &self,
        from: &ast::FromClause,
        cte_results: &HashMap<String, CteResult>,
        predicates: Option<&PredicateDecomposition>,
    ) -> Result<FromResult, ExecutorError> {
        use crate::select::scan::execute_from_clause_with_predicates;
        execute_from_clause_with_predicates(from, cte_results, self.database, |query| self.execute(query), predicates)
    }
}
