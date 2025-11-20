//! Validation functions for WHERE clause subqueries
//!
//! This module validates IN subqueries in WHERE clauses before row iteration,
//! ensuring schema validation happens even when there are no rows to process.

use crate::errors::ExecutorError;

/// Validate IN subqueries in WHERE clause before row iteration
/// This ensures schema validation happens even when there are no rows to process
pub(super) fn validate_where_clause_subqueries(
    expr: &vibesql_ast::Expression,
    database: &vibesql_storage::Database,
) -> Result<(), ExecutorError> {
    use vibesql_ast::Expression;

    match expr {
        Expression::In { subquery, .. } => {
            // Validate that the subquery returns exactly 1 column (scalar subquery requirement)
            let column_count = compute_select_list_column_count(subquery, database)?;
            if column_count != 1 {
                return Err(ExecutorError::SubqueryColumnCountMismatch {
                    expected: 1,
                    actual: column_count,
                });
            }
            Ok(())
        }
        // Recurse into binary operations
        Expression::BinaryOp { left, right, .. } => {
            validate_where_clause_subqueries(left, database)?;
            validate_where_clause_subqueries(right, database)
        }
        // Recurse into unary operations
        Expression::UnaryOp { expr, .. } => validate_where_clause_subqueries(expr, database),
        // Recurse into other composite expressions
        Expression::IsNull { expr, .. } => validate_where_clause_subqueries(expr, database),
        Expression::InList { expr, values, .. } => {
            validate_where_clause_subqueries(expr, database)?;
            for val in values {
                validate_where_clause_subqueries(val, database)?;
            }
            Ok(())
        }
        Expression::Between { expr, low, high, .. } => {
            validate_where_clause_subqueries(expr, database)?;
            validate_where_clause_subqueries(low, database)?;
            validate_where_clause_subqueries(high, database)
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                validate_where_clause_subqueries(op, database)?;
            }
            for when_clause in when_clauses {
                for cond in &when_clause.conditions {
                    validate_where_clause_subqueries(cond, database)?;
                }
                validate_where_clause_subqueries(&when_clause.result, database)?;
            }
            if let Some(else_res) = else_result {
                validate_where_clause_subqueries(else_res, database)?;
            }
            Ok(())
        }
        // For all other expressions, no validation needed
        _ => Ok(()),
    }
}

/// Compute the number of columns in a SELECT statement's result
/// Handles wildcards by expanding them using table schemas from the database
fn compute_select_list_column_count(
    stmt: &vibesql_ast::SelectStmt,
    database: &vibesql_storage::Database,
) -> Result<usize, ExecutorError> {
    let mut count = 0;

    for item in &stmt.select_list {
        match item {
            vibesql_ast::SelectItem::Wildcard { .. } => {
                // Expand * to count all columns from all tables in FROM clause
                if let Some(from) = &stmt.from {
                    count += count_columns_in_from_clause(from, database)?;
                } else {
                    // SELECT * without FROM is an error (should be caught earlier)
                    return Err(ExecutorError::UnsupportedFeature(
                        "SELECT * requires FROM clause".to_string(),
                    ));
                }
            }
            vibesql_ast::SelectItem::QualifiedWildcard { qualifier, .. } => {
                // Expand table.* to count columns from that specific table
                let tbl = database
                    .get_table(qualifier)
                    .ok_or_else(|| ExecutorError::TableNotFound(qualifier.clone()))?;
                count += tbl.schema.columns.len();
            }
            vibesql_ast::SelectItem::Expression { .. } => {
                // Each expression contributes one column
                count += 1;
            }
        }
    }

    Ok(count)
}

/// Count total columns in a FROM clause (handles joins and multiple tables)
fn count_columns_in_from_clause(
    from: &vibesql_ast::FromClause,
    database: &vibesql_storage::Database,
) -> Result<usize, ExecutorError> {
    match from {
        vibesql_ast::FromClause::Table { name, .. } => {
            let table = database
                .get_table(name)
                .ok_or_else(|| ExecutorError::TableNotFound(name.clone()))?;
            Ok(table.schema.columns.len())
        }
        vibesql_ast::FromClause::Join { left, right, .. } => {
            let left_count = count_columns_in_from_clause(left, database)?;
            let right_count = count_columns_in_from_clause(right, database)?;
            Ok(left_count + right_count)
        }
        vibesql_ast::FromClause::Subquery { .. } => {
            // For subqueries in FROM, we'd need to execute them to know column count
            // This is complex, so for now we'll return an error
            // In practice, this case is rare in IN subqueries
            Err(ExecutorError::UnsupportedFeature(
                "Subqueries in FROM clause within IN predicates are not yet supported for schema validation".to_string(),
            ))
        }
    }
}
