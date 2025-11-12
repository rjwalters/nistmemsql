//! Predicate pushdown and filtering logic
//!
//! Handles optimization of WHERE clause predicates by:
//! - Decomposing WHERE clause into table-local predicates
//! - Applying predicates early during table scans
//! - Reducing intermediate result sizes before joins

use crate::{
    errors::ExecutorError, evaluator::CombinedExpressionEvaluator,
    optimizer::decompose_where_clause, schema::CombinedSchema,
};

/// Apply table-local predicates from WHERE clause during table scan
///
/// This function implements predicate pushdown by filtering rows early,
/// before they contribute to larger Cartesian products in JOINs.
pub(crate) fn apply_table_local_predicates(
    rows: Vec<vibesql_storage::Row>,
    schema: CombinedSchema,
    where_clause: &vibesql_ast::Expression,
    table_name: &str,
    database: &vibesql_storage::Database,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    // Decompose WHERE clause using branch-specific API with schema
    let decomposition = decompose_where_clause(Some(where_clause), &schema)
        .map_err(ExecutorError::InvalidWhereClause)?;

    // Extract predicates that can be applied to this table
    let table_local_preds: Option<&Vec<vibesql_ast::Expression>> =
        decomposition.table_local_predicates.get(table_name);

    // If there are table-local predicates, apply them
    if let Some(preds) = table_local_preds {
        if !preds.is_empty() {
            // Combine predicates with AND
            let combined_where = combine_predicates_with_and(preds.clone());

            // Create evaluator for filtering
            let evaluator = CombinedExpressionEvaluator::with_database(&schema, database);

            // Apply filtering to rows directly (without executor for timeout checking)
            let mut filtered_rows = Vec::new();
            for row in rows {
                let include_row = match evaluator.eval(&combined_where, &row)? {
                    vibesql_types::SqlValue::Boolean(true) => true,
                    vibesql_types::SqlValue::Boolean(false) | vibesql_types::SqlValue::Null => false,
                    // SQLLogicTest compatibility: treat integers as truthy/falsy (C-like behavior)
                    vibesql_types::SqlValue::Integer(0) => false,
                    vibesql_types::SqlValue::Integer(_) => true,
                    vibesql_types::SqlValue::Smallint(0) => false,
                    vibesql_types::SqlValue::Smallint(_) => true,
                    vibesql_types::SqlValue::Bigint(0) => false,
                    vibesql_types::SqlValue::Bigint(_) => true,
                    vibesql_types::SqlValue::Float(0.0) => false,
                    vibesql_types::SqlValue::Float(_) => true,
                    vibesql_types::SqlValue::Real(0.0) => false,
                    vibesql_types::SqlValue::Real(_) => true,
                    vibesql_types::SqlValue::Double(0.0) => false,
                    vibesql_types::SqlValue::Double(_) => true,
                    other => {
                        return Err(ExecutorError::InvalidWhereClause(format!(
                            "WHERE clause must evaluate to boolean, got: {:?}",
                            other
                        )))
                    }
                };

                if include_row {
                    filtered_rows.push(row);
                }
            }
            return Ok(filtered_rows);
        }
    }

    // No table-local predicates - return rows as-is
    Ok(rows)
}

/// Helper function to combine predicates with AND operator
pub(crate) fn combine_predicates_with_and(mut predicates: Vec<vibesql_ast::Expression>) -> vibesql_ast::Expression {
    if predicates.is_empty() {
        // This shouldn't happen, but default to TRUE
        vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(true))
    } else if predicates.len() == 1 {
        predicates.pop().unwrap()
    } else {
        let mut result = predicates.remove(0);
        for predicate in predicates {
            result = vibesql_ast::Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::And,
                left: Box::new(result),
                right: Box::new(predicate),
            };
        }
        result
    }
}
