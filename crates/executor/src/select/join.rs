use crate::errors::ExecutorError;
use crate::evaluator::CombinedExpressionEvaluator;
use crate::schema::CombinedSchema;

/// Result of executing a FROM clause
pub(super) struct FromResult {
    pub(super) schema: CombinedSchema,
    pub(super) rows: Vec<storage::Row>,
}

/// Perform nested loop join between two FROM results
pub(super) fn nested_loop_join(
    left: FromResult,
    right: FromResult,
    join_type: &ast::JoinType,
    condition: &Option<ast::Expression>,
) -> Result<FromResult, ExecutorError> {
    match join_type {
        ast::JoinType::Inner => nested_loop_inner_join(left, right, condition),
        ast::JoinType::LeftOuter => nested_loop_left_outer_join(left, right, condition),
        _ => Err(ExecutorError::UnsupportedFeature(format!(
            "JOIN type {:?} not yet implemented",
            join_type
        ))),
    }
}

/// Nested loop INNER JOIN implementation
pub(super) fn nested_loop_inner_join(
    left: FromResult,
    right: FromResult,
    condition: &Option<ast::Expression>,
) -> Result<FromResult, ExecutorError> {
    // Extract right table name (assume single table for now)
    let right_table_name = right
        .schema
        .table_schemas
        .keys()
        .next()
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .clone();

    let right_schema = right
        .schema
        .table_schemas
        .get(&right_table_name)
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .1
        .clone();

    // Combine schemas
    let combined_schema = CombinedSchema::combine(left.schema, right_table_name, right_schema);
    let evaluator = CombinedExpressionEvaluator::new(&combined_schema);

    // Nested loop join algorithm
    let mut result_rows = Vec::new();
    for left_row in &left.rows {
        for right_row in &right.rows {
            // Concatenate rows
            let mut combined_values = left_row.values.clone();
            combined_values.extend(right_row.values.clone());
            let combined_row = storage::Row::new(combined_values);

            // Evaluate join condition
            let matches = if let Some(cond) = condition {
                match evaluator.eval(cond, &combined_row)? {
                    types::SqlValue::Boolean(true) => true,
                    types::SqlValue::Boolean(false) => false,
                    types::SqlValue::Null => false,
                    other => {
                        return Err(ExecutorError::InvalidWhereClause(format!(
                            "JOIN condition must evaluate to boolean, got: {:?}",
                            other
                        )))
                    }
                }
            } else {
                true // No condition = CROSS JOIN
            };

            if matches {
                result_rows.push(combined_row);
            }
        }
    }

    Ok(FromResult { schema: combined_schema, rows: result_rows })
}

/// Nested loop LEFT OUTER JOIN implementation
pub(super) fn nested_loop_left_outer_join(
    left: FromResult,
    right: FromResult,
    condition: &Option<ast::Expression>,
) -> Result<FromResult, ExecutorError> {
    // Extract right table name and schema
    let right_table_name = right
        .schema
        .table_schemas
        .keys()
        .next()
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .clone();

    let right_schema = right
        .schema
        .table_schemas
        .get(&right_table_name)
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .1
        .clone();

    let right_column_count = right_schema.columns.len();

    // Combine schemas
    let combined_schema = CombinedSchema::combine(left.schema, right_table_name, right_schema);
    let evaluator = CombinedExpressionEvaluator::new(&combined_schema);

    // Nested loop LEFT OUTER JOIN algorithm
    let mut result_rows = Vec::new();
    for left_row in &left.rows {
        let mut matched = false;

        for right_row in &right.rows {
            // Concatenate rows
            let mut combined_values = left_row.values.clone();
            combined_values.extend(right_row.values.clone());
            let combined_row = storage::Row::new(combined_values);

            // Evaluate join condition
            let matches = if let Some(cond) = condition {
                match evaluator.eval(cond, &combined_row)? {
                    types::SqlValue::Boolean(true) => true,
                    types::SqlValue::Boolean(false) => false,
                    types::SqlValue::Null => false,
                    other => {
                        return Err(ExecutorError::InvalidWhereClause(format!(
                            "JOIN condition must evaluate to boolean, got: {:?}",
                            other
                        )))
                    }
                }
            } else {
                true // No condition = CROSS JOIN
            };

            if matches {
                result_rows.push(combined_row);
                matched = true;
            }
        }

        // If no match found, add left row with NULLs for right columns
        if !matched {
            let mut combined_values = left_row.values.clone();
            // Add NULL values for all right table columns
            combined_values.extend(vec![types::SqlValue::Null; right_column_count]);
            result_rows.push(storage::Row::new(combined_values));
        }
    }

    Ok(FromResult { schema: combined_schema, rows: result_rows })
}
