use crate::errors::ExecutorError;

/// Evaluate an INSERT expression to SqlValue
/// Supports literals and DEFAULT keyword
pub fn evaluate_insert_expression(
    expr: &vibesql_ast::Expression,
    column: &vibesql_catalog::ColumnSchema,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    match expr {
        vibesql_ast::Expression::Literal(lit) => Ok(lit.clone()),
        vibesql_ast::Expression::Default => {
            // Use column's default value, or NULL if no default is defined
            if let Some(default_expr) = &column.default_value {
                // Evaluate the default expression
                evaluate_default_expression(default_expr)
            } else {
                // No default value defined, use NULL
                Ok(vibesql_types::SqlValue::Null)
            }
        }
        _ => Err(ExecutorError::UnsupportedExpression(
            "INSERT only supports literal values and DEFAULT".to_string(),
        )),
    }
}

/// Evaluate a DEFAULT expression to get its value
/// Supports literals and special functions (CURRENT_DATE, CURRENT_USER, etc.)
pub fn evaluate_default_expression(
    expr: &vibesql_ast::Expression,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    match expr {
        vibesql_ast::Expression::Literal(lit) => Ok(lit.clone()),
        vibesql_ast::Expression::Function { name, .. } => {
            // Evaluate special SQL functions that can be used in DEFAULT
            match name.to_uppercase().as_str() {
                "CURRENT_DATE" => {
                    use chrono::Datelike;
                    let now = chrono::Local::now();
                    let date = vibesql_types::Date::new(now.year(), now.month() as u8, now.day() as u8)
                        .map_err(|e| ExecutorError::UnsupportedFeature(format!("Failed to create date: {}", e)))?;
                    Ok(vibesql_types::SqlValue::Date(date))
                }
                "CURRENT_TIME" => {
                    use chrono::Timelike;
                    let now = chrono::Local::now();
                    let time_naive = now.time();
                    let time = vibesql_types::Time::new(
                        time_naive.hour() as u8,
                        time_naive.minute() as u8,
                        time_naive.second() as u8,
                        time_naive.nanosecond(),
                    ).map_err(|e| ExecutorError::UnsupportedFeature(format!("Failed to create time: {}", e)))?;
                    Ok(vibesql_types::SqlValue::Time(time))
                }
                "CURRENT_TIMESTAMP" => {
                    use chrono::{Datelike, Timelike};
                    let now = chrono::Local::now();
                    let time_naive = now.time();
                    let date = vibesql_types::Date::new(now.year(), now.month() as u8, now.day() as u8)
                        .map_err(|e| ExecutorError::UnsupportedFeature(format!("Failed to create date: {}", e)))?;
                    let time = vibesql_types::Time::new(
                        time_naive.hour() as u8,
                        time_naive.minute() as u8,
                        time_naive.second() as u8,
                        time_naive.nanosecond(),
                    ).map_err(|e| ExecutorError::UnsupportedFeature(format!("Failed to create time: {}", e)))?;
                    Ok(vibesql_types::SqlValue::Timestamp(vibesql_types::Timestamp::new(date, time)))
                }
                "CURRENT_USER" | "USER" | "SESSION_USER" => {
                    // Return current user (placeholder - would come from session context)
                    Ok(vibesql_types::SqlValue::Varchar("public".to_string()))
                }
                "CURRENT_ROLE" => {
                    // Return current role (placeholder - would come from session context)
                    Ok(vibesql_types::SqlValue::Varchar("public".to_string()))
                }
                _ => Err(ExecutorError::UnsupportedExpression(format!(
                    "Function '{}' not supported in DEFAULT expressions",
                    name
                ))),
            }
        }
        _ => Err(ExecutorError::UnsupportedExpression(
            "Only literals and functions are supported in DEFAULT expressions".to_string(),
        )),
    }
}

/// Apply DEFAULT values for unspecified columns
pub fn apply_default_values(
    schema: &vibesql_catalog::TableSchema,
    row_values: &mut [vibesql_types::SqlValue],
) -> Result<(), ExecutorError> {
    for (col_idx, col) in schema.columns.iter().enumerate() {
        // If column is NULL and has a default value, apply it
        if row_values[col_idx] == vibesql_types::SqlValue::Null {
            if let Some(default_expr) = &col.default_value {
                let default_value = evaluate_default_expression(default_expr)?;
                let coerced_value = super::validation::coerce_value(default_value, &col.data_type)?;
                row_values[col_idx] = coerced_value;
            }
        }
    }
    Ok(())
}
