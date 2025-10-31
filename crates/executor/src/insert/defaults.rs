use crate::errors::ExecutorError;

/// Evaluate an INSERT expression to SqlValue
/// Supports literals and DEFAULT keyword
pub fn evaluate_insert_expression(
    expr: &ast::Expression,
    column: &catalog::ColumnSchema,
) -> Result<types::SqlValue, ExecutorError> {
    match expr {
        ast::Expression::Literal(lit) => Ok(lit.clone()),
        ast::Expression::Default => {
            // Use column's default value, or NULL if no default is defined
            if let Some(default_expr) = &column.default_value {
                // Evaluate the default expression
                evaluate_default_expression(default_expr)
            } else {
                // No default value defined, use NULL
                Ok(types::SqlValue::Null)
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
    expr: &ast::Expression,
) -> Result<types::SqlValue, ExecutorError> {
    match expr {
        ast::Expression::Literal(lit) => Ok(lit.clone()),
        ast::Expression::Function { name, .. } => {
            // Evaluate special SQL functions that can be used in DEFAULT
            match name.to_uppercase().as_str() {
                "CURRENT_DATE" => {
                    Ok(types::SqlValue::Date(chrono::Local::now().date_naive().to_string()))
                }
                "CURRENT_TIME" => {
                    Ok(types::SqlValue::Time(chrono::Local::now().time().to_string()))
                }
                "CURRENT_TIMESTAMP" => {
                    let now = chrono::Local::now();
                    Ok(types::SqlValue::Timestamp(now.naive_local().to_string()))
                }
                "CURRENT_USER" | "USER" | "SESSION_USER" => {
                    // Return current user (placeholder - would come from session context)
                    Ok(types::SqlValue::Varchar("public".to_string()))
                }
                "CURRENT_ROLE" => {
                    // Return current role (placeholder - would come from session context)
                    Ok(types::SqlValue::Varchar("public".to_string()))
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
    schema: &catalog::TableSchema,
    row_values: &mut [types::SqlValue],
) -> Result<(), ExecutorError> {
    for (col_idx, col) in schema.columns.iter().enumerate() {
        // If column is NULL and has a default value, apply it
        if row_values[col_idx] == types::SqlValue::Null {
            if let Some(default_expr) = &col.default_value {
                let default_value = evaluate_default_expression(default_expr)?;
                let coerced_value = super::validation::coerce_value(default_value, &col.data_type)?;
                row_values[col_idx] = coerced_value;
            }
        }
    }
    Ok(())
}
