//! Utility functions for window function evaluation

use ast::Expression;
use storage::Row;
use types::SqlValue;

/// Simple expression evaluation for window functions
/// TODO: This is a simplified version that handles basic cases.
/// For full integration, use ExpressionEvaluator with schema context.
pub fn evaluate_expression(expr: &Expression, row: &Row) -> Result<SqlValue, String> {
    match expr {
        Expression::Literal(val) => Ok(val.clone()),
        Expression::ColumnRef { table: _, column } => {
            // For now, try parsing column name as index (e.g., "0", "1")
            // Or use first column if it's not a number
            if let Ok(index) = column.parse::<usize>() {
                row.get(index).cloned().ok_or_else(|| format!("Column index {} out of bounds", index))
            } else {
                // Fallback: assume first column
                row.get(0).cloned().ok_or_else(|| "Row has no columns".to_string())
            }
        }
        _ => Err("Unsupported expression in window function".to_string()),
    }
}

/// Evaluate default value expression
pub fn evaluate_default_value(default: Option<&Expression>) -> Result<SqlValue, String> {
    match default {
        None => Ok(SqlValue::Null),
        Some(expr) => match expr {
            Expression::Literal(val) => Ok(val.clone()),
            _ => Err("Default value must be a literal".to_string()),
        },
    }
}
