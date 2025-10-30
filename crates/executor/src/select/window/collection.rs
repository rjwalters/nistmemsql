//! Window function collection logic

use super::types::WindowFunctionInfo;
use crate::errors::ExecutorError;
use ast::{Expression, SelectItem, WindowFunctionSpec};

/// Collect all window functions from SELECT list
pub(super) fn collect_window_functions(select_list: &[SelectItem]) -> Result<Vec<WindowFunctionInfo>, ExecutorError> {
    let mut window_functions = Vec::new();

    for (idx, item) in select_list.iter().enumerate() {
        if let SelectItem::Expression { expr, .. } = item {
            collect_from_expression(expr, idx, &mut window_functions)?;
        }
    }

    Ok(window_functions)
}

/// Recursively collect window functions from an expression
fn collect_from_expression(
    expr: &Expression,
    select_index: usize,
    window_functions: &mut Vec<WindowFunctionInfo>,
) -> Result<(), ExecutorError> {
    match expr {
        Expression::WindowFunction { function, over } => {
            // Only handle aggregate window functions for now
            if let WindowFunctionSpec::Aggregate { .. } = function {
                window_functions.push(WindowFunctionInfo {
                    _select_index: select_index,
                    function_spec: function.clone(),
                    window_spec: over.clone(),
                });
            }
        }
        Expression::BinaryOp { left, right, .. } => {
            collect_from_expression(left, select_index, window_functions)?;
            collect_from_expression(right, select_index, window_functions)?;
        }
        Expression::UnaryOp { expr, .. } => {
            collect_from_expression(expr, select_index, window_functions)?;
        }
        Expression::Function { args, .. } => {
            for arg in args {
                collect_from_expression(arg, select_index, window_functions)?;
            }
        }
        Expression::Case {
            when_clauses,
            else_result,
            ..
        } => {
            for when_clause in when_clauses {
                for cond in &when_clause.conditions {
                    collect_from_expression(cond, select_index, window_functions)?;
                }
                collect_from_expression(&when_clause.result, select_index, window_functions)?;
            }
            if let Some(else_expr) = else_result {
                collect_from_expression(else_expr, select_index, window_functions)?;
            }
        }
        _ => {}
    }
    Ok(())
}
