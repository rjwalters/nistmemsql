//! Window function integration for SELECT executor
//!
//! This module handles evaluation of window functions (with OVER clause) in SELECT queries.
//! Window functions are different from regular aggregates - they don't collapse rows.

use crate::errors::ExecutorError;
use crate::evaluator::window::{
    calculate_frame, evaluate_avg_window, evaluate_count_window, evaluate_max_window,
    evaluate_min_window, evaluate_sum_window, partition_rows, sort_partition, Partition,
};
use crate::evaluator::CombinedExpressionEvaluator;
use ast::{Expression, SelectItem, WindowFunctionSpec};
use storage::Row;
use types::SqlValue;

/// Information about a window function in the SELECT list
#[derive(Debug, Clone)]
struct WindowFunctionInfo {
    /// Index in the SELECT list
    select_index: usize,
    /// The window function specification
    function_spec: WindowFunctionSpec,
    /// The OVER clause specification
    window_spec: ast::WindowSpec,
}

/// Check if SELECT list contains any window functions
pub(super) fn has_window_functions(select_list: &[SelectItem]) -> bool {
    select_list.iter().any(|item| match item {
        SelectItem::Expression { expr, .. } => expression_has_window_function(expr),
        SelectItem::Wildcard => false,
    })
}

/// Check if an expression contains a window function
fn expression_has_window_function(expr: &Expression) -> bool {
    match expr {
        Expression::WindowFunction { .. } => true,
        Expression::BinaryOp { left, right, .. } => {
            expression_has_window_function(left) || expression_has_window_function(right)
        }
        Expression::UnaryOp { expr, .. } => expression_has_window_function(expr),
        Expression::Function { args, .. } => {
            args.iter().any(|arg| expression_has_window_function(arg))
        }
        Expression::Case {
            when_clauses,
            else_result,
            ..
        } => {
            when_clauses.iter().any(|(cond, result)| {
                expression_has_window_function(cond) || expression_has_window_function(result)
            }) || else_result
                .as_ref()
                .map_or(false, |e| expression_has_window_function(e))
        }
        _ => false,
    }
}

/// Evaluate window functions and add results to rows
///
/// This processes all window functions in the SELECT list and adds computed values
/// to each row. Window functions don't collapse rows like GROUP BY - each input row
/// produces one output row with window function values added.
///
/// # Algorithm
/// 1. Find all window functions in SELECT list
/// 2. Group window functions by their window specification (for optimization)
/// 3. For each unique window spec:
///    - Partition rows according to PARTITION BY
///    - Sort each partition according to ORDER BY
///    - For each row, calculate frame and evaluate window functions
///    - Store results
/// 4. Extend each row with window function results
pub(super) fn evaluate_window_functions(
    mut rows: Vec<Row>,
    select_list: &[SelectItem],
    _evaluator: &CombinedExpressionEvaluator,
) -> Result<Vec<Row>, ExecutorError> {
    // Find all window functions in SELECT list
    let window_functions = collect_window_functions(select_list)?;

    if window_functions.is_empty() {
        return Ok(rows);
    }

    // For each window function, compute values for all rows
    // We'll build a Vec<Vec<SqlValue>> where outer vec is window functions,
    // inner vec is values for each row
    let mut window_results: Vec<Vec<SqlValue>> = Vec::new();

    for win_func in &window_functions {
        let values = evaluate_single_window_function(&rows, win_func)?;
        window_results.push(values);
    }

    // Extend each row with window function results
    for (row_idx, row) in rows.iter_mut().enumerate() {
        for results in &window_results {
            row.values.push(results[row_idx].clone());
        }
    }

    Ok(rows)
}

/// Collect all window functions from SELECT list
fn collect_window_functions(select_list: &[SelectItem]) -> Result<Vec<WindowFunctionInfo>, ExecutorError> {
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
                    select_index,
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
            for (cond, result) in when_clauses {
                collect_from_expression(cond, select_index, window_functions)?;
                collect_from_expression(result, select_index, window_functions)?;
            }
            if let Some(else_expr) = else_result {
                collect_from_expression(else_expr, select_index, window_functions)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Evaluate a single window function over all rows
fn evaluate_single_window_function(
    rows: &[Row],
    win_func: &WindowFunctionInfo,
) -> Result<Vec<SqlValue>, ExecutorError> {
    // Extract function details
    let (func_name, args) = match &win_func.function_spec {
        WindowFunctionSpec::Aggregate { name, args } => (name.as_str(), args),
        _ => {
            return Err(ExecutorError::UnsupportedFeature(
                "Only aggregate window functions are supported in this phase".to_string(),
            ))
        }
    };

    // Partition rows
    let mut partitions = partition_rows(rows.to_vec(), &win_func.window_spec.partition_by);

    // Sort each partition
    for partition in &mut partitions {
        sort_partition(partition, &win_func.window_spec.order_by);
    }

    // Evaluate window function for each partition
    let mut all_results = Vec::new();

    for partition in &partitions {
        let partition_results = evaluate_window_function_for_partition(
            partition,
            func_name,
            args,
            &win_func.window_spec.frame,
        )?;
        all_results.extend(partition_results);
    }

    Ok(all_results)
}

/// Evaluate a window function for a single partition
fn evaluate_window_function_for_partition(
    partition: &Partition,
    func_name: &str,
    args: &[Expression],
    frame_spec: &Option<ast::WindowFrame>,
) -> Result<Vec<SqlValue>, ExecutorError> {
    let mut results = Vec::with_capacity(partition.len());

    // Evaluate function for each row in the partition
    for row_idx in 0..partition.len() {
        // Calculate frame for this row
        let frame = calculate_frame(partition, row_idx, frame_spec);

        // Evaluate the aggregate function over the frame
        let value = match func_name.to_uppercase().as_str() {
            "COUNT" => {
                // COUNT(*) or COUNT(expr)
                let arg_expr = if args.is_empty() {
                    None
                } else {
                    Some(&args[0])
                };
                evaluate_count_window(partition, &frame, arg_expr)
            }
            "SUM" => {
                if args.is_empty() {
                    return Err(ExecutorError::UnsupportedExpression(
                        "SUM requires an argument".to_string(),
                    ));
                }
                evaluate_sum_window(partition, &frame, &args[0])
            }
            "AVG" => {
                if args.is_empty() {
                    return Err(ExecutorError::UnsupportedExpression(
                        "AVG requires an argument".to_string(),
                    ));
                }
                evaluate_avg_window(partition, &frame, &args[0])
            }
            "MIN" => {
                if args.is_empty() {
                    return Err(ExecutorError::UnsupportedExpression(
                        "MIN requires an argument".to_string(),
                    ));
                }
                evaluate_min_window(partition, &frame, &args[0])
            }
            "MAX" => {
                if args.is_empty() {
                    return Err(ExecutorError::UnsupportedExpression(
                        "MAX requires an argument".to_string(),
                    ));
                }
                evaluate_max_window(partition, &frame, &args[0])
            }
            _ => {
                return Err(ExecutorError::UnsupportedExpression(format!(
                    "Unsupported window function: {}",
                    func_name
                )))
            }
        };

        results.push(value);
    }

    Ok(results)
}
