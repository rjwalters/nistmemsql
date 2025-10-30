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
use std::collections::HashMap;
use storage::Row;
use types::SqlValue;

/// Information about a window function in the SELECT list
#[derive(Debug, Clone)]
struct WindowFunctionInfo {
    /// Index in the SELECT list
    _select_index: usize,
    /// The window function specification
    function_spec: WindowFunctionSpec,
    /// The OVER clause specification
    window_spec: ast::WindowSpec,
}

/// Key for identifying and hashing window function expressions
/// Used to map window function expressions to their pre-computed column indices
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WindowFunctionKey {
    /// Serialized representation of the window function for hashing
    /// Format: "FUNC_NAME(args)|PARTITION BY col1,col2|ORDER BY col3 ASC|FRAME_SPEC"
    pub key: String,
}

impl WindowFunctionKey {
    /// Create a key from a window function expression
    pub fn from_expression(function: &WindowFunctionSpec, window: &ast::WindowSpec) -> Self {
        let mut key_parts = Vec::new();

        // Add function name and args
        match function {
            WindowFunctionSpec::Aggregate { name, args } => {
                let args_str = args
                    .iter()
                    .map(|expr| format!("{:?}", expr))
                    .collect::<Vec<_>>()
                    .join(",");
                key_parts.push(format!("{}({})", name, args_str));
            }
            WindowFunctionSpec::Ranking { name, args } => {
                let args_str = args
                    .iter()
                    .map(|expr| format!("{:?}", expr))
                    .collect::<Vec<_>>()
                    .join(",");
                key_parts.push(format!("{}({})", name, args_str));
            }
            WindowFunctionSpec::Value { name, args } => {
                let args_str = args
                    .iter()
                    .map(|expr| format!("{:?}", expr))
                    .collect::<Vec<_>>()
                    .join(",");
                key_parts.push(format!("{}({})", name, args_str));
            }
        }

        // Add PARTITION BY clause
        if let Some(partition_by) = &window.partition_by {
            if !partition_by.is_empty() {
                let partition_str = partition_by
                    .iter()
                    .map(|expr| format!("{:?}", expr))
                    .collect::<Vec<_>>()
                    .join(",");
                key_parts.push(format!("PARTITION BY {}", partition_str));
            }
        }

        // Add ORDER BY clause
        if let Some(order_by) = &window.order_by {
            if !order_by.is_empty() {
                let order_str = order_by
                    .iter()
                    .map(|item| format!("{:?} {:?}", item.expr, item.direction))
                    .collect::<Vec<_>>()
                    .join(",");
                key_parts.push(format!("ORDER BY {}", order_str));
            }
        }

        // Add FRAME clause
        if let Some(frame) = &window.frame {
            key_parts.push(format!("{:?}", frame));
        }

        WindowFunctionKey { key: key_parts.join("|") }
    }
}

/// Check if SELECT list contains any window functions
pub(super) fn has_window_functions(select_list: &[SelectItem]) -> bool {
    select_list.iter().any(|item| match item {
        SelectItem::Expression { expr, .. } => expression_has_window_function(expr),
        SelectItem::Wildcard => false,
    })
}

/// Check if an expression contains a window function
pub(super) fn expression_has_window_function(expr: &Expression) -> bool {
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
/// # Returns
/// Returns a tuple of (rows_with_window_values, mapping) where:
/// - rows_with_window_values: Original rows with window function results appended
/// - mapping: HashMap mapping WindowFunctionKey to column index in extended row
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
/// 5. Build mapping from WindowFunctionKey to column index
pub(super) fn evaluate_window_functions(
    mut rows: Vec<Row>,
    select_list: &[SelectItem],
    evaluator: &CombinedExpressionEvaluator,
) -> Result<(Vec<Row>, HashMap<WindowFunctionKey, usize>), ExecutorError> {
    // Find all window functions in SELECT list
    let window_functions = collect_window_functions(select_list)?;

    if window_functions.is_empty() {
        return Ok((rows, HashMap::new()));
    }

    // For each window function, compute values for all rows
    // We'll build a Vec<Vec<SqlValue>> where outer vec is window functions,
    // inner vec is values for each row
    let mut window_results: Vec<Vec<SqlValue>> = Vec::new();
    let mut window_mapping = HashMap::new();

    // Track the column index where window function results start
    let base_column_count = if rows.is_empty() { 0 } else { rows[0].values.len() };

    for (idx, win_func) in window_functions.iter().enumerate() {
        let values = evaluate_single_window_function(&rows, win_func, evaluator)?;
        window_results.push(values);

        // Build mapping: WindowFunctionKey -> column index
        let key = WindowFunctionKey::from_expression(
            &win_func.function_spec,
            &win_func.window_spec,
        );
        let col_idx = base_column_count + idx;
        window_mapping.insert(key, col_idx);
    }

    // Extend each row with window function results
    for (row_idx, row) in rows.iter_mut().enumerate() {
        for results in &window_results {
            row.values.push(results[row_idx].clone());
        }
    }

    Ok((rows, window_mapping))
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
    evaluator: &CombinedExpressionEvaluator,
) -> Result<Vec<SqlValue>, ExecutorError> {
    // Extract function details
    let (func_name, args) = match &win_func.function_spec {
        WindowFunctionSpec::Aggregate { name, args } => (name.as_str(), args.as_slice()),
        WindowFunctionSpec::Ranking { name, args } => (name.as_str(), args.as_slice()),
        WindowFunctionSpec::Value { name, args } => (name.as_str(), args.as_slice()),
    };

    // Partition rows using evaluator for column resolution
    let eval_fn = |expr: &Expression, row: &Row| -> Result<SqlValue, String> {
        evaluator.eval(expr, row).map_err(|e| format!("{:?}", e))
    };
    let mut partitions = partition_rows(rows.to_vec(), &win_func.window_spec.partition_by, eval_fn);

    // Sort each partition
    for partition in &mut partitions {
        sort_partition(partition, &win_func.window_spec.order_by);
    }

    // Evaluate window function for each partition
    // We need to collect results with their original indices, then reorder
    let mut results_with_indices = Vec::new();

    for partition in &partitions {
        let partition_results = evaluate_window_function_for_partition(
            partition,
            func_name,
            args,
            &win_func.window_spec.order_by,
            &win_func.window_spec.frame,
            evaluator,
        )?;

        // Pair each result with its original index
        for (result, &original_idx) in partition_results.iter().zip(partition.original_indices.iter()) {
            results_with_indices.push((original_idx, result.clone()));
        }
    }

    // Sort by original index to restore original row order
    results_with_indices.sort_by_key(|(idx, _)| *idx);

    // Extract just the results
    let all_results = results_with_indices.into_iter().map(|(_, result)| result).collect();

    Ok(all_results)
}

/// Evaluate a window function for a single partition
fn evaluate_window_function_for_partition(
    partition: &Partition,
    func_name: &str,
    args: &[Expression],
    order_by: &Option<Vec<ast::OrderByItem>>,
    frame_spec: &Option<ast::WindowFrame>,
    evaluator: &CombinedExpressionEvaluator,
) -> Result<Vec<SqlValue>, ExecutorError> {

    // Handle ranking functions (they don't use frames)
    let results = match func_name.to_uppercase().as_str() {
        "ROW_NUMBER" => {
            crate::evaluator::window::evaluate_row_number(partition)
        }
        "RANK" => {
            crate::evaluator::window::evaluate_rank(partition, order_by)
        }
        "DENSE_RANK" => {
            crate::evaluator::window::evaluate_dense_rank(partition, order_by)
        }
        "NTILE" => {
            if args.is_empty() {
                return Err(ExecutorError::UnsupportedExpression(
                    "NTILE requires an argument".to_string(),
                ));
            }
            // Evaluate the NTILE argument (should be a constant)
            let n_value = evaluator.eval(&args[0], &partition.rows[0])?;
            let n = match n_value {
                types::SqlValue::Integer(n) => n,
                _ => return Err(ExecutorError::UnsupportedExpression(
                    "NTILE argument must be an integer".to_string(),
                )),
            };
            crate::evaluator::window::evaluate_ntile(partition, n).map_err(|e| ExecutorError::UnsupportedExpression(e))?
        }
        _ => {
            // Handle aggregate functions that use frames
            let mut results: Vec<SqlValue> = Vec::with_capacity(partition.len());

            // Evaluate function for each row in the partition
            for row_idx in 0..partition.len() {
                // Calculate frame for this row
                let frame = calculate_frame(partition, row_idx, order_by, frame_spec);

                // Create closure that evaluates expressions using the evaluator
                let eval_fn = |expr: &Expression, row: &Row| -> Result<SqlValue, String> {
                    evaluator.eval(expr, row).map_err(|e| format!("{:?}", e))
                };

                // Evaluate the aggregate function over the frame
                let value = match func_name.to_uppercase().as_str() {
            "COUNT" => {
                // COUNT(*) or COUNT(expr)
                // Check if arg is the special "*" column reference
                let arg_expr = if args.is_empty() {
                    None
                } else if matches!(&args[0], Expression::ColumnRef { column, .. } if column == "*") {
                    None  // COUNT(*) should count all rows
                } else {
                    Some(&args[0])
                };
                evaluate_count_window(partition, &frame, arg_expr, eval_fn)
            }
            "SUM" => {
                if args.is_empty() {
                    return Err(ExecutorError::UnsupportedExpression(
                        "SUM requires an argument".to_string(),
                    ));
                }
                evaluate_sum_window(partition, &frame, &args[0], eval_fn)
            }
            "AVG" => {
                if args.is_empty() {
                    return Err(ExecutorError::UnsupportedExpression(
                        "AVG requires an argument".to_string(),
                    ));
                }
                evaluate_avg_window(partition, &frame, &args[0], eval_fn)
            }
            "MIN" => {
                if args.is_empty() {
                    return Err(ExecutorError::UnsupportedExpression(
                        "MIN requires an argument".to_string(),
                    ));
                }
                evaluate_min_window(partition, &frame, &args[0], eval_fn)
            }
            "MAX" => {
                if args.is_empty() {
                    return Err(ExecutorError::UnsupportedExpression(
                        "MAX requires an argument".to_string(),
                    ));
                }
                evaluate_max_window(partition, &frame, &args[0], eval_fn)
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

            results
        }
    };

    Ok(results)
}

/// Collect window functions from ORDER BY expressions
pub(super) fn collect_order_by_window_functions(order_by: &[ast::OrderByItem]) -> Vec<(WindowFunctionSpec, ast::WindowSpec)> {
    let mut window_functions = Vec::new();

    for item in order_by {
        collect_window_functions_from_expression(&item.expr, &mut window_functions);
    }

    window_functions
}

/// Recursively collect window functions from an expression
fn collect_window_functions_from_expression(
    expr: &Expression,
    window_functions: &mut Vec<(WindowFunctionSpec, ast::WindowSpec)>,
) {
    match expr {
        Expression::WindowFunction { function, over } => {
            window_functions.push((function.clone(), over.clone()));
        }
        Expression::BinaryOp { left, right, .. } => {
            collect_window_functions_from_expression(left, window_functions);
            collect_window_functions_from_expression(right, window_functions);
        }
        Expression::UnaryOp { expr, .. } => {
            collect_window_functions_from_expression(expr, window_functions);
        }
        Expression::Function { args, .. } => {
            for arg in args {
                collect_window_functions_from_expression(arg, window_functions);
            }
        }
        Expression::Case {
            when_clauses,
            else_result,
            ..
        } => {
            for (cond, result) in when_clauses {
                collect_window_functions_from_expression(cond, window_functions);
                collect_window_functions_from_expression(result, window_functions);
            }
            if let Some(else_result) = else_result {
                collect_window_functions_from_expression(else_result, window_functions);
            }
        }
        Expression::In { expr, .. } => {
            collect_window_functions_from_expression(expr, window_functions);
        }
        Expression::InList { expr, .. } => {
            collect_window_functions_from_expression(expr, window_functions);
        }
        Expression::Between { expr, low, high, .. } => {
            collect_window_functions_from_expression(expr, window_functions);
            collect_window_functions_from_expression(low, window_functions);
            collect_window_functions_from_expression(high, window_functions);
        }
        Expression::Like { expr, pattern, .. } => {
            collect_window_functions_from_expression(expr, window_functions);
            collect_window_functions_from_expression(pattern, window_functions);
        }
        Expression::Cast { expr, .. } => {
            collect_window_functions_from_expression(expr, window_functions);
        }
        Expression::IsNull { expr, .. } => {
            collect_window_functions_from_expression(expr, window_functions);
        }
        Expression::Position { substring, string } => {
            collect_window_functions_from_expression(substring, window_functions);
            collect_window_functions_from_expression(string, window_functions);
        }
        Expression::Trim {
            removal_char,
            string,
            ..
        } => {
            if let Some(removal_char) = removal_char {
                collect_window_functions_from_expression(removal_char, window_functions);
            }
            collect_window_functions_from_expression(string, window_functions);
        }
        Expression::Exists { .. } | Expression::ScalarSubquery(_) | Expression::QuantifiedComparison { .. } => {
            // These don't contain window functions in their expressions
        }
        Expression::AggregateFunction { .. } => {
            // Aggregate functions don't contain window functions
        }
        Expression::Wildcard => {
            // Wildcard doesn't contain window functions
        }
        Expression::Literal(_) | Expression::ColumnRef { .. } => {
            // These are leaf nodes
        }
    }
}

/// Evaluate window functions found in ORDER BY expressions
pub(super) fn evaluate_order_by_window_functions(
    mut rows: Vec<storage::Row>,
    order_by_window_functions: Vec<(WindowFunctionSpec, ast::WindowSpec)>,
    evaluator: &CombinedExpressionEvaluator,
    existing_mapping: Option<&HashMap<WindowFunctionKey, usize>>,
) -> Result<(Vec<storage::Row>, HashMap<WindowFunctionKey, usize>), ExecutorError> {
    if order_by_window_functions.is_empty() {
        return Ok((rows, HashMap::new()));
    }

    // Build mapping from existing functions to avoid duplicates
    let existing_keys: std::collections::HashSet<_> = existing_mapping
        .map(|m| m.keys().collect())
        .unwrap_or_default();

    let mut window_results: Vec<Vec<SqlValue>> = Vec::new();
    let mut window_mapping = HashMap::new();

    // Track the column index where window function results start
    let base_column_count = if rows.is_empty() { 0 } else { rows[0].values.len() };

    for (idx, (function_spec, window_spec)) in order_by_window_functions.iter().enumerate() {
        let key = WindowFunctionKey::from_expression(function_spec, window_spec);

        // Skip if this window function is already evaluated
        if existing_keys.contains(&key) {
            continue;
        }

        let win_func_info = WindowFunctionInfo {
            _select_index: 0, // Dummy value for ORDER BY functions
            function_spec: function_spec.clone(),
            window_spec: window_spec.clone(),
        };
        let values = evaluate_single_window_function(&rows, &win_func_info, evaluator)?;
        window_results.push(values);

        // Build mapping: WindowFunctionKey -> column index
        let col_idx = base_column_count + idx;
        window_mapping.insert(key, col_idx);
    }

    // Extend each row with window function results
    for (row_idx, row) in rows.iter_mut().enumerate() {
        for results in &window_results {
            row.values.push(results[row_idx].clone());
        }
    }

    Ok((rows, window_mapping))
}
