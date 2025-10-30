//! Core window function evaluation logic

use super::types::WindowFunctionInfo;
use crate::errors::ExecutorError;
use crate::evaluator::window::{
    calculate_frame, evaluate_avg_window, evaluate_count_window, evaluate_max_window,
    evaluate_min_window, evaluate_sum_window, partition_rows, sort_partition, Partition,
};
use crate::evaluator::CombinedExpressionEvaluator;
use ast::{Expression, WindowFunctionSpec};
use storage::Row;
use types::SqlValue;

/// Evaluate a single window function over all rows
pub(super) fn evaluate_single_window_function(
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
        for (result, &original_idx) in
            partition_results.iter().zip(partition.original_indices.iter())
        {
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
        "ROW_NUMBER" => crate::evaluator::window::evaluate_row_number(partition),
        "RANK" => crate::evaluator::window::evaluate_rank(partition, order_by),
        "DENSE_RANK" => crate::evaluator::window::evaluate_dense_rank(partition, order_by),
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
                _ => {
                    return Err(ExecutorError::UnsupportedExpression(
                        "NTILE argument must be an integer".to_string(),
                    ))
                }
            };
            crate::evaluator::window::evaluate_ntile(partition, n)
                .map_err(|e| ExecutorError::UnsupportedExpression(e))?
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
                        } else if matches!(&args[0], Expression::ColumnRef { column, .. } if column == "*")
                        {
                            None // COUNT(*) should count all rows
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
