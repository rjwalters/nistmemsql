use super::*;
use crate::evaluator::window::utils::evaluate_expression;
use ast::{Expression, FrameBound, FrameUnit, OrderByItem, OrderDirection, WindowFrame};
use storage::Row;
use types::SqlValue;

fn make_test_rows(values: Vec<i64>) -> Vec<Row> {
    values.into_iter().map(|v| Row::new(vec![SqlValue::Integer(v)])).collect()
}

#[test]
fn test_partition_rows_no_partition_by() {
    let rows = make_test_rows(vec![1, 2, 3]);
    let partitions = partition_rows(rows, &None, evaluate_expression);

    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0].len(), 3);
}

#[test]
fn test_partition_rows_empty_partition_by() {
    let rows = make_test_rows(vec![1, 2, 3]);
    let partitions = partition_rows(rows, &Some(vec![]), evaluate_expression);

    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0].len(), 3);
}

#[test]
fn test_calculate_frame_default() {
    let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5]));

    // Default frame WITHOUT ORDER BY: entire partition
    let frame = calculate_frame(&partition, 2, &None, &None);

    assert_eq!(frame, 0..5); // Entire partition (no ORDER BY)
}

#[test]
fn test_calculate_frame_unbounded_preceding() {
    let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5]));

    let frame_spec = WindowFrame {
        unit: FrameUnit::Rows,
        start: FrameBound::UnboundedPreceding,
        end: Some(FrameBound::CurrentRow),
    };

    let frame = calculate_frame(&partition, 2, &None, &Some(frame_spec));

    assert_eq!(frame, 0..3); // Rows 0, 1, 2
}

#[test]
fn test_calculate_frame_preceding() {
    let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5]));

    let frame_spec = WindowFrame {
        unit: FrameUnit::Rows,
        start: FrameBound::Preceding(Box::new(Expression::Literal(SqlValue::Integer(2)))),
        end: Some(FrameBound::CurrentRow),
    };

    let frame = calculate_frame(&partition, 3, &None, &Some(frame_spec));

    // 2 PRECEDING from row 3 is row 1, so rows 1, 2, 3
    assert_eq!(frame, 1..4);
}

#[test]
fn test_calculate_frame_following() {
    let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5]));

    let frame_spec = WindowFrame {
        unit: FrameUnit::Rows,
        start: FrameBound::CurrentRow,
        end: Some(FrameBound::Following(Box::new(Expression::Literal(SqlValue::Integer(2))))),
    };

    let frame = calculate_frame(&partition, 1, &None, &Some(frame_spec));

    // Current row 1 to 2 FOLLOWING (row 3), so rows 1, 2, 3
    assert_eq!(frame, 1..4);
}

#[test]
fn test_calculate_frame_unbounded_following() {
    let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5]));

    let frame_spec = WindowFrame {
        unit: FrameUnit::Rows,
        start: FrameBound::CurrentRow,
        end: Some(FrameBound::UnboundedFollowing),
    };

    let frame = calculate_frame(&partition, 2, &None, &Some(frame_spec));

    // Current row 2 to end: rows 2, 3, 4
    assert_eq!(frame, 2..5);
}

// ===== Ranking Function Tests =====

#[test]
fn test_row_number_simple() {
    let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

    let result = evaluate_row_number(&partition);

    assert_eq!(result.len(), 5);
    assert_eq!(result[0], SqlValue::Integer(1));
    assert_eq!(result[1], SqlValue::Integer(2));
    assert_eq!(result[2], SqlValue::Integer(3));
    assert_eq!(result[3], SqlValue::Integer(4));
    assert_eq!(result[4], SqlValue::Integer(5));
}

#[test]
fn test_rank_with_ties() {
    // Scores: 95, 90, 90, 85
    // Expected ranks: 1, 2, 2, 4
    let partition = Partition::new(make_test_rows(vec![95, 90, 90, 85]));

    let order_by = Some(vec![OrderByItem {
        expr: Expression::ColumnRef {
            table: None,
            column: String::new(), // Will use first column
        },
        direction: OrderDirection::Desc,
    }]);

    let result = evaluate_rank(&partition, &order_by);

    assert_eq!(result.len(), 4);
    assert_eq!(result[0], SqlValue::Integer(1)); // 95 -> rank 1
    assert_eq!(result[1], SqlValue::Integer(2)); // 90 -> rank 2
    assert_eq!(result[2], SqlValue::Integer(2)); // 90 -> rank 2 (tie)
    assert_eq!(result[3], SqlValue::Integer(4)); // 85 -> rank 4 (gap at 3)
}

#[test]
fn test_rank_no_ties() {
    let partition = Partition::new(make_test_rows(vec![4, 3, 2, 1]));

    let order_by = Some(vec![OrderByItem {
        expr: Expression::ColumnRef { table: None, column: String::new() },
        direction: OrderDirection::Desc,
    }]);

    let result = evaluate_rank(&partition, &order_by);

    assert_eq!(result.len(), 4);
    assert_eq!(result[0], SqlValue::Integer(1));
    assert_eq!(result[1], SqlValue::Integer(2));
    assert_eq!(result[2], SqlValue::Integer(3));
    assert_eq!(result[3], SqlValue::Integer(4));
}

#[test]
fn test_rank_without_order_by() {
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

    let result = evaluate_rank(&partition, &None);

    // Without ORDER BY, all rows get rank 1
    assert_eq!(result.len(), 3);
    assert_eq!(result[0], SqlValue::Integer(1));
    assert_eq!(result[1], SqlValue::Integer(1));
    assert_eq!(result[2], SqlValue::Integer(1));
}

#[test]
fn test_dense_rank_with_ties() {
    // Scores: 95, 90, 90, 85
    // Expected ranks: 1, 2, 2, 3 (no gap)
    let partition = Partition::new(make_test_rows(vec![95, 90, 90, 85]));

    let order_by = Some(vec![OrderByItem {
        expr: Expression::ColumnRef { table: None, column: String::new() },
        direction: OrderDirection::Desc,
    }]);

    let result = evaluate_dense_rank(&partition, &order_by);

    assert_eq!(result.len(), 4);
    assert_eq!(result[0], SqlValue::Integer(1)); // 95 -> rank 1
    assert_eq!(result[1], SqlValue::Integer(2)); // 90 -> rank 2
    assert_eq!(result[2], SqlValue::Integer(2)); // 90 -> rank 2 (tie)
    assert_eq!(result[3], SqlValue::Integer(3)); // 85 -> rank 3 (no gap)
}

#[test]
fn test_dense_rank_multiple_tie_groups() {
    // Scores: 100, 90, 90, 80, 80, 80, 70
    // Expected ranks: 1, 2, 2, 3, 3, 3, 4
    let partition = Partition::new(make_test_rows(vec![100, 90, 90, 80, 80, 80, 70]));

    let order_by = Some(vec![OrderByItem {
        expr: Expression::ColumnRef { table: None, column: String::new() },
        direction: OrderDirection::Desc,
    }]);

    let result = evaluate_dense_rank(&partition, &order_by);

    assert_eq!(result.len(), 7);
    assert_eq!(result[0], SqlValue::Integer(1));
    assert_eq!(result[1], SqlValue::Integer(2));
    assert_eq!(result[2], SqlValue::Integer(2));
    assert_eq!(result[3], SqlValue::Integer(3));
    assert_eq!(result[4], SqlValue::Integer(3));
    assert_eq!(result[5], SqlValue::Integer(3));
    assert_eq!(result[6], SqlValue::Integer(4));
}

#[test]
fn test_ntile_even_division() {
    // 8 rows, NTILE(4) -> 2 rows per bucket
    let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5, 6, 7, 8]));

    let result = evaluate_ntile(&partition, 4).unwrap();

    assert_eq!(result.len(), 8);
    assert_eq!(result[0], SqlValue::Integer(1));
    assert_eq!(result[1], SqlValue::Integer(1));
    assert_eq!(result[2], SqlValue::Integer(2));
    assert_eq!(result[3], SqlValue::Integer(2));
    assert_eq!(result[4], SqlValue::Integer(3));
    assert_eq!(result[5], SqlValue::Integer(3));
    assert_eq!(result[6], SqlValue::Integer(4));
    assert_eq!(result[7], SqlValue::Integer(4));
}

#[test]
fn test_ntile_uneven_division() {
    // 10 rows, NTILE(4) -> buckets of 3, 3, 2, 2
    let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));

    let result = evaluate_ntile(&partition, 4).unwrap();

    assert_eq!(result.len(), 10);
    // First bucket: 3 rows (base_size=2 + 1)
    assert_eq!(result[0], SqlValue::Integer(1));
    assert_eq!(result[1], SqlValue::Integer(1));
    assert_eq!(result[2], SqlValue::Integer(1));
    // Second bucket: 3 rows
    assert_eq!(result[3], SqlValue::Integer(2));
    assert_eq!(result[4], SqlValue::Integer(2));
    assert_eq!(result[5], SqlValue::Integer(2));
    // Third bucket: 2 rows (base_size)
    assert_eq!(result[6], SqlValue::Integer(3));
    assert_eq!(result[7], SqlValue::Integer(3));
    // Fourth bucket: 2 rows
    assert_eq!(result[8], SqlValue::Integer(4));
    assert_eq!(result[9], SqlValue::Integer(4));
}

#[test]
fn test_ntile_n_greater_than_rows() {
    // 3 rows, NTILE(5) -> each row gets own bucket
    let partition = Partition::new(make_test_rows(vec![1, 2, 3]));

    let result = evaluate_ntile(&partition, 5).unwrap();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0], SqlValue::Integer(1));
    assert_eq!(result[1], SqlValue::Integer(2));
    assert_eq!(result[2], SqlValue::Integer(3));
}

#[test]
fn test_ntile_single_bucket() {
    let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5]));

    let result = evaluate_ntile(&partition, 1).unwrap();

    assert_eq!(result.len(), 5);
    // All rows in bucket 1
    for val in result {
        assert_eq!(val, SqlValue::Integer(1));
    }
}

#[test]
fn test_ntile_invalid_argument() {
    let partition = Partition::new(make_test_rows(vec![1, 2, 3]));

    let result = evaluate_ntile(&partition, 0);
    assert!(result.is_err());

    let result = evaluate_ntile(&partition, -1);
    assert!(result.is_err());
}

#[test]
fn test_count_star_window() {
    // COUNT(*) over entire partition
    let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));
    let frame = 0..5; // All rows

    let result = evaluate_count_window(&partition, &frame, None, evaluate_expression);

    assert_eq!(result, SqlValue::Integer(5));
}

#[test]
fn test_count_window_with_frame() {
    // COUNT(*) over 3-row moving window
    let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5]));

    // Frame: rows 1, 2, 3 (3 rows)
    let frame = 1..4;
    let result = evaluate_count_window(&partition, &frame, None, evaluate_expression);

    assert_eq!(result, SqlValue::Integer(3));
}

#[test]
fn test_count_expr_window() {
    // COUNT(expr) - counts non-NULL values
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));
    let frame = 0..3;

    let expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    let result = evaluate_count_window(&partition, &frame, Some(&expr), evaluate_expression);

    // All 3 values are non-NULL
    assert_eq!(result, SqlValue::Integer(3));
}

#[test]
fn test_sum_window_running_total() {
    // SUM for running total
    let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

    let expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // Running total at position 2: sum of rows 0, 1, 2
    let frame = 0..3; // 10 + 20 + 30 = 60
    let result = evaluate_sum_window(&partition, &frame, &expr, evaluate_expression);

    assert_eq!(result, SqlValue::Integer(60));
}

#[test]
fn test_sum_window_moving() {
    // SUM over 3-row moving window
    let partition = Partition::new(make_test_rows(vec![5, 10, 15, 20, 25]));

    let expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // Frame: rows 2, 3, 4 (values 15, 20, 25)
    let frame = 2..5;
    let result = evaluate_sum_window(&partition, &frame, &expr, evaluate_expression);

    assert_eq!(result, SqlValue::Integer(60)); // 15 + 20 + 25
}

#[test]
fn test_sum_window_empty_frame() {
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

    let expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // Empty frame
    let frame = 0..0;
    let result = evaluate_sum_window(&partition, &frame, &expr, evaluate_expression);

    assert_eq!(result, SqlValue::Null);
}

#[test]
fn test_avg_window_simple() {
    // AVG over entire partition
    let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

    let expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    let frame = 0..5;
    let result = evaluate_avg_window(&partition, &frame, &expr, evaluate_expression);

    // Average: (10 + 20 + 30 + 40 + 50) / 5 = 30
    assert_eq!(result, SqlValue::Integer(30));
}

#[test]
fn test_avg_window_moving() {
    // 3-row moving average
    let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

    let expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // Frame: rows 1, 2, 3 (values 20, 30, 40)
    let frame = 1..4;
    let result = evaluate_avg_window(&partition, &frame, &expr, evaluate_expression);

    // Average: (20 + 30 + 40) / 3 = 30
    assert_eq!(result, SqlValue::Integer(30));
}

#[test]
fn test_avg_window_empty_frame() {
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

    let expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // Empty frame
    let frame = 0..0;
    let result = evaluate_avg_window(&partition, &frame, &expr, evaluate_expression);

    assert_eq!(result, SqlValue::Null);
}

#[test]
fn test_min_window_partition() {
    // MIN over entire partition
    let partition = Partition::new(make_test_rows(vec![50, 20, 80, 10, 40]));

    let expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    let frame = 0..5;
    let result = evaluate_min_window(&partition, &frame, &expr, evaluate_expression);

    assert_eq!(result, SqlValue::Integer(10));
}

#[test]
fn test_min_window_moving() {
    // MIN in 3-row window
    let partition = Partition::new(make_test_rows(vec![50, 20, 80, 10, 40]));

    let expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // Frame: rows 1, 2, 3 (values 20, 80, 10)
    let frame = 1..4;
    let result = evaluate_min_window(&partition, &frame, &expr, evaluate_expression);

    assert_eq!(result, SqlValue::Integer(10));
}

#[test]
fn test_min_window_empty_frame() {
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

    let expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // Empty frame
    let frame = 0..0;
    let result = evaluate_min_window(&partition, &frame, &expr, evaluate_expression);

    assert_eq!(result, SqlValue::Null);
}

#[test]
fn test_max_window_partition() {
    // MAX over entire partition
    let partition = Partition::new(make_test_rows(vec![50, 20, 80, 10, 40]));

    let expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    let frame = 0..5;
    let result = evaluate_max_window(&partition, &frame, &expr, evaluate_expression);

    assert_eq!(result, SqlValue::Integer(80));
}

#[test]
fn test_max_window_moving() {
    // MAX in 3-row window
    let partition = Partition::new(make_test_rows(vec![50, 20, 80, 10, 40]));

    let expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // Frame: rows 2, 3, 4 (values 80, 10, 40)
    let frame = 2..5;
    let result = evaluate_max_window(&partition, &frame, &expr, evaluate_expression);

    assert_eq!(result, SqlValue::Integer(80));
}

#[test]
fn test_max_window_empty_frame() {
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

    let expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // Empty frame
    let frame = 0..0;
    let result = evaluate_max_window(&partition, &frame, &expr, evaluate_expression);

    assert_eq!(result, SqlValue::Null);
}

#[test]
fn test_frame_at_partition_boundaries() {
    // Test frame behavior at partition start and end
    let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

    let expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // Frame extends beyond partition end
    let frame = 3..10; // Should clamp to 3..5
    let result = evaluate_sum_window(&partition, &frame, &expr, evaluate_expression);

    // Should only sum rows 3, 4 (values 40, 50)
    assert_eq!(result, SqlValue::Integer(90));
}

// ===== LAG/LEAD Value Function Tests =====

#[test]
fn test_lag_default_offset() {
    // LAG(value) with default offset of 1
    let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

    let value_expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // Row 0: LAG should return NULL (no previous row)
    let result = evaluate_lag(&partition, 0, &value_expr, None, None).unwrap();
    assert_eq!(result, SqlValue::Null);

    // Row 1: LAG should return 10 (previous row value)
    let result = evaluate_lag(&partition, 1, &value_expr, None, None).unwrap();
    assert_eq!(result, SqlValue::Integer(10));

    // Row 2: LAG should return 20
    let result = evaluate_lag(&partition, 2, &value_expr, None, None).unwrap();
    assert_eq!(result, SqlValue::Integer(20));

    // Row 4: LAG should return 40
    let result = evaluate_lag(&partition, 4, &value_expr, None, None).unwrap();
    assert_eq!(result, SqlValue::Integer(40));
}

#[test]
fn test_lag_custom_offset() {
    // LAG(value, 2) - look back 2 rows
    let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

    let value_expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // Row 0: offset 2 goes before partition start -> NULL
    let result = evaluate_lag(&partition, 0, &value_expr, Some(2), None).unwrap();
    assert_eq!(result, SqlValue::Null);

    // Row 1: offset 2 goes before partition start -> NULL
    let result = evaluate_lag(&partition, 1, &value_expr, Some(2), None).unwrap();
    assert_eq!(result, SqlValue::Null);

    // Row 2: LAG(value, 2) should return 10 (row 0)
    let result = evaluate_lag(&partition, 2, &value_expr, Some(2), None).unwrap();
    assert_eq!(result, SqlValue::Integer(10));

    // Row 3: LAG(value, 2) should return 20 (row 1)
    let result = evaluate_lag(&partition, 3, &value_expr, Some(2), None).unwrap();
    assert_eq!(result, SqlValue::Integer(20));

    // Row 4: LAG(value, 2) should return 30 (row 2)
    let result = evaluate_lag(&partition, 4, &value_expr, Some(2), None).unwrap();
    assert_eq!(result, SqlValue::Integer(30));
}

#[test]
fn test_lag_with_default_value() {
    // LAG(value, 1, 0) - default value of 0 instead of NULL
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

    let value_expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    let default_expr = Expression::Literal(SqlValue::Integer(0));

    // Row 0: should return 0 (default) instead of NULL
    let result = evaluate_lag(&partition, 0, &value_expr, None, Some(&default_expr)).unwrap();
    assert_eq!(result, SqlValue::Integer(0));

    // Row 1: should return 10 (previous row)
    let result = evaluate_lag(&partition, 1, &value_expr, None, Some(&default_expr)).unwrap();
    assert_eq!(result, SqlValue::Integer(10));

    // Row 2: should return 20 (previous row)
    let result = evaluate_lag(&partition, 2, &value_expr, None, Some(&default_expr)).unwrap();
    assert_eq!(result, SqlValue::Integer(20));
}

#[test]
fn test_lag_offset_beyond_partition_start() {
    // Large offset that goes way before partition start
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

    let value_expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // Row 2 with offset 100 should return NULL
    let result = evaluate_lag(&partition, 2, &value_expr, Some(100), None).unwrap();
    assert_eq!(result, SqlValue::Null);

    // With default value
    let default_expr = Expression::Literal(SqlValue::Integer(-1));
    let result = evaluate_lag(&partition, 2, &value_expr, Some(100), Some(&default_expr)).unwrap();
    assert_eq!(result, SqlValue::Integer(-1));
}

#[test]
fn test_lead_default_offset() {
    // LEAD(value) with default offset of 1
    let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

    let value_expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // Row 0: LEAD should return 20 (next row value)
    let result = evaluate_lead(&partition, 0, &value_expr, None, None).unwrap();
    assert_eq!(result, SqlValue::Integer(20));

    // Row 1: LEAD should return 30
    let result = evaluate_lead(&partition, 1, &value_expr, None, None).unwrap();
    assert_eq!(result, SqlValue::Integer(30));

    // Row 3: LEAD should return 50
    let result = evaluate_lead(&partition, 3, &value_expr, None, None).unwrap();
    assert_eq!(result, SqlValue::Integer(50));

    // Row 4: LEAD should return NULL (no next row)
    let result = evaluate_lead(&partition, 4, &value_expr, None, None).unwrap();
    assert_eq!(result, SqlValue::Null);
}

#[test]
fn test_lead_custom_offset() {
    // LEAD(value, 2) - look forward 2 rows
    let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

    let value_expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // Row 0: LEAD(value, 2) should return 30 (row 2)
    let result = evaluate_lead(&partition, 0, &value_expr, Some(2), None).unwrap();
    assert_eq!(result, SqlValue::Integer(30));

    // Row 1: LEAD(value, 2) should return 40 (row 3)
    let result = evaluate_lead(&partition, 1, &value_expr, Some(2), None).unwrap();
    assert_eq!(result, SqlValue::Integer(40));

    // Row 2: LEAD(value, 2) should return 50 (row 4)
    let result = evaluate_lead(&partition, 2, &value_expr, Some(2), None).unwrap();
    assert_eq!(result, SqlValue::Integer(50));

    // Row 3: offset 2 goes past partition end -> NULL
    let result = evaluate_lead(&partition, 3, &value_expr, Some(2), None).unwrap();
    assert_eq!(result, SqlValue::Null);

    // Row 4: offset 2 goes past partition end -> NULL
    let result = evaluate_lead(&partition, 4, &value_expr, Some(2), None).unwrap();
    assert_eq!(result, SqlValue::Null);
}

#[test]
fn test_lead_with_default_value() {
    // LEAD(value, 1, 999) - default value of 999 instead of NULL
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

    let value_expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    let default_expr = Expression::Literal(SqlValue::Integer(999));

    // Row 0: should return 20 (next row)
    let result = evaluate_lead(&partition, 0, &value_expr, None, Some(&default_expr)).unwrap();
    assert_eq!(result, SqlValue::Integer(20));

    // Row 1: should return 30 (next row)
    let result = evaluate_lead(&partition, 1, &value_expr, None, Some(&default_expr)).unwrap();
    assert_eq!(result, SqlValue::Integer(30));

    // Row 2: should return 999 (default) instead of NULL
    let result = evaluate_lead(&partition, 2, &value_expr, None, Some(&default_expr)).unwrap();
    assert_eq!(result, SqlValue::Integer(999));
}

#[test]
fn test_lead_offset_beyond_partition_end() {
    // Large offset that goes way past partition end
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

    let value_expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // Row 0 with offset 100 should return NULL
    let result = evaluate_lead(&partition, 0, &value_expr, Some(100), None).unwrap();
    assert_eq!(result, SqlValue::Null);

    // With default value
    let default_expr = Expression::Literal(SqlValue::Integer(-1));
    let result = evaluate_lead(&partition, 0, &value_expr, Some(100), Some(&default_expr)).unwrap();
    assert_eq!(result, SqlValue::Integer(-1));
}

#[test]
fn test_lag_lead_single_row_partition() {
    // Edge case: partition with only one row
    let partition = Partition::new(make_test_rows(vec![42]));

    let value_expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // LAG on single row should return NULL
    let result = evaluate_lag(&partition, 0, &value_expr, None, None).unwrap();
    assert_eq!(result, SqlValue::Null);

    // LEAD on single row should return NULL
    let result = evaluate_lead(&partition, 0, &value_expr, None, None).unwrap();
    assert_eq!(result, SqlValue::Null);
}

#[test]
fn test_lag_lead_with_zero_offset() {
    // Special case: offset of 0 should return current row value
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

    let value_expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // LAG(value, 0) should return current row value
    let result = evaluate_lag(&partition, 1, &value_expr, Some(0), None).unwrap();
    assert_eq!(result, SqlValue::Integer(20));

    // LEAD(value, 0) should return current row value
    let result = evaluate_lead(&partition, 1, &value_expr, Some(0), None).unwrap();
    assert_eq!(result, SqlValue::Integer(20));
}

#[test]
fn test_lag_negative_offset_error() {
    // LAG with negative offset should return error
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

    let value_expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    let result = evaluate_lag(&partition, 1, &value_expr, Some(-1), None);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("non-negative"));
}

#[test]
fn test_lead_negative_offset_error() {
    // LEAD with negative offset should return error
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

    let value_expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    let result = evaluate_lead(&partition, 1, &value_expr, Some(-1), None);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("non-negative"));
}
