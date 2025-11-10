use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::*;

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
