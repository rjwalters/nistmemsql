use std::collections::HashSet;

use super::FromResult;
use crate::errors::ExecutorError;

/// Hash semi-join implementation
///
/// Semi-join returns rows from the left table that have at least one match in the right table.
/// Unlike INNER JOIN, each left row appears at most once in the result, regardless of how many
/// right rows match.
///
/// This is the optimal implementation for correlated EXISTS subqueries.
///
/// Algorithm:
/// 1. Build phase: Create a HashSet of join keys from right table (O(m))
/// 2. Probe phase: For each left row, check if its key exists in the set (O(n))
/// Total: O(n + m) vs O(n*m) for nested loop EXISTS execution
///
/// Performance characteristics:
/// - Time: O(n + m) where n = left rows, m = right rows
/// - Space: O(distinct_keys_in_right) = O(m) worst case
/// - Expected speedup: 100-10,000x for large tables with correlated EXISTS
pub(super) fn hash_semi_join(
    mut left: FromResult,
    mut right: FromResult,
    left_col_idx: usize,
    right_col_idx: usize,
) -> Result<FromResult, ExecutorError> {
    // Build phase: Create hash set from right side (smaller table in typical EXISTS case)
    // We only store the join keys, not the full rows
    let right_rows = right.rows();
    let mut right_keys: HashSet<vibesql_types::SqlValue> = HashSet::with_capacity(right_rows.len());

    for row in right_rows.iter() {
        let key = &row.values[right_col_idx];
        // Skip NULL values - they never match in semi-joins (consistent with SQL semantics)
        if key != &vibesql_types::SqlValue::Null {
            right_keys.insert(key.clone());
        }
    }

    // Probe phase: Filter left rows based on hash set membership
    let left_rows = left.rows();
    let mut result_rows = Vec::new();

    for row in left_rows.iter() {
        let key = &row.values[left_col_idx];
        // Skip NULL keys (NULLs never match in equi-joins)
        if key == &vibesql_types::SqlValue::Null {
            continue;
        }

        // If left key exists in right keys, include this left row
        if right_keys.contains(key) {
            result_rows.push(row.clone());
        }
    }

    // Semi-join returns only left table columns (not combined)
    Ok(FromResult::from_rows(left.schema.clone(), result_rows))
}

#[cfg(test)]
mod tests {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_storage::Row;
    use vibesql_types::{DataType, SqlValue};

    use super::*;
    use crate::schema::CombinedSchema;

    /// Helper to create a simple FromResult for testing
    fn create_test_from_result(
        table_name: &str,
        columns: Vec<(&str, DataType)>,
        rows: Vec<Vec<SqlValue>>,
    ) -> FromResult {
        let schema = TableSchema::new(
            table_name.to_string(),
            columns
                .iter()
                .map(|(name, dtype)| {
                    ColumnSchema::new(
                        name.to_string(),
                        dtype.clone(),
                        true, // nullable
                    )
                })
                .collect(),
        );

        let combined_schema = CombinedSchema::from_table(table_name.to_string(), schema);

        let rows = rows.into_iter().map(|values| Row::new(values)).collect();

        FromResult::from_rows(combined_schema, rows)
    }

    #[test]
    fn test_hash_semi_join_basic() {
        // Left table: orders(o_orderkey, o_custkey)
        let left = create_test_from_result(
            "orders",
            vec![("o_orderkey", DataType::Integer), ("o_custkey", DataType::Integer)],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Integer(100)],
                vec![SqlValue::Integer(2), SqlValue::Integer(200)],
                vec![SqlValue::Integer(3), SqlValue::Integer(300)],
                vec![SqlValue::Integer(4), SqlValue::Integer(400)],
            ],
        );

        // Right table: lineitem(l_orderkey) - only orders 1, 2, and 2 again have lineitems
        let right = create_test_from_result(
            "lineitem",
            vec![("l_orderkey", DataType::Integer)],
            vec![
                vec![SqlValue::Integer(1)],
                vec![SqlValue::Integer(2)],
                vec![SqlValue::Integer(2)], // Duplicate - should not cause duplicate in result
            ],
        );

        // Semi-join on o_orderkey = l_orderkey
        let mut result = hash_semi_join(left, right, 0, 0).unwrap();

        // Should have 2 rows (orders 1 and 2), not 3 (no duplicate for order 2)
        assert_eq!(result.rows().len(), 2);

        // Result should only have left table columns (2 columns)
        for row in result.rows() {
            assert_eq!(row.values.len(), 2);
        }

        // Check specific rows
        assert!(result.rows().iter().any(|r| r.values[0] == SqlValue::Integer(1)));
        assert!(result.rows().iter().any(|r| r.values[0] == SqlValue::Integer(2)));
        assert!(!result.rows().iter().any(|r| r.values[0] == SqlValue::Integer(3)));
        assert!(!result.rows().iter().any(|r| r.values[0] == SqlValue::Integer(4)));
    }

    #[test]
    fn test_hash_semi_join_no_matches() {
        // Left table
        let left = create_test_from_result(
            "orders",
            vec![("o_orderkey", DataType::Integer)],
            vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]],
        );

        // Right table with non-matching keys
        let right = create_test_from_result(
            "lineitem",
            vec![("l_orderkey", DataType::Integer)],
            vec![vec![SqlValue::Integer(3)], vec![SqlValue::Integer(4)]],
        );

        let mut result = hash_semi_join(left, right, 0, 0).unwrap();

        // No matches
        assert_eq!(result.rows().len(), 0);
    }

    #[test]
    fn test_hash_semi_join_all_match() {
        // Left table
        let left = create_test_from_result(
            "orders",
            vec![("o_orderkey", DataType::Integer)],
            vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]],
        );

        // Right table where all left keys exist
        let right = create_test_from_result(
            "lineitem",
            vec![("l_orderkey", DataType::Integer)],
            vec![
                vec![SqlValue::Integer(1)],
                vec![SqlValue::Integer(2)],
                vec![SqlValue::Integer(1)], // Duplicate
                vec![SqlValue::Integer(3)], // Extra key not in left
            ],
        );

        let mut result = hash_semi_join(left, right, 0, 0).unwrap();

        // All left rows should match (2 rows)
        assert_eq!(result.rows().len(), 2);
    }

    #[test]
    fn test_hash_semi_join_null_handling() {
        // Left table with NULL key
        let left = create_test_from_result(
            "orders",
            vec![("o_orderkey", DataType::Integer)],
            vec![
                vec![SqlValue::Integer(1)],
                vec![SqlValue::Null], // NULL should not match
                vec![SqlValue::Integer(2)],
            ],
        );

        // Right table with NULL key and matching keys
        let right = create_test_from_result(
            "lineitem",
            vec![("l_orderkey", DataType::Integer)],
            vec![
                vec![SqlValue::Integer(1)],
                vec![SqlValue::Null], // NULL should not be in hash set
            ],
        );

        let mut result = hash_semi_join(left, right, 0, 0).unwrap();

        // Only order 1 should match (NULL keys don't match)
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0].values[0], SqlValue::Integer(1));
    }

    #[test]
    fn test_hash_semi_join_empty_tables() {
        // Empty left table
        let left = create_test_from_result("orders", vec![("o_orderkey", DataType::Integer)], vec![]);

        // Empty right table
        let right = create_test_from_result("lineitem", vec![("l_orderkey", DataType::Integer)], vec![]);

        let mut result = hash_semi_join(left, right, 0, 0).unwrap();

        // No rows
        assert_eq!(result.rows().len(), 0);
    }

    #[test]
    fn test_hash_semi_join_preserves_left_schema() {
        // Left table with multiple columns
        let left = create_test_from_result(
            "orders",
            vec![
                ("o_orderkey", DataType::Integer),
                ("o_custkey", DataType::Integer),
                ("o_orderpriority", DataType::Varchar { max_length: Some(15) }),
            ],
            vec![vec![
                SqlValue::Integer(1),
                SqlValue::Integer(100),
                SqlValue::Varchar("1-URGENT".to_string()),
            ]],
        );

        // Right table with single column
        let right = create_test_from_result(
            "lineitem",
            vec![("l_orderkey", DataType::Integer)],
            vec![vec![SqlValue::Integer(1)]],
        );

        let mut result = hash_semi_join(left, right, 0, 0).unwrap();

        // Schema should match left table (3 columns)
        assert_eq!(result.schema.total_columns, 3);
        assert_eq!(result.rows()[0].values.len(), 3);
    }

    #[test]
    fn test_hash_semi_join_tpch_q4_pattern() {
        // Simulate TPC-H Q4 pattern:
        // Orders table (already filtered by date)
        let orders = create_test_from_result(
            "orders",
            vec![
                ("o_orderkey", DataType::Integer),
                ("o_orderpriority", DataType::Varchar { max_length: Some(15) }),
            ],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Varchar("1-URGENT".to_string())],
                vec![SqlValue::Integer(2), SqlValue::Varchar("2-HIGH".to_string())],
                vec![SqlValue::Integer(3), SqlValue::Varchar("3-MEDIUM".to_string())],
                vec![SqlValue::Integer(4), SqlValue::Varchar("4-NOT SPECIFIED".to_string())],
            ],
        );

        // Lineitem table (already filtered by l_commitdate < l_receiptdate)
        // Only orders 1, 2, and 4 have qualifying lineitems
        let lineitem = create_test_from_result(
            "lineitem",
            vec![("l_orderkey", DataType::Integer)],
            vec![
                vec![SqlValue::Integer(1)],
                vec![SqlValue::Integer(2)],
                vec![SqlValue::Integer(2)], // Order 2 has multiple lineitems
                vec![SqlValue::Integer(4)],
            ],
        );

        // Semi-join: keep only orders that have lineitems
        let mut result = hash_semi_join(orders, lineitem, 0, 0).unwrap();

        // Should have 3 orders (1, 2, and 4)
        assert_eq!(result.rows().len(), 3);

        // Verify specific orders
        let order_keys: Vec<i64> = result
            .rows()
            .iter()
            .map(|r| match &r.values[0] {
                SqlValue::Integer(k) => *k,
                _ => panic!("Expected integer"),
            })
            .collect();

        assert!(order_keys.contains(&1));
        assert!(order_keys.contains(&2));
        assert!(!order_keys.contains(&3)); // Order 3 has no lineitems
        assert!(order_keys.contains(&4));

        // Each order appears only once (no duplicates from multiple lineitems)
        assert_eq!(order_keys.iter().filter(|&&k| k == 2).count(), 1);
    }
}
