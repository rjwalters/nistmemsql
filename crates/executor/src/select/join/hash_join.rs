use crate::errors::ExecutorError;
use crate::schema::CombinedSchema;
use std::collections::HashMap;

use super::{combine_rows, FromResult};

/// Hash join INNER JOIN implementation (optimized for equi-joins)
///
/// This implementation uses a hash join algorithm for better performance
/// on equi-join conditions (e.g., t1.id = t2.id).
///
/// Algorithm:
/// 1. Build phase: Hash the smaller table into a HashMap (O(n))
/// 2. Probe phase: For each row in larger table, lookup matches (O(m))
/// Total: O(n + m) instead of O(n * m) for nested loop join
///
/// Performance characteristics:
/// - Time: O(n + m) vs O(n*m) for nested loop
/// - Space: O(n) where n is the size of the smaller table
/// - Expected speedup: 100-10,000x for large equi-joins
pub(super) fn hash_join_inner(
    left: FromResult,
    right: FromResult,
    left_col_idx: usize,
    right_col_idx: usize,
) -> Result<FromResult, ExecutorError> {
    // Extract right table name and schema for combining
    let right_table_name = right
        .schema
        .table_schemas
        .keys()
        .next()
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .clone();

    let right_schema = right
        .schema
        .table_schemas
        .get(&right_table_name)
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .1
        .clone();

    // Combine schemas
    let combined_schema = CombinedSchema::combine(left.schema.clone(), right_table_name, right_schema);

    // Choose build and probe sides (build hash table on smaller table)
    let (build_rows, probe_rows, build_col_idx, probe_col_idx, left_is_build) =
        if left.rows.len() <= right.rows.len() {
            (&left.rows, &right.rows, left_col_idx, right_col_idx, true)
        } else {
            (&right.rows, &left.rows, right_col_idx, left_col_idx, false)
        };

    // Build phase: Create hash table from build side
    // Key: join column value
    // Value: vector of rows with that key (handles duplicates)
    let mut hash_table: HashMap<types::SqlValue, Vec<&storage::Row>> = HashMap::new();
    for row in build_rows {
        let key = row.values[build_col_idx].clone();
        // Skip NULL values - they never match in equi-joins
        if key != types::SqlValue::Null {
            hash_table.entry(key).or_default().push(row);
        }
    }

    // Probe phase: Look up matches for each probe row
    let mut result_rows = Vec::new();
    for probe_row in probe_rows {
        let key = &probe_row.values[probe_col_idx];

        // Skip NULL values - they never match in equi-joins
        if key == &types::SqlValue::Null {
            continue;
        }

        if let Some(build_matches) = hash_table.get(key) {
            for build_row in build_matches {
                // Combine rows in correct order (left first, then right)
                let combined_row = if left_is_build {
                    // build_row is from left, probe_row is from right
                    combine_rows(build_row, probe_row)
                } else {
                    // probe_row is from left, build_row is from right
                    combine_rows(probe_row, build_row)
                };

                result_rows.push(combined_row);
            }
        }
    }

    Ok(FromResult { schema: combined_schema, rows: result_rows })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::CombinedSchema;
    use catalog::{ColumnSchema, TableSchema};
    use storage::Row;
    use types::{DataType, SqlValue};

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
                .map(|(name, dtype)| ColumnSchema::new(
                    name.to_string(),
                    dtype.clone(),
                    true, // nullable
                ))
                .collect(),
        );

        let combined_schema = CombinedSchema::from_table(table_name.to_string(), schema);

        let rows = rows.into_iter().map(|values| Row::new(values)).collect();

        FromResult { schema: combined_schema, rows }
    }

    #[test]
    fn test_hash_join_simple() {
        // Left table: users(id, name)
        let left = create_test_from_result(
            "users",
            vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())],
                vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())],
                vec![SqlValue::Integer(3), SqlValue::Varchar("Charlie".to_string())],
            ],
        );

        // Right table: orders(user_id, amount)
        let right = create_test_from_result(
            "orders",
            vec![("user_id", DataType::Integer), ("amount", DataType::Integer)],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Integer(100)],
                vec![SqlValue::Integer(2), SqlValue::Integer(200)],
                vec![SqlValue::Integer(1), SqlValue::Integer(150)],
            ],
        );

        // Join on users.id = orders.user_id (column 0 from both sides)
        let result = hash_join_inner(left, right, 0, 0).unwrap();

        // Should have 3 rows (user 1 has 2 orders, user 2 has 1 order, user 3 has no orders)
        assert_eq!(result.rows.len(), 3);

        // Verify combined rows have correct structure (4 columns: id, name, user_id, amount)
        for row in &result.rows {
            assert_eq!(row.values.len(), 4);
        }

        // Check specific matches
        // Alice (id=1) should appear twice (2 orders)
        let alice_orders: Vec<_> = result
            .rows
            .iter()
            .filter(|r| r.values[0] == SqlValue::Integer(1))
            .collect();
        assert_eq!(alice_orders.len(), 2);

        // Bob (id=2) should appear once (1 order)
        let bob_orders: Vec<_> = result
            .rows
            .iter()
            .filter(|r| r.values[0] == SqlValue::Integer(2))
            .collect();
        assert_eq!(bob_orders.len(), 1);

        // Charlie (id=3) should not appear (no orders)
        let charlie_orders: Vec<_> = result
            .rows
            .iter()
            .filter(|r| r.values[0] == SqlValue::Integer(3))
            .collect();
        assert_eq!(charlie_orders.len(), 0);
    }

    #[test]
    fn test_hash_join_null_values() {
        // Left table with NULL id
        let left = create_test_from_result(
            "users",
            vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())],
                vec![SqlValue::Null, SqlValue::Varchar("Unknown".to_string())],
            ],
        );

        // Right table with NULL user_id
        let right = create_test_from_result(
            "orders",
            vec![("user_id", DataType::Integer), ("amount", DataType::Integer)],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Integer(100)],
                vec![SqlValue::Null, SqlValue::Integer(200)],
            ],
        );

        let result = hash_join_inner(left, right, 0, 0).unwrap();

        // Only one match: Alice (id=1) with order (user_id=1)
        // NULLs should not match each other in equi-joins
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], SqlValue::Integer(1)); // user id
        assert_eq!(
            result.rows[0].values[1],
            SqlValue::Varchar("Alice".to_string())
        ); // user name
        assert_eq!(result.rows[0].values[2], SqlValue::Integer(1)); // order user_id
        assert_eq!(result.rows[0].values[3], SqlValue::Integer(100)); // order amount
    }

    #[test]
    fn test_hash_join_no_matches() {
        // Left table
        let left = create_test_from_result(
            "users",
            vec![("id", DataType::Integer)],
            vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]],
        );

        // Right table with non-matching ids
        let right = create_test_from_result(
            "orders",
            vec![("user_id", DataType::Integer)],
            vec![vec![SqlValue::Integer(3)], vec![SqlValue::Integer(4)]],
        );

        let result = hash_join_inner(left, right, 0, 0).unwrap();

        // No matches
        assert_eq!(result.rows.len(), 0);
    }

    #[test]
    fn test_hash_join_empty_tables() {
        // Left table (empty)
        let left =
            create_test_from_result("users", vec![("id", DataType::Integer)], vec![]);

        // Right table (empty)
        let right =
            create_test_from_result("orders", vec![("user_id", DataType::Integer)], vec![]);

        let result = hash_join_inner(left, right, 0, 0).unwrap();

        // No rows
        assert_eq!(result.rows.len(), 0);
    }

    #[test]
    fn test_hash_join_duplicate_keys() {
        // Left table with duplicate ids
        let left = create_test_from_result(
            "users",
            vec![("id", DataType::Integer), ("type", DataType::Varchar { max_length: Some(10) })],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Varchar("admin".to_string())],
                vec![SqlValue::Integer(1), SqlValue::Varchar("user".to_string())],
            ],
        );

        // Right table with duplicate user_ids
        let right = create_test_from_result(
            "orders",
            vec![("user_id", DataType::Integer), ("amount", DataType::Integer)],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Integer(100)],
                vec![SqlValue::Integer(1), SqlValue::Integer(200)],
            ],
        );

        let result = hash_join_inner(left, right, 0, 0).unwrap();

        // Cartesian product of matching keys: 2 left rows * 2 right rows = 4 results
        assert_eq!(result.rows.len(), 4);

        // All should have id=1
        for row in &result.rows {
            assert_eq!(row.values[0], SqlValue::Integer(1));
        }
    }
}
