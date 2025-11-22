use std::collections::HashMap;

#[cfg(feature = "parallel")]
use rayon::prelude::*;

use super::FromResult;
use crate::errors::ExecutorError;

#[cfg(feature = "parallel")]
use crate::select::parallel::ParallelConfig;

/// Build hash table sequentially for semi-join (stores only keys, not indices)
///
/// For semi-join, we only need to know if a key exists, not track all matching rows.
/// This saves memory compared to inner join's Vec<usize> storage.
fn build_hash_table_sequential(
    build_rows: &[vibesql_storage::Row],
    build_col_idx: usize,
) -> HashMap<vibesql_types::SqlValue, ()> {
    let mut hash_table: HashMap<vibesql_types::SqlValue, ()> = HashMap::new();
    for row in build_rows.iter() {
        let key = row.values[build_col_idx].clone();
        // Skip NULL values - they never match in equi-joins
        if key != vibesql_types::SqlValue::Null {
            hash_table.insert(key, ());
        }
    }
    hash_table
}

/// Build hash table in parallel for semi-join
///
/// Algorithm (when parallel feature enabled):
/// 1. Divide build_rows into chunks (one per thread)
/// 2. Each thread builds a local hash table from its chunk (no synchronization)
/// 3. Merge partial hash tables sequentially (fast because we only store keys)
///
/// Performance: 3-6x speedup on large joins (50k+ rows) with 4+ cores
/// Note: Falls back to sequential when parallel feature is disabled
fn build_hash_table_parallel(
    build_rows: &[vibesql_storage::Row],
    build_col_idx: usize,
) -> HashMap<vibesql_types::SqlValue, ()> {
    #[cfg(feature = "parallel")]
    {
        let config = ParallelConfig::global();

        // Use sequential fallback for small inputs
        if !config.should_parallelize_join(build_rows.len()) {
            return build_hash_table_sequential(build_rows, build_col_idx);
        }

        // Phase 1: Parallel build of partial hash tables
        // Each thread processes a chunk and builds its own hash table
        let chunk_size = (build_rows.len() / config.num_threads).max(1000);
        let partial_tables: Vec<HashMap<_, ()>> = build_rows
            .par_chunks(chunk_size)
            .map(|chunk| {
                let mut local_table: HashMap<vibesql_types::SqlValue, ()> = HashMap::new();
                for row in chunk.iter() {
                    let key = row.values[build_col_idx].clone();
                    if key != vibesql_types::SqlValue::Null {
                        local_table.insert(key, ());
                    }
                }
                local_table
            })
            .collect();

        // Phase 2: Sequential merge of partial tables
        // This is fast because we only need to insert keys, not append vectors
        partial_tables.into_iter().fold(HashMap::new(), |mut acc, partial| {
            for (key, _) in partial {
                acc.insert(key, ());
            }
            acc
        })
    }

    #[cfg(not(feature = "parallel"))]
    {
        // Always use sequential build when parallel feature is disabled
        build_hash_table_sequential(build_rows, build_col_idx)
    }
}

/// Hash semi-join implementation
///
/// Semi-join returns rows from the LEFT table that have a match in the RIGHT table.
/// Unlike inner join, each left row is returned at most ONCE, regardless of how many
/// right rows match.
///
/// Use cases:
/// - EXISTS subqueries: SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM lineitem WHERE l_orderkey = o_orderkey)
/// - IN subqueries: SELECT * FROM orders WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem)
///
/// Algorithm:
/// 1. Build phase: Hash the RIGHT table into a HashSet (O(n))
/// 2. Probe phase: For each row in LEFT table, check if key exists in hash set (O(m))
/// 3. If key exists, emit the LEFT row (only once)
/// Total: O(n + m) vs O(n*m) for nested loop
///
/// Performance characteristics:
/// - Time: O(n + m) vs O(n*m) for nested loop
/// - Space: O(n) where n is the size of the right table (smaller than inner join because we don't store indices)
/// - Expected speedup: 100-10,000x for large semi-joins
pub(super) fn hash_semi_join(
    mut left: FromResult,
    mut right: FromResult,
    left_col_idx: usize,
    right_col_idx: usize,
) -> Result<FromResult, ExecutorError> {
    // Get left and right row data
    let left_rows = left.rows();
    let right_rows = right.rows();

    // Build phase: Create hash table from right side (using parallel algorithm)
    // Key: join column value
    // Value: () (we only need to know if the key exists, not store row indices)
    // Automatically uses parallel build when beneficial (based on row count and hardware)
    let hash_table = build_hash_table_parallel(right_rows, right_col_idx);

    // Probe phase: Check each left row for a match
    // We only emit left rows that have a match in the right table
    let estimated_capacity = left_rows.len().min(100_000);
    let mut result_rows = Vec::with_capacity(estimated_capacity);

    for left_row in left_rows.iter() {
        let key = &left_row.values[left_col_idx];

        // Skip NULL values - they never match in equi-joins
        if key == &vibesql_types::SqlValue::Null {
            continue;
        }

        // If key exists in hash table, emit this left row (only once)
        if hash_table.contains_key(key) {
            result_rows.push(left_row.clone());
        }
    }

    // Return result with left schema only (we don't combine with right schema)
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
        // Left table: users(id, name)
        let left = create_test_from_result(
            "users",
            vec![
                ("id", DataType::Integer),
                ("name", DataType::Varchar { max_length: Some(50) }),
            ],
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

        // Semi-join on users.id = orders.user_id (column 0 from both sides)
        let mut result = hash_semi_join(left, right, 0, 0).unwrap();

        // Should have 2 rows (users 1 and 2 have orders, user 3 does not)
        // Note: user 1 appears only ONCE despite having 2 orders
        assert_eq!(result.rows().len(), 2);

        // Verify result rows only have left table columns (2 columns: id, name)
        for row in result.rows() {
            assert_eq!(row.values.len(), 2);
        }

        // Check that we have users 1 and 2
        let user_ids: Vec<i64> = result
            .rows()
            .iter()
            .map(|r| match &r.values[0] {
                SqlValue::Integer(id) => *id,
                _ => panic!("Expected integer"),
            })
            .collect();
        assert!(user_ids.contains(&1));
        assert!(user_ids.contains(&2));
        assert!(!user_ids.contains(&3)); // Charlie has no orders
    }

    #[test]
    fn test_hash_semi_join_null_values() {
        // Left table with NULL id
        let left = create_test_from_result(
            "users",
            vec![
                ("id", DataType::Integer),
                ("name", DataType::Varchar { max_length: Some(50) }),
            ],
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

        let mut result = hash_semi_join(left, right, 0, 0).unwrap();

        // Only Alice should match (id=1)
        // NULL values should not match each other in equi-joins
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0].values[0], SqlValue::Integer(1));
        assert_eq!(result.rows()[0].values[1], SqlValue::Varchar("Alice".to_string()));
    }

    #[test]
    fn test_hash_semi_join_no_matches() {
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

        let mut result = hash_semi_join(left, right, 0, 0).unwrap();

        // No matches
        assert_eq!(result.rows().len(), 0);
    }

    #[test]
    fn test_hash_semi_join_duplicate_right_keys() {
        // Left table
        let left = create_test_from_result(
            "users",
            vec![
                ("id", DataType::Integer),
                ("name", DataType::Varchar { max_length: Some(50) }),
            ],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())],
                vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())],
            ],
        );

        // Right table with many duplicate user_ids
        let right = create_test_from_result(
            "orders",
            vec![("user_id", DataType::Integer), ("amount", DataType::Integer)],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Integer(100)],
                vec![SqlValue::Integer(1), SqlValue::Integer(150)],
                vec![SqlValue::Integer(1), SqlValue::Integer(200)],
                vec![SqlValue::Integer(1), SqlValue::Integer(250)],
            ],
        );

        let mut result = hash_semi_join(left, right, 0, 0).unwrap();

        // Should return only 1 row for Alice, despite 4 matching orders
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0].values[0], SqlValue::Integer(1));
        assert_eq!(result.rows()[0].values[1], SqlValue::Varchar("Alice".to_string()));
    }
}
