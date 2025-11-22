use std::collections::HashMap;

#[cfg(feature = "parallel")]
use rayon::prelude::*;

use super::{combine_rows, FromResult};
use crate::{errors::ExecutorError, schema::CombinedSchema};

#[cfg(feature = "parallel")]
use crate::select::parallel::ParallelConfig;

/// Build hash table sequentially using indices (fallback for small inputs)
///
/// Returns a map from join key to row indices, avoiding storing row references
/// which enables deferred materialization.
fn build_hash_table_sequential(
    build_rows: &[vibesql_storage::Row],
    build_col_idx: usize,
) -> HashMap<vibesql_types::SqlValue, Vec<usize>> {
    let mut hash_table: HashMap<vibesql_types::SqlValue, Vec<usize>> = HashMap::new();
    for (idx, row) in build_rows.iter().enumerate() {
        let key = row.values[build_col_idx].clone();
        // Skip NULL values - they never match in equi-joins
        if key != vibesql_types::SqlValue::Null {
            hash_table.entry(key).or_default().push(idx);
        }
    }
    hash_table
}

/// Build hash table in parallel using partitioned approach (index-based)
///
/// Algorithm (when parallel feature enabled):
/// 1. Divide build_rows into chunks (one per thread)
/// 2. Each thread builds a local hash table from its chunk (no synchronization)
/// 3. Merge partial hash tables sequentially (fast because only touching shared keys)
///
/// Performance: 3-6x speedup on large joins (50k+ rows) with 4+ cores
/// Note: Falls back to sequential when parallel feature is disabled
fn build_hash_table_parallel(
    build_rows: &[vibesql_storage::Row],
    build_col_idx: usize,
) -> HashMap<vibesql_types::SqlValue, Vec<usize>> {
    #[cfg(feature = "parallel")]
    {
        let config = ParallelConfig::global();

        // Use sequential fallback for small inputs
        if !config.should_parallelize_join(build_rows.len()) {
            return build_hash_table_sequential(build_rows, build_col_idx);
        }

        // Phase 1: Parallel build of partial hash tables with indices
        // Each thread processes a chunk and builds its own hash table
        let chunk_size = (build_rows.len() / config.num_threads).max(1000);
        let partial_tables: Vec<(usize, HashMap<_, _>)> = build_rows
            .par_chunks(chunk_size)
            .enumerate()
            .map(|(chunk_idx, chunk)| {
                let base_idx = chunk_idx * chunk_size;
                let mut local_table: HashMap<vibesql_types::SqlValue, Vec<usize>> = HashMap::new();
                for (i, row) in chunk.iter().enumerate() {
                    let key = row.values[build_col_idx].clone();
                    if key != vibesql_types::SqlValue::Null {
                        local_table.entry(key).or_default().push(base_idx + i);
                    }
                }
                (chunk_idx, local_table)
            })
            .collect();

        // Phase 2: Sequential merge of partial tables
        // This is fast because we only touch keys that appear in multiple partitions
        partial_tables.into_iter()
            .fold(HashMap::new(), |mut acc, (_chunk_idx, partial)| {
                for (key, mut indices) in partial {
                    acc.entry(key).or_default().append(&mut indices);
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

// Note: Memory limit checking removed from hash join.
// Hash join uses O(smaller_table) memory for the hash table, not O(result_size).
// The actual join output size depends on data distribution and selectivity,
// which we cannot accurately predict. Since hash join is already the optimal
// algorithm for equijoins, we trust it to handle the join efficiently.

/// Hash join SEMI JOIN implementation (optimized for EXISTS subqueries)
///
/// Semi-join returns rows from the left table where matching rows exist in the right table.
/// This is perfect for optimizing EXISTS subqueries.
///
/// Algorithm:
/// 1. Build phase: Create a HashSet of join keys from right table (O(n))
/// 2. Probe phase: For each left row, check if key exists in set (O(m))
/// Total: O(n + m) with minimal memory overhead
///
/// Key optimizations vs INNER JOIN:
/// - Only stores keys (not full rows) in hash set
/// - Returns left rows as-is (no row combination)
/// - Early termination per key (first match is enough)
/// - Much lower memory usage
///
/// Performance characteristics:
/// - Time: O(n + m) vs O(n*m) for nested loop
/// - Space: O(distinct keys) instead of O(n rows)
/// - Expected speedup: 100-10,000x for large EXISTS queries
pub(super) fn hash_join_semi(
    mut left: FromResult,
    mut right: FromResult,
    left_col_idx: usize,
    right_col_idx: usize,
) -> Result<FromResult, ExecutorError> {
    use std::collections::HashSet;

    // Build phase: Create HashSet of right table join keys
    // We only need to know if a key EXISTS, not store the rows
    let mut key_set: HashSet<vibesql_types::SqlValue> = HashSet::new();
    for row in right.rows() {
        let key = row.values[right_col_idx].clone();
        // Skip NULL values - they never match in equi-joins
        if key != vibesql_types::SqlValue::Null {
            key_set.insert(key);
        }
    }

    // Probe phase: Filter left rows to only those with matching keys
    let mut result_rows = Vec::new();
    for row in left.rows() {
        let key = &row.values[left_col_idx];

        // Skip NULL values
        if key == &vibesql_types::SqlValue::Null {
            continue;
        }

        // If key exists in right table, include this left row
        if key_set.contains(key) {
            result_rows.push(row.clone());
        }
    }

    // Return only left schema and rows (no combination with right)
    Ok(FromResult::from_rows(left.schema, result_rows))
}

/// Hash join INNER JOIN implementation (optimized for equi-joins)
///
/// This implementation uses a hash join algorithm for better performance
/// on equi-join conditions (e.g., t1.id = t2.id).
///
/// Algorithm:
/// 1. Build phase: Hash the smaller table into a HashMap (O(n))
/// 2. Probe phase: For each row in larger table, lookup matches (O(m)) Total: O(n + m) instead of
///    O(n * m) for nested loop join
///
/// Performance characteristics:
/// - Time: O(n + m) vs O(n*m) for nested loop
/// - Space: O(n) where n is the size of the smaller table
/// - Expected speedup: 100-10,000x for large equi-joins
pub(super) fn hash_join_inner(
    mut left: FromResult,
    mut right: FromResult,
    left_col_idx: usize,
    right_col_idx: usize,
) -> Result<FromResult, ExecutorError> {
    // Note: No memory limit check here. Hash join is already O(n+m) time and O(smaller_table) space,
    // which is optimal for equijoins. We cannot predict output size accurately anyway.

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
    let combined_schema =
        CombinedSchema::combine(left.schema.clone(), right_table_name, right_schema);

    // Choose build and probe sides (build hash table on smaller table)
    let (build_rows, probe_rows, build_col_idx, probe_col_idx, left_is_build) =
        if left.rows().len() <= right.rows().len() {
            (left.rows(), right.rows(), left_col_idx, right_col_idx, true)
        } else {
            (right.rows(), left.rows(), right_col_idx, left_col_idx, false)
        };

    // Build phase: Create hash table from build side (using parallel algorithm)
    // Key: join column value
    // Value: vector of row indices (not row references) for deferred materialization
    // Automatically uses parallel build when beneficial (based on row count and hardware)
    let hash_table = build_hash_table_parallel(build_rows, build_col_idx);

    // Probe phase: Collect (build_idx, probe_idx) pairs without materializing rows
    // This defers the expensive row cloning until after we know all matches
    let estimated_capacity = probe_rows.len().saturating_mul(2).min(100_000);
    let mut join_pairs: Vec<(usize, usize)> = Vec::with_capacity(estimated_capacity);

    for (probe_idx, probe_row) in probe_rows.iter().enumerate() {
        let key = &probe_row.values[probe_col_idx];

        // Skip NULL values - they never match in equi-joins
        if key == &vibesql_types::SqlValue::Null {
            continue;
        }

        if let Some(build_indices) = hash_table.get(key) {
            for &build_idx in build_indices {
                join_pairs.push((build_idx, probe_idx));
            }
        }
    }

    // Materialization phase: Create combined rows from index pairs
    // Pre-allocate result vector with exact size now that we know it
    let mut result_rows = Vec::with_capacity(join_pairs.len());
    for (build_idx, probe_idx) in join_pairs {
        // Combine rows in correct order (left first, then right)
        let combined_row = if left_is_build {
            combine_rows(&build_rows[build_idx], &probe_rows[probe_idx])
        } else {
            combine_rows(&probe_rows[probe_idx], &build_rows[build_idx])
        };
        result_rows.push(combined_row);
    }

    Ok(FromResult::from_rows(combined_schema, result_rows))
}

/// Hash join SEMI JOIN implementation (optimized for equi-joins)
///
/// Semi-join returns left rows where at least one matching right row exists.
/// Result contains only left columns. Each left row appears at most once.
///
/// Algorithm:
/// 1. Build phase: Hash the right table into a HashMap (O(m))
/// 2. Probe phase: For each left row, check if key exists in hash table (O(n))
/// Total: O(n + m) instead of O(n * m) for nested loop semi-join
///
/// Performance characteristics:
/// - Time: O(n + m) vs O(n*m) for nested loop
/// - Space: O(m) for right table hash set
/// - Expected speedup: 100-10,000x for large semi-joins
pub(super) fn hash_join_semi(
    mut left: FromResult,
    mut right: FromResult,
    left_col_idx: usize,
    right_col_idx: usize,
) -> Result<FromResult, ExecutorError> {
    // Schema is just the left side (semi-join doesn't include right columns)
    let result_schema = left.schema.clone();

    // Build hash table from right side (we only care if key exists)
    let hash_table = build_hash_table_parallel(right.rows(), right_col_idx);

    // Probe phase: Keep left rows that have a match in right
    let mut result_rows = Vec::new();
    for left_row in left.rows() {
        let key = &left_row.values[left_col_idx];

        // Skip NULL values - they never match in equi-joins
        if key == &vibesql_types::SqlValue::Null {
            continue;
        }

        // If key exists in right table, include this left row
        if hash_table.contains_key(key) {
            result_rows.push(left_row.clone());
        }
    }

    Ok(FromResult::from_rows(result_schema, result_rows))
}

/// Hash join ANTI JOIN implementation (optimized for equi-joins)
///
/// Anti-join returns left rows where NO matching right row exists.
/// Result contains only left columns. This is the inverse of semi-join.
///
/// Algorithm:
/// 1. Build phase: Hash the right table into a HashMap (O(m))
/// 2. Probe phase: For each left row, check if key does NOT exist in hash table (O(n))
/// Total: O(n + m) instead of O(n * m) for nested loop anti-join
///
/// Performance characteristics:
/// - Time: O(n + m) vs O(n*m) for nested loop
/// - Space: O(m) for right table hash set
/// - Expected speedup: 100-10,000x for large anti-joins
pub(super) fn hash_join_anti(
    mut left: FromResult,
    mut right: FromResult,
    left_col_idx: usize,
    right_col_idx: usize,
) -> Result<FromResult, ExecutorError> {
    // Schema is just the left side (anti-join doesn't include right columns)
    let result_schema = left.schema.clone();

    // Build hash table from right side (we only care if key exists)
    let hash_table = build_hash_table_parallel(right.rows(), right_col_idx);

    // Probe phase: Keep left rows that have NO match in right
    let mut result_rows = Vec::new();
    for left_row in left.rows() {
        let key = &left_row.values[left_col_idx];

        // NULL handling for anti-join:
        // In SQL, NULL != NULL, so NULL keys on left should be kept
        // unless we're doing NOT IN (which has special NULL semantics)
        // For now, treat NULLs same as semi-join (skip them)
        if key == &vibesql_types::SqlValue::Null {
            continue;
        }

        // If key does NOT exist in right table, include this left row
        if !hash_table.contains_key(key) {
            result_rows.push(left_row.clone());
        }
    }

    Ok(FromResult::from_rows(result_schema, result_rows))
}

/// Hash join LEFT OUTER JOIN implementation (optimized for equi-joins)
///
/// This implementation uses a hash join algorithm for better performance
/// on equi-join conditions with LEFT OUTER JOIN semantics.
///
/// Algorithm:
/// 1. Build phase: Hash the right table into a HashMap (O(m))
/// 2. Probe phase: For each left row, lookup matches (O(n))
///    - If matches found: emit left + right rows
///    - If no match: emit left + NULLs (preserves left rows)
///
/// Total: O(n + m) instead of O(n * m) for nested loop join
///
/// Performance: Critical for Q13 where customer LEFT JOIN orders
/// with 150k customers and 1.5M orders.
pub(super) fn hash_join_left_outer(
    mut left: FromResult,
    mut right: FromResult,
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

    let right_col_count = right_schema.columns.len();

    // Combine schemas
    let combined_schema =
        CombinedSchema::combine(left.schema.clone(), right_table_name, right_schema);

    // Build hash table on the RIGHT side (we need to preserve ALL left rows)
    // For LEFT OUTER JOIN, we always probe with left, so build on right
    let right_rows = right.rows();
    let hash_table = build_hash_table_parallel(right_rows, right_col_idx);

    // Probe with LEFT side, preserving unmatched left rows
    let mut result_rows = Vec::new();
    let left_rows = left.rows();

    for left_row in left_rows {
        let key = &left_row.values[left_col_idx];

        // For NULL keys in left, still emit the row with NULL right side
        if key == &vibesql_types::SqlValue::Null {
            // Left row with NULLs for right columns
            let null_right = create_null_row(right_col_count);
            result_rows.push(combine_rows(left_row, &null_right));
            continue;
        }

        if let Some(right_indices) = hash_table.get(key) {
            // Found matches - emit all combinations
            for &right_idx in right_indices {
                result_rows.push(combine_rows(left_row, &right_rows[right_idx]));
            }
        } else {
            // No match - emit left row with NULLs for right columns
            let null_right = create_null_row(right_col_count);
            result_rows.push(combine_rows(left_row, &null_right));
        }
    }

    Ok(FromResult::from_rows(combined_schema, result_rows))
}

/// Create a row with all NULL values
fn create_null_row(col_count: usize) -> vibesql_storage::Row {
    vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Null; col_count])
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
        let mut result = hash_join_inner(left, right, 0, 0).unwrap();

        // Should have 3 rows (user 1 has 2 orders, user 2 has 1 order, user 3 has no orders)
        assert_eq!(result.rows().len(), 3);

        // Verify combined rows have correct structure (4 columns: id, name, user_id, amount)
        for row in result.rows() {
            assert_eq!(row.values.len(), 4);
        }

        // Check specific matches
        // Alice (id=1) should appear twice (2 orders)
        let alice_orders: Vec<_> =
            result.rows().iter().filter(|r| r.values[0] == SqlValue::Integer(1)).collect();
        assert_eq!(alice_orders.len(), 2);

        // Bob (id=2) should appear once (1 order)
        let bob_orders: Vec<_> =
            result.rows().iter().filter(|r| r.values[0] == SqlValue::Integer(2)).collect();
        assert_eq!(bob_orders.len(), 1);

        // Charlie (id=3) should not appear (no orders)
        let charlie_orders: Vec<_> =
            result.rows().iter().filter(|r| r.values[0] == SqlValue::Integer(3)).collect();
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

        let mut result = hash_join_inner(left, right, 0, 0).unwrap();

        // Only one match: Alice (id=1) with order (user_id=1)
        // NULLs should not match each other in equi-joins
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0].values[0], SqlValue::Integer(1)); // user id
        assert_eq!(result.rows()[0].values[1], SqlValue::Varchar("Alice".to_string())); // user name
        assert_eq!(result.rows()[0].values[2], SqlValue::Integer(1)); // order user_id
        assert_eq!(result.rows()[0].values[3], SqlValue::Integer(100)); // order amount
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

        let mut result = hash_join_inner(left, right, 0, 0).unwrap();

        // No matches
        assert_eq!(result.rows().len(), 0);
    }

    #[test]
    fn test_hash_join_empty_tables() {
        // Left table (empty)
        let left = create_test_from_result("users", vec![("id", DataType::Integer)], vec![]);

        // Right table (empty)
        let right = create_test_from_result("orders", vec![("user_id", DataType::Integer)], vec![]);

        let mut result = hash_join_inner(left, right, 0, 0).unwrap();

        // No rows
        assert_eq!(result.rows().len(), 0);
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

        let mut result = hash_join_inner(left, right, 0, 0).unwrap();

        // Cartesian product of matching keys: 2 left rows * 2 right rows = 4 results
        assert_eq!(result.rows().len(), 4);

        // All should have id=1
        for row in result.rows() {
            assert_eq!(row.values[0], SqlValue::Integer(1));
        }
    }

    // Tests for parallel hash table building

    fn create_test_rows(count: usize) -> Vec<vibesql_storage::Row> {
        (0..count)
            .map(|i| vibesql_storage::Row {
                values: vec![
                    SqlValue::Integer(i as i64 % 100), // Keys with duplicates
                    SqlValue::Varchar(format!("value{}", i)),
                ],
            })
            .collect()
    }

    #[test]
    fn test_build_hash_table_sequential_basic() {
        let build_rows = create_test_rows(100);
        let hash_table = build_hash_table_sequential(&build_rows, 0);

        // Should have 100 unique keys (0-99)
        assert_eq!(hash_table.len(), 100);

        // Each key should have 1 row index
        for (key, row_indices) in hash_table.iter() {
            assert_eq!(row_indices.len(), 1);
            assert_eq!(build_rows[row_indices[0]].values[0], *key);
        }
    }

    #[test]
    fn test_build_hash_table_sequential_with_duplicates() {
        let rows = create_test_rows(1000); // 1000 rows with keys 0-99 (10 duplicates each)
        let hash_table = build_hash_table_sequential(&rows, 0);

        // Should have 100 unique keys
        assert_eq!(hash_table.len(), 100);

        // Each key should have 10 rows
        for (_, rows) in hash_table.iter() {
            assert_eq!(rows.len(), 10);
        }
    }

    #[test]
    fn test_build_hash_table_sequential_null_values() {
        let rows = vec![
            vibesql_storage::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar("one".to_string())],
            },
            vibesql_storage::Row {
                values: vec![SqlValue::Null, SqlValue::Varchar("null1".to_string())],
            },
            vibesql_storage::Row {
                values: vec![SqlValue::Integer(2), SqlValue::Varchar("two".to_string())],
            },
            vibesql_storage::Row {
                values: vec![SqlValue::Null, SqlValue::Varchar("null2".to_string())],
            },
        ];

        let hash_table = build_hash_table_sequential(&rows, 0);

        // Should only have 2 keys (NULLs are skipped)
        assert_eq!(hash_table.len(), 2);
        assert!(hash_table.contains_key(&SqlValue::Integer(1)));
        assert!(hash_table.contains_key(&SqlValue::Integer(2)));
        assert!(!hash_table.contains_key(&SqlValue::Null));
    }

    #[test]
    fn test_parallel_sequential_equivalence_small() {
        // Small dataset - should use sequential path in parallel version
        let rows = create_test_rows(100);

        let seq_table = build_hash_table_sequential(&rows, 0);
        let par_table = build_hash_table_parallel(&rows, 0);

        // Should produce identical results
        assert_eq!(seq_table.len(), par_table.len());

        for (key, seq_rows) in seq_table.iter() {
            let par_rows = par_table.get(key).expect("Key should exist in parallel table");
            assert_eq!(seq_rows.len(), par_rows.len());
        }
    }

    #[test]
    fn test_parallel_sequential_equivalence_large() {
        // Large dataset - should use parallel path
        let build_rows = create_test_rows(10000); // Well above threshold (5000)

        let seq_table = build_hash_table_sequential(&build_rows, 0);
        let par_table = build_hash_table_parallel(&build_rows, 0);

        // Should produce identical results
        assert_eq!(seq_table.len(), par_table.len());

        for (key, seq_indices) in seq_table.iter() {
            let par_indices = par_table.get(key).expect("Key should exist in parallel table");
            assert_eq!(seq_indices.len(), par_indices.len(), "Row count mismatch for key {:?}", key);

            // Verify all row indices are present (order may differ)
            for &seq_idx in seq_indices {
                assert!(
                    par_indices.iter().any(|&par_idx| build_rows[par_idx].values == build_rows[seq_idx].values),
                    "Row not found in parallel table"
                );
            }
        }
    }

    #[test]
    fn test_parallel_with_null_values() {
        let rows = vec![
            vibesql_storage::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar("one".to_string())],
            },
            vibesql_storage::Row {
                values: vec![SqlValue::Null, SqlValue::Varchar("null1".to_string())],
            },
            vibesql_storage::Row {
                values: vec![SqlValue::Integer(2), SqlValue::Varchar("two".to_string())],
            },
            vibesql_storage::Row {
                values: vec![SqlValue::Null, SqlValue::Varchar("null2".to_string())],
            },
            vibesql_storage::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar("one_dup".to_string())],
            },
        ];

        let par_table = build_hash_table_parallel(&rows, 0);

        // Should only have 2 keys (NULLs are skipped)
        assert_eq!(par_table.len(), 2);
        assert!(par_table.contains_key(&SqlValue::Integer(1)));
        assert!(par_table.contains_key(&SqlValue::Integer(2)));
        assert!(!par_table.contains_key(&SqlValue::Null));

        // Key 1 should have 2 rows
        assert_eq!(par_table.get(&SqlValue::Integer(1)).unwrap().len(), 2);
    }

    #[test]
    fn test_parallel_hash_join_integration() {
        // Integration test: Create large tables and verify parallel join works correctly

        // Left table: 6000 rows (above join threshold of 5000)
        let left_rows: Vec<Vec<SqlValue>> = (0..6000)
            .map(|i| vec![SqlValue::Integer(i % 100), SqlValue::Varchar(format!("left{}", i))])
            .collect();

        let left = create_test_from_result(
            "large_left",
            vec![("id", DataType::Integer), ("data", DataType::Varchar { max_length: Some(50) })],
            left_rows,
        );

        // Right table: 6000 rows
        let right_rows: Vec<Vec<SqlValue>> = (0..6000)
            .map(|i| vec![SqlValue::Integer(i % 100), SqlValue::Varchar(format!("right{}", i))])
            .collect();

        let right = create_test_from_result(
            "large_right",
            vec![("id", DataType::Integer), ("data", DataType::Varchar { max_length: Some(50) })],
            right_rows,
        );

        let mut result = hash_join_inner(left, right, 0, 0).unwrap();

        // Each key (0-99) appears 60 times on left and 60 times on right
        // So we expect 100 keys * 60 * 60 = 360,000 result rows
        assert_eq!(result.rows().len(), 360_000);

        // Verify combined row structure
        for row in result.rows() {
            assert_eq!(row.values.len(), 4); // 2 columns from left + 2 from right
        }
    }
}
