use std::collections::HashMap;

#[cfg(feature = "parallel")]
use rayon::prelude::*;

use super::{FromResult};
use crate::errors::ExecutorError;

#[cfg(feature = "parallel")]
use crate::select::parallel::ParallelConfig;

/// Build hash table from right side for anti-join
///
/// For anti-join, we only need to know if a key exists in the right table.
/// Returns a HashMap where the key is the join column value.
fn build_anti_join_hash_table_sequential(
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

/// Build hash table in parallel for anti-join
fn build_anti_join_hash_table_parallel(
    build_rows: &[vibesql_storage::Row],
    build_col_idx: usize,
) -> HashMap<vibesql_types::SqlValue, ()> {
    #[cfg(feature = "parallel")]
    {
        let config = ParallelConfig::global();

        // Use sequential fallback for small inputs
        if !config.should_parallelize_join(build_rows.len()) {
            return build_anti_join_hash_table_sequential(build_rows, build_col_idx);
        }

        // Phase 1: Parallel build of partial hash tables
        let chunk_size = (build_rows.len() / config.num_threads).max(1000);
        let partial_tables: Vec<HashMap<_, _>> = build_rows
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
        partial_tables.into_iter()
            .fold(HashMap::new(), |mut acc, partial| {
                for (key, val) in partial {
                    acc.insert(key, val);
                }
                acc
            })
    }

    #[cfg(not(feature = "parallel"))]
    {
        build_anti_join_hash_table_sequential(build_rows, build_col_idx)
    }
}

/// Hash anti-join implementation
///
/// Anti-join returns rows from the LEFT table that have NO match in the RIGHT table.
/// This is the opposite of semi-join.
///
/// Algorithm:
/// 1. Build phase: Create hash set from right table keys (O(n))
/// 2. Probe phase: For each left row, check if key does NOT exist in hash set (O(m))
/// 3. Emit left row only if no match found
///
/// Performance:
/// - Time: O(n + m) where n = right size, m = left size
/// - Space: O(unique keys in right table)
/// - Much faster than nested loop for large tables
///
/// Use cases:
/// - SQL NOT EXISTS subqueries
/// - SQL NOT IN subqueries
/// - Filtering left table to exclude rows with right table membership
pub(super) fn hash_anti_join(
    mut left: FromResult,
    mut right: FromResult,
    left_col_idx: usize,
    right_col_idx: usize,
) -> Result<FromResult, ExecutorError> {
    // Build hash table from right side (just tracking keys that exist)
    let right_rows = right.rows();
    let hash_table = build_anti_join_hash_table_parallel(right_rows, right_col_idx);

    // Probe with left side and collect non-matching rows
    let left_rows = left.rows();
    let mut result_rows = Vec::new();

    for left_row in left_rows.iter() {
        let key = &left_row.values[left_col_idx];

        // For anti-join, NULL values on the left side should be included in results
        // because NULLs never match anything in equi-joins
        let should_include = if key == &vibesql_types::SqlValue::Null {
            // NULL keys never match, so include in anti-join result
            true
        } else {
            // Non-NULL: include if key does NOT exist in right table
            !hash_table.contains_key(key)
        };

        if should_include {
            result_rows.push(left_row.clone());
        }
    }

    // Return result with left schema only (anti-join doesn't include right columns)
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
    fn test_anti_join_basic() {
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
                vec![SqlValue::Integer(1), SqlValue::Integer(150)], // Alice has 2 orders
            ],
        );

        // Anti-join on users.id = orders.user_id
        let mut result = hash_anti_join(left, right, 0, 0).unwrap();

        // Should have 1 row: Charlie (Alice and Bob have orders)
        assert_eq!(result.rows().len(), 1);

        // Result should only have left table columns (id, name)
        for row in result.rows() {
            assert_eq!(row.values.len(), 2);
        }

        // Verify we have only Charlie
        assert_eq!(result.rows()[0].values[0], SqlValue::Integer(3));
        assert_eq!(result.rows()[0].values[1], SqlValue::Varchar("Charlie".to_string()));
    }

    #[test]
    fn test_anti_join_null_values() {
        // Left table with NULL id
        let left = create_test_from_result(
            "users",
            vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())],
                vec![SqlValue::Null, SqlValue::Varchar("Unknown".to_string())],
                vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())],
            ],
        );

        // Right table with matching id=1
        let right = create_test_from_result(
            "orders",
            vec![("user_id", DataType::Integer)],
            vec![
                vec![SqlValue::Integer(1)],
            ],
        );

        let mut result = hash_anti_join(left, right, 0, 0).unwrap();

        // Should have 2 rows: Unknown (NULL id) and Bob (id=2)
        // NULL never matches anything, so it's included in anti-join
        // Alice (id=1) matches and is excluded
        assert_eq!(result.rows().len(), 2);

        // Verify we have Unknown and Bob
        let names: Vec<String> = result.rows()
            .iter()
            .map(|r| match &r.values[1] {
                SqlValue::Varchar(s) => s.clone(),
                _ => panic!("Expected varchar"),
            })
            .collect();

        assert!(names.contains(&"Unknown".to_string()));
        assert!(names.contains(&"Bob".to_string()));
        assert!(!names.contains(&"Alice".to_string()));
    }

    #[test]
    fn test_anti_join_all_match() {
        // Left table
        let left = create_test_from_result(
            "users",
            vec![("id", DataType::Integer)],
            vec![
                vec![SqlValue::Integer(1)],
                vec![SqlValue::Integer(2)],
            ],
        );

        // Right table with all matching ids
        let right = create_test_from_result(
            "orders",
            vec![("user_id", DataType::Integer)],
            vec![
                vec![SqlValue::Integer(1)],
                vec![SqlValue::Integer(2)],
            ],
        );

        let mut result = hash_anti_join(left, right, 0, 0).unwrap();

        // No rows because all left rows have matches
        assert_eq!(result.rows().len(), 0);
    }

    #[test]
    fn test_anti_join_no_matches() {
        // Left table
        let left = create_test_from_result(
            "users",
            vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())],
                vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())],
            ],
        );

        // Right table with non-matching ids
        let right = create_test_from_result(
            "orders",
            vec![("user_id", DataType::Integer)],
            vec![
                vec![SqlValue::Integer(3)],
                vec![SqlValue::Integer(4)],
            ],
        );

        let mut result = hash_anti_join(left, right, 0, 0).unwrap();

        // All left rows should be in result (no matches)
        assert_eq!(result.rows().len(), 2);
    }

    #[test]
    fn test_anti_join_empty_right() {
        // Left table
        let left = create_test_from_result(
            "users",
            vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())],
                vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())],
            ],
        );

        // Right table (empty)
        let right = create_test_from_result(
            "orders",
            vec![("user_id", DataType::Integer)],
            vec![],
        );

        let mut result = hash_anti_join(left, right, 0, 0).unwrap();

        // All left rows should be in result (no matches because right is empty)
        assert_eq!(result.rows().len(), 2);
    }

    #[test]
    fn test_anti_join_empty_left() {
        // Left table (empty)
        let left = create_test_from_result(
            "users",
            vec![("id", DataType::Integer)],
            vec![],
        );

        // Right table
        let right = create_test_from_result(
            "orders",
            vec![("user_id", DataType::Integer)],
            vec![
                vec![SqlValue::Integer(1)],
                vec![SqlValue::Integer(2)],
            ],
        );

        let mut result = hash_anti_join(left, right, 0, 0).unwrap();

        // No rows because left is empty
        assert_eq!(result.rows().len(), 0);
    }

    #[test]
    fn test_anti_join_partial_match() {
        // Left table with 5 users
        let left = create_test_from_result(
            "users",
            vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())],
                vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())],
                vec![SqlValue::Integer(3), SqlValue::Varchar("Charlie".to_string())],
                vec![SqlValue::Integer(4), SqlValue::Varchar("David".to_string())],
                vec![SqlValue::Integer(5), SqlValue::Varchar("Eve".to_string())],
            ],
        );

        // Right table: users 1 and 3 have orders
        let right = create_test_from_result(
            "orders",
            vec![("user_id", DataType::Integer)],
            vec![
                vec![SqlValue::Integer(1)],
                vec![SqlValue::Integer(3)],
            ],
        );

        let mut result = hash_anti_join(left, right, 0, 0).unwrap();

        // Should have 3 rows: Bob, David, Eve (Alice and Charlie have orders)
        assert_eq!(result.rows().len(), 3);

        let ids: Vec<i64> = result.rows()
            .iter()
            .map(|r| match &r.values[0] {
                SqlValue::Integer(i) => *i,
                _ => panic!("Expected integer"),
            })
            .collect();

        assert!(!ids.contains(&1)); // Alice - excluded
        assert!(ids.contains(&2)); // Bob - included
        assert!(!ids.contains(&3)); // Charlie - excluded
        assert!(ids.contains(&4)); // David - included
        assert!(ids.contains(&5)); // Eve - included
    }

    #[test]
    fn test_anti_join_multiple_right_duplicates() {
        // Left table
        let left = create_test_from_result(
            "users",
            vec![("id", DataType::Integer)],
            vec![
                vec![SqlValue::Integer(1)],
                vec![SqlValue::Integer(2)],
            ],
        );

        // Right table with duplicate entries for id=1
        let right = create_test_from_result(
            "orders",
            vec![("user_id", DataType::Integer)],
            vec![
                vec![SqlValue::Integer(1)],
                vec![SqlValue::Integer(1)],
                vec![SqlValue::Integer(1)],
            ],
        );

        let mut result = hash_anti_join(left, right, 0, 0).unwrap();

        // Should have 1 row: id=2 (id=1 has matches, even though duplicated)
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0].values[0], SqlValue::Integer(2));
    }

    #[test]
    fn test_build_anti_join_hash_table_sequential() {
        let rows = vec![
            Row { values: vec![SqlValue::Integer(1), SqlValue::Varchar("one".to_string())] },
            Row { values: vec![SqlValue::Integer(2), SqlValue::Varchar("two".to_string())] },
            Row { values: vec![SqlValue::Integer(1), SqlValue::Varchar("one_dup".to_string())] }, // Duplicate key
            Row { values: vec![SqlValue::Null, SqlValue::Varchar("null".to_string())] }, // NULL
        ];

        let hash_table = build_anti_join_hash_table_sequential(&rows, 0);

        // Should have 2 unique keys (1 and 2), NULLs excluded
        assert_eq!(hash_table.len(), 2);
        assert!(hash_table.contains_key(&SqlValue::Integer(1)));
        assert!(hash_table.contains_key(&SqlValue::Integer(2)));
        assert!(!hash_table.contains_key(&SqlValue::Null));
    }

    #[test]
    fn test_build_anti_join_hash_table_parallel() {
        let rows = vec![
            Row { values: vec![SqlValue::Integer(1), SqlValue::Varchar("one".to_string())] },
            Row { values: vec![SqlValue::Integer(2), SqlValue::Varchar("two".to_string())] },
            Row { values: vec![SqlValue::Integer(1), SqlValue::Varchar("one_dup".to_string())] },
            Row { values: vec![SqlValue::Null, SqlValue::Varchar("null".to_string())] },
        ];

        let hash_table = build_anti_join_hash_table_parallel(&rows, 0);

        // Should have same result as sequential
        assert_eq!(hash_table.len(), 2);
        assert!(hash_table.contains_key(&SqlValue::Integer(1)));
        assert!(hash_table.contains_key(&SqlValue::Integer(2)));
        assert!(!hash_table.contains_key(&SqlValue::Null));
    }
}
