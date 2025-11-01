use crate::errors::ExecutorError;
use crate::evaluator::CombinedExpressionEvaluator;
use crate::schema::CombinedSchema;
use std::collections::HashMap;

mod join_analyzer;

/// Result of executing a FROM clause
#[derive(Clone)]
pub(super) struct FromResult {
    pub(super) schema: CombinedSchema,
    pub(super) rows: Vec<storage::Row>,
}

/// Perform join between two FROM results, optimizing with hash join when possible
pub(super) fn nested_loop_join(
    left: FromResult,
    right: FromResult,
    join_type: &ast::JoinType,
    condition: &Option<ast::Expression>,
    database: &storage::Database,
) -> Result<FromResult, ExecutorError> {
    // Try to use hash join for INNER JOINs with simple equi-join conditions
    if let ast::JoinType::Inner = join_type {
        if let Some(cond) = condition {
            let left_col_count = left
                .schema
                .table_schemas
                .values()
                .next()
                .map(|(_, schema)| schema.columns.len())
                .unwrap_or(0);

            // Create a temporary combined schema to analyze the join condition
            let right_table_name = right
                .schema
                .table_schemas
                .keys()
                .next()
                .ok_or_else(|| {
                    ExecutorError::UnsupportedFeature("Complex JOIN".to_string())
                })?
                .clone();

            let right_schema = right
                .schema
                .table_schemas
                .get(&right_table_name)
                .ok_or_else(|| {
                    ExecutorError::UnsupportedFeature("Complex JOIN".to_string())
                })?
                .1
                .clone();

            let temp_schema =
                CombinedSchema::combine(left.schema.clone(), right_table_name, right_schema);

            // Check if this is a simple equi-join that can use hash join
            if let Some(equi_join_info) =
                join_analyzer::analyze_equi_join(cond, &temp_schema, left_col_count)
            {
                return hash_join_inner(
                    left,
                    right,
                    equi_join_info.left_col_idx,
                    equi_join_info.right_col_idx,
                );
            }
        }
    }

    // Fall back to nested loop join for all other cases
    match join_type {
        ast::JoinType::Inner => nested_loop_inner_join(left, right, condition, database),
        ast::JoinType::LeftOuter => {
            nested_loop_left_outer_join(left, right, condition, database)
        }
        ast::JoinType::RightOuter => {
            nested_loop_right_outer_join(left, right, condition, database)
        }
        ast::JoinType::FullOuter => {
            nested_loop_full_outer_join(left, right, condition, database)
        }
        ast::JoinType::Cross => nested_loop_cross_join(left, right, condition, database),
    }
}

/// Nested loop INNER JOIN implementation
pub(super) fn nested_loop_inner_join(
    left: FromResult,
    right: FromResult,
    condition: &Option<ast::Expression>,
    database: &storage::Database,
) -> Result<FromResult, ExecutorError> {
    // Extract right table name (assume single table for now)
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
    let combined_schema = CombinedSchema::combine(left.schema, right_table_name, right_schema);
    let evaluator = CombinedExpressionEvaluator::with_database(&combined_schema, database);

    // Nested loop join algorithm
    let mut result_rows = Vec::new();
    for left_row in &left.rows {
        for right_row in &right.rows {
            // Concatenate rows
            let mut combined_values = left_row.values.clone();
            combined_values.extend(right_row.values.clone());
            let combined_row = storage::Row::new(combined_values);

            // Evaluate join condition
            let matches = if let Some(cond) = condition {
                match evaluator.eval(cond, &combined_row)? {
                    types::SqlValue::Boolean(true) => true,
                    types::SqlValue::Boolean(false) => false,
                    types::SqlValue::Null => false,
                    other => {
                        return Err(ExecutorError::InvalidWhereClause(format!(
                            "JOIN condition must evaluate to boolean, got: {:?}",
                            other
                        )))
                    }
                }
            } else {
                true // No condition = CROSS JOIN
            };

            if matches {
                result_rows.push(combined_row);
            }
        }
    }

    Ok(FromResult { schema: combined_schema, rows: result_rows })
}

/// Nested loop LEFT OUTER JOIN implementation
pub(super) fn nested_loop_left_outer_join(
    left: FromResult,
    right: FromResult,
    condition: &Option<ast::Expression>,
    database: &storage::Database,
) -> Result<FromResult, ExecutorError> {
    // Extract right table name and schema
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

    let right_column_count = right_schema.columns.len();

    // Combine schemas
    let combined_schema = CombinedSchema::combine(left.schema, right_table_name, right_schema);
    let evaluator = CombinedExpressionEvaluator::with_database(&combined_schema, database);

    // Nested loop LEFT OUTER JOIN algorithm
    let mut result_rows = Vec::new();
    for left_row in &left.rows {
        let mut matched = false;

        for right_row in &right.rows {
            // Concatenate rows
            let mut combined_values = left_row.values.clone();
            combined_values.extend(right_row.values.clone());
            let combined_row = storage::Row::new(combined_values);

            // Evaluate join condition
            let matches = if let Some(cond) = condition {
                match evaluator.eval(cond, &combined_row)? {
                    types::SqlValue::Boolean(true) => true,
                    types::SqlValue::Boolean(false) => false,
                    types::SqlValue::Null => false,
                    other => {
                        return Err(ExecutorError::InvalidWhereClause(format!(
                            "JOIN condition must evaluate to boolean, got: {:?}",
                            other
                        )))
                    }
                }
            } else {
                true // No condition = CROSS JOIN
            };

            if matches {
                result_rows.push(combined_row);
                matched = true;
            }
        }

        // If no match found, add left row with NULLs for right columns
        if !matched {
            let mut combined_values = left_row.values.clone();
            // Add NULL values for all right table columns
            combined_values.extend(vec![types::SqlValue::Null; right_column_count]);
            result_rows.push(storage::Row::new(combined_values));
        }
    }

    Ok(FromResult { schema: combined_schema, rows: result_rows })
}

/// Nested loop RIGHT OUTER JOIN implementation
pub(super) fn nested_loop_right_outer_join(
    left: FromResult,
    right: FromResult,
    condition: &Option<ast::Expression>,
    database: &storage::Database,
) -> Result<FromResult, ExecutorError> {
    // RIGHT OUTER JOIN = LEFT OUTER JOIN with sides swapped
    // Then we need to reorder columns to put left first, right second

    // Get the right column count before moving
    let right_col_count = right
        .schema
        .table_schemas
        .values()
        .next()
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .1
        .columns
        .len();

    // Do LEFT OUTER JOIN with swapped sides
    let swapped_result = nested_loop_left_outer_join(right, left, condition, database)?;

    // Now we need to reorder the columns in the result
    // The swapped result has right columns first, then left columns
    // We need to reverse this to left first, then right

    // Reorder rows: move left columns (currently at positions right_col_count..) to front
    let reordered_rows: Vec<storage::Row> = swapped_result
        .rows
        .iter()
        .map(|row| {
            let mut new_values = Vec::new();
            // Add left columns (currently at end)
            new_values.extend_from_slice(&row.values[right_col_count..]);
            // Add right columns (currently at start)
            new_values.extend_from_slice(&row.values[0..right_col_count]);
            storage::Row::new(new_values)
        })
        .collect();

    Ok(FromResult { schema: swapped_result.schema, rows: reordered_rows })
}

/// Nested loop FULL OUTER JOIN implementation
pub(super) fn nested_loop_full_outer_join(
    left: FromResult,
    right: FromResult,
    condition: &Option<ast::Expression>,
    database: &storage::Database,
) -> Result<FromResult, ExecutorError> {
    // Extract right table name and schema
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

    let left_column_count = left
        .schema
        .table_schemas
        .values()
        .next()
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .1
        .columns
        .len();
    let right_column_count = right_schema.columns.len();

    // Combine schemas
    let combined_schema = CombinedSchema::combine(left.schema, right_table_name, right_schema);
    let evaluator = CombinedExpressionEvaluator::with_database(&combined_schema, database);

    // FULL OUTER JOIN = LEFT OUTER JOIN + unmatched rows from right
    let mut result_rows = Vec::new();
    let mut right_matched = vec![false; right.rows.len()];

    // First pass: LEFT OUTER JOIN logic
    for left_row in &left.rows {
        let mut matched = false;

        for (right_idx, right_row) in right.rows.iter().enumerate() {
            // Concatenate rows
            let mut combined_values = left_row.values.clone();
            combined_values.extend(right_row.values.clone());
            let combined_row = storage::Row::new(combined_values);

            // Evaluate join condition
            let matches = if let Some(cond) = condition {
                match evaluator.eval(cond, &combined_row)? {
                    types::SqlValue::Boolean(true) => true,
                    types::SqlValue::Boolean(false) => false,
                    types::SqlValue::Null => false,
                    other => {
                        return Err(ExecutorError::InvalidWhereClause(format!(
                            "JOIN condition must evaluate to boolean, got: {:?}",
                            other
                        )))
                    }
                }
            } else {
                true
            };

            if matches {
                result_rows.push(combined_row);
                matched = true;
                right_matched[right_idx] = true;
            }
        }

        // If no match found, add left row with NULLs for right columns
        if !matched {
            let mut combined_values = left_row.values.clone();
            combined_values.extend(vec![types::SqlValue::Null; right_column_count]);
            result_rows.push(storage::Row::new(combined_values));
        }
    }

    // Second pass: Add unmatched right rows with NULLs for left columns
    for (right_idx, right_row) in right.rows.iter().enumerate() {
        if !right_matched[right_idx] {
            let mut combined_values = vec![types::SqlValue::Null; left_column_count];
            combined_values.extend(right_row.values.clone());
            result_rows.push(storage::Row::new(combined_values));
        }
    }

    Ok(FromResult { schema: combined_schema, rows: result_rows })
}

/// Nested loop CROSS JOIN implementation (Cartesian product)
pub(super) fn nested_loop_cross_join(
    left: FromResult,
    right: FromResult,
    condition: &Option<ast::Expression>,
    _database: &storage::Database,
) -> Result<FromResult, ExecutorError> {
    // CROSS JOIN should not have a condition
    if condition.is_some() {
        return Err(ExecutorError::UnsupportedFeature(
            "CROSS JOIN does not support ON clause".to_string(),
        ));
    }

    // Extract right table name and schema
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
    let combined_schema = CombinedSchema::combine(left.schema, right_table_name, right_schema);

    // CROSS JOIN = Cartesian product (every row from left × every row from right)
    let mut result_rows = Vec::new();
    for left_row in &left.rows {
        for right_row in &right.rows {
            let mut combined_values = left_row.values.clone();
            combined_values.extend(right_row.values.clone());
            result_rows.push(storage::Row::new(combined_values));
        }
    }

    Ok(FromResult { schema: combined_schema, rows: result_rows })
}

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
fn hash_join_inner(
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
                    let mut combined_values = build_row.values.clone();
                    combined_values.extend(probe_row.values.clone());
                    storage::Row::new(combined_values)
                } else {
                    // probe_row is from left, build_row is from right
                    let mut combined_values = probe_row.values.clone();
                    combined_values.extend(build_row.values.clone());
                    storage::Row::new(combined_values)
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
        let schema = TableSchema {
            name: table_name.to_string(),
            columns: columns
                .iter()
                .map(|(name, dtype)| ColumnSchema::new(
                    name.to_string(),
                    dtype.clone(),
                    true, // nullable
                ))
                .collect(),
            primary_key: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            check_constraints: vec![],
        };

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
