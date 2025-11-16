// ============================================================================
// Index Operations - Query methods on IndexData
// ============================================================================

use vibesql_types::SqlValue;

use super::index_metadata::{acquire_btree_lock, IndexData};

/// Normalize a SqlValue to a consistent numeric type for comparison in range scans.
/// This ensures that Real, Numeric, Float, Double, Integer, Smallint, Bigint, and Unsigned
/// values can be compared correctly regardless of their underlying type.
///
/// IMPORTANT: This function is also used at index insertion time to normalize all numeric values
/// to a canonical form (Double) before storing in the BTreeMap. This ensures that queries
/// comparing different numeric types (e.g., Real > Numeric) work correctly.
///
/// Uses f64 (Double) instead of f32 (Real) to preserve precision for:
/// - Large integers (Bigint, Unsigned) beyond f32 precision range (> 2^24 ≈ 16 million)
/// - High-precision floating point values (Double, Numeric)
pub fn normalize_for_comparison(value: &SqlValue) -> SqlValue {
    match value {
        SqlValue::Integer(i) => SqlValue::Double(*i as f64),
        SqlValue::Smallint(i) => SqlValue::Double(*i as f64),
        SqlValue::Bigint(i) => SqlValue::Double(*i as f64),
        SqlValue::Unsigned(u) => SqlValue::Double(*u as f64),
        SqlValue::Float(f) => SqlValue::Double(*f as f64),
        SqlValue::Real(r) => SqlValue::Double(*r as f64),
        SqlValue::Double(d) => SqlValue::Double(*d),
        SqlValue::Numeric(n) => SqlValue::Double(*n),
        // For non-numeric types, return as-is
        other => other.clone(),
    }
}

/// Calculate the next value after a given SqlValue for use as an exclusive upper bound
/// in prefix matching range scans.
///
/// For numeric types, this returns value + 1. For other types, returns None (unbounded).
/// This is used in DiskBacked index prefix matching to efficiently bound the range scan.
///
/// # Examples
/// - Double(10.0) → Some(Double(11.0))
/// - Integer(42) → Some(Integer(43))
/// - Varchar("abc") → None (no natural successor)
fn calculate_next_value(value: &SqlValue) -> Option<SqlValue> {
    match value {
        SqlValue::Double(d) => {
            // For doubles, add 1.0. Note: This works for integers represented as doubles
            // which is the normalized form used in indexes.
            Some(SqlValue::Double(d + 1.0))
        }
        SqlValue::Integer(i) => Some(SqlValue::Integer(i + 1)),
        SqlValue::Smallint(i) => Some(SqlValue::Smallint(i + 1)),
        SqlValue::Bigint(i) => Some(SqlValue::Bigint(i + 1)),
        SqlValue::Unsigned(u) => Some(SqlValue::Unsigned(u + 1)),
        SqlValue::Float(f) => Some(SqlValue::Float(f + 1.0)),
        SqlValue::Real(r) => Some(SqlValue::Real(r + 1.0)),
        SqlValue::Numeric(n) => Some(SqlValue::Numeric(n + 1.0)),
        // For non-numeric types, we can't easily calculate a successor
        // Return None to indicate unbounded end
        _ => None,
    }
}

/// Try to increment a SqlValue by the smallest possible amount
/// Returns None if the value is already at its maximum or increment is not supported
///
/// This is used to calculate upper bounds for BTreeMap ranges when doing prefix matching
/// on multi-column indexes. For example, to find all keys where first column <= 90,
/// we can use range(lower..Excluded([91])) instead of range(lower..Unbounded) + manual check.
fn try_increment_sqlvalue(value: &SqlValue) -> Option<SqlValue> {
    match value {
        // Integer types: increment by 1, handle overflow
        SqlValue::Integer(i) if *i < i64::MAX => Some(SqlValue::Integer(i + 1)),
        SqlValue::Smallint(i) if *i < i16::MAX => Some(SqlValue::Smallint(i + 1)),
        SqlValue::Bigint(i) if *i < i64::MAX => Some(SqlValue::Bigint(i + 1)),
        SqlValue::Unsigned(u) if *u < u64::MAX => Some(SqlValue::Unsigned(u + 1)),

        // Floating point: increment by smallest representable step
        // For floats, we use nextafter functionality via adding epsilon
        SqlValue::Float(f) if f.is_finite() => {
            let next = f + f.abs() * f32::EPSILON;
            if next > *f && next.is_finite() {
                Some(SqlValue::Float(next))
            } else {
                None
            }
        }
        SqlValue::Real(f) if f.is_finite() => {
            let next = f + f.abs() * f32::EPSILON;
            if next > *f && next.is_finite() {
                Some(SqlValue::Real(next))
            } else {
                None
            }
        }
        SqlValue::Double(f) if f.is_finite() => {
            let next = f + f.abs() * f64::EPSILON;
            if next > *f && next.is_finite() {
                Some(SqlValue::Double(next))
            } else {
                None
            }
        }
        SqlValue::Numeric(f) if f.is_finite() => {
            let next = f + f.abs() * f64::EPSILON;
            if next > *f && next.is_finite() {
                Some(SqlValue::Numeric(next))
            } else {
                None
            }
        }

        // String types: append a null character to get the next string
        // This works because "\0" is the smallest character
        SqlValue::Varchar(s) => Some(SqlValue::Varchar(format!("{}\0", s))),
        SqlValue::Character(s) => Some(SqlValue::Character(format!("{}\0", s))),

        // Boolean: false < true, so true has no next value
        SqlValue::Boolean(false) => Some(SqlValue::Boolean(true)),
        SqlValue::Boolean(true) => None,

        // Null is always smallest, so it has a next value (any non-null)
        // But we don't have a clear "next" value, so return None
        SqlValue::Null => None,

        // Date/Time types: for now, return None (could implement increment by smallest unit)
        SqlValue::Date(_) | SqlValue::Time(_) | SqlValue::Timestamp(_) | SqlValue::Interval(_) => None,

        // All other cases (overflow, already at max, etc.): return None
        _ => None,
    }
}

impl IndexData {
    /// Scan index for rows matching range predicate
    ///
    /// # Arguments
    /// * `start` - Lower bound (None = unbounded)
    /// * `end` - Upper bound (None = unbounded)
    /// * `inclusive_start` - Include rows equal to start value
    /// * `inclusive_end` - Include rows equal to end value
    ///
    /// # Returns
    /// Vector of row indices that match the range predicate
    ///
    /// # Performance
    /// Uses BTreeMap's efficient range() method for O(log n + k) complexity,
    /// where n is the number of unique keys and k is the number of matching keys.
    /// This is significantly faster than the previous O(n) full scan approach.
    pub fn range_scan(
        &self,
        start: Option<&SqlValue>,
        end: Option<&SqlValue>,
        inclusive_start: bool,
        inclusive_end: bool,
    ) -> Vec<usize> {
        match self {
            IndexData::InMemory { data } => {
                use std::ops::Bound;

                let mut matching_row_indices = Vec::new();

                // Normalize bounds for consistent numeric comparison
                // This allows Real, Numeric, Integer, etc. to be compared correctly
                let normalized_start = start.map(normalize_for_comparison);
                let normalized_end = end.map(normalize_for_comparison);

                // Special handling for prefix matching on multi-column indexes
                // This handles both equality queries (start == end) and range queries (start != end)
                // on the first column of multi-column indexes.
                //
                // The key insight: For multi-column indexes, we can't use single-element keys
                // like [value] because the index stores composite keys like [col1_val, col2_val].
                // Instead, we need to iterate and check the first column of each key.
                if let (Some(start_val), Some(end_val)) = (&normalized_start, &normalized_end) {
                    // Equality check (prefix matching for multi-column IN clauses)
                    if start_val == end_val && inclusive_start && inclusive_end {
                        // Prefix matching using efficient BTreeMap::range() - O(log n + k) instead of O(n)
                        // Strategy: Start iteration at [target_value] and continue while first column matches
                        //
                        // For example, to find all rows where column `a` = 10 in index (a, b):
                        //   Start: Bound::Included([10])
                        //   Continue iterating while key[0] == 10
                        //   Stop when key[0] != 10
                        //
                        // This works because BTreeMap orders Vec<SqlValue> lexicographically:
                        //   [10] < [10, 1] < [10, 2] < [10, 99] < [11] < [11, 1]

                        let start_key = vec![start_val.clone()];
                        let start_bound: Bound<&[SqlValue]> = Bound::Included(start_key.as_slice());

                        // Iterate from start_key to end of map, stopping when first column changes
                        for (key_values, row_indices) in data.range::<[SqlValue], _>((start_bound, Bound::Unbounded)) {
                            // Check if first column still matches target
                            if key_values.is_empty() || &key_values[0] != start_val {
                                break; // Stop iteration when prefix no longer matches
                            }
                            matching_row_indices.extend(row_indices);
                        }
                        return matching_row_indices;
                    }
                }

                // Edge case: Check for invalid range (start == end with at least one exclusive bound)
                // Example: col > 5 AND col < 5 (mathematically empty range, but valid SQL)
                // This would create invalid BTreeMap bounds (both excluded at same value) → panic
                if let (Some(start_val), Some(end_val)) = (&normalized_start, &normalized_end) {
                    if start_val == end_val && (!inclusive_start || !inclusive_end) {
                        // Empty range: no values can satisfy this condition
                        return Vec::new();
                    }
                }

                // Special handling for range queries that might use multi-column indexes
                // If we have both start and end bounds, we need to handle multi-column indexes specially
                // by checking if keys in the map have multiple elements (indicating multi-column index)
                if normalized_start.is_some() || normalized_end.is_some() {
                    // Peek at first key to determine if this is a multi-column index
                    // Multi-column indexes have keys with > 1 element
                    let is_multi_column = data.keys().next().map_or(false, |k| k.len() > 1);

                    if is_multi_column {
                        // Multi-column index range query: iterate and compare first column only
                        // For example, col3 BETWEEN 24 AND 90 on index (col3, col0):
                        //   Start at keys with col3 >= 24, continue while col3 <= 90

                        let start_key = normalized_start.as_ref().map(|v| vec![v.clone()]);
                        let start_bound: Bound<&[SqlValue]> = match start_key.as_ref() {
                            Some(key) if inclusive_start => Bound::Included(key.as_slice()),
                            Some(key) => Bound::Excluded(key.as_slice()),
                            None => Bound::Unbounded,
                        };

                        // Calculate upper bound efficiently instead of using Unbounded + manual checking
                        // For multi-column indexes, we need an upper bound that stops after all keys
                        // starting with end_val (if inclusive) or before them (if exclusive)
                        let end_key = normalized_end.as_ref().map(|v| {
                            if inclusive_end {
                                // For inclusive: try to increment the value to get next prefix
                                // If successful, use as Excluded bound; otherwise use Unbounded
                                try_increment_sqlvalue(v).map(|incremented| (vec![incremented], false))
                            } else {
                                // For exclusive: use end_val itself as Excluded bound
                                Some((vec![v.clone()], false))
                            }
                        }).flatten();

                        let end_bound: Bound<&[SqlValue]> = match end_key.as_ref() {
                            Some((key, _)) => Bound::Excluded(key.as_slice()),
                            None => Bound::Unbounded,
                        };

                        // Iterate through BTreeMap with proper bounds - no manual checking needed!
                        for (_key_values, row_indices) in data.range::<[SqlValue], _>((start_bound, end_bound)) {
                            matching_row_indices.extend(row_indices);
                        }
                        return matching_row_indices;
                    }
                }

                // Standard range scan for single-column indexes or actual range queries
                // Convert to single-element keys (for single-column indexes)
                let start_key = normalized_start.as_ref().map(|v| vec![v.clone()]);
                let end_key = normalized_end.as_ref().map(|v| vec![v.clone()]);

                // Build bounds for BTreeMap::range()
                // Note: BTreeMap<Vec<SqlValue>, _> requires &[SqlValue] (slice) for range bounds
                // Type annotation is needed to help Rust's type inference
                let start_bound: Bound<&[SqlValue]> = match start_key.as_ref() {
                    Some(key) if inclusive_start => Bound::Included(key.as_slice()),
                    Some(key) => Bound::Excluded(key.as_slice()),
                    None => Bound::Unbounded,
                };

                let end_bound: Bound<&[SqlValue]> = match end_key.as_ref() {
                    Some(key) if inclusive_end => Bound::Included(key.as_slice()),
                    Some(key) => Bound::Excluded(key.as_slice()),
                    None => Bound::Unbounded,
                };

                // Use BTreeMap's efficient range() method instead of full iteration
                // This is O(log n + k) instead of O(n) where n = total keys, k = matching keys
                // Explicit type parameter needed due to Borrow trait ambiguity
                for (_key_values, row_indices) in data.range::<[SqlValue], _>((start_bound, end_bound)) {
                    matching_row_indices.extend(row_indices);
                }

                // Return row indices in the order established by BTreeMap iteration
                // BTreeMap gives us results sorted by index key value, which is the
                // expected order for indexed queries. We should NOT sort by row index
                // as that would destroy the index-based ordering.
                matching_row_indices
            }
            IndexData::DiskBacked { btree, .. } => {
                // Normalize bounds for consistent numeric comparison (same as InMemory)
                // This ensures Real, Numeric, Integer, etc. can be compared correctly
                let normalized_start = start.map(normalize_for_comparison);
                let normalized_end = end.map(normalize_for_comparison);

                // Special handling for prefix matching (multi-column IN clauses) - same as InMemory
                // When start == end with inclusive bounds, we're doing an equality check on the
                // first column of a multi-column index.
                let is_prefix_match = if let (Some(start_val), Some(end_val)) = (&normalized_start, &normalized_end) {
                    start_val == end_val && inclusive_start && inclusive_end
                } else {
                    false
                };

                if is_prefix_match {
                    // Prefix matching for DiskBacked - optimized using calculated upper bound
                    // Strategy: Calculate the next value to use as exclusive upper bound
                    //
                    // For example, to find all rows where column `a` = 10 in index (a, b):
                    //   Range: [10] (inclusive) to [11] (exclusive)
                    //   This captures all keys like [10, 1], [10, 2], ... [10, 999]
                    //   But excludes [11, x] for any x
                    //
                    // This works because BTreeMap orders Vec<SqlValue> lexicographically:
                    //   [10] < [10, 1] < [10, 2] < [10, 99] < [11] < [11, 1]

                    let target_value = normalized_start.as_ref().unwrap(); // Safe: checked above
                    let start_key = vec![target_value.clone()];

                    // Calculate upper bound: next value after target
                    // For numeric types, we can increment; for others, use unbounded
                    let end_key = calculate_next_value(target_value).map(|next_val| vec![next_val]);

                    // Range scan from [target] to [next_value) exclusive
                    match acquire_btree_lock(btree) {
                        Ok(guard) => guard
                            .range_scan(
                                Some(&start_key),
                                end_key.as_ref(), // Bounded end (or unbounded if can't increment)
                                true,  // Inclusive start
                                false, // Exclusive end
                            )
                            .unwrap_or_else(|_| vec![]),
                        Err(e) => {
                            log::warn!("BTreeIndex lock acquisition failed in range_scan: {}", e);
                            vec![]
                        }
                    }
                } else {
                    // Standard range scan for single-column indexes or actual range queries
                    let start_key = normalized_start.as_ref().map(|v| vec![v.clone()]);
                    let end_key = normalized_end.as_ref().map(|v| vec![v.clone()]);

                    // Safely acquire lock and call BTreeIndex::range_scan
                    match acquire_btree_lock(btree) {
                        Ok(guard) => guard
                            .range_scan(
                                start_key.as_ref(),
                                end_key.as_ref(),
                                inclusive_start,
                                inclusive_end,
                            )
                            .unwrap_or_else(|_| vec![]),
                        Err(e) => {
                            // Log error and return empty result set
                            log::warn!("BTreeIndex lock acquisition failed in range_scan: {}", e);
                            vec![]
                        }
                    }
                }
            }
        }
    }

    /// Lookup multiple values in the index (for IN predicates)
    ///
    /// # Arguments
    /// * `values` - List of values to look up
    ///
    /// # Returns
    /// Vector of row indices that match any of the values
    pub fn multi_lookup(&self, values: &[SqlValue]) -> Vec<usize> {
        match self {
            IndexData::InMemory { data } => {
                let mut matching_row_indices = Vec::new();

                for value in values {
                    // Normalize value for consistent lookup (matches insertion-time normalization)
                    let normalized_value = normalize_for_comparison(value);
                    let search_key = vec![normalized_value];
                    if let Some(row_indices) = data.get(&search_key) {
                        matching_row_indices.extend(row_indices);
                    }
                }

                // Return row indices in the order they were collected from BTreeMap
                // For IN predicates, we collect results for each value in the order
                // specified. We should NOT sort by row index as that would destroy
                // the semantic ordering of the results.
                matching_row_indices
            }
            IndexData::DiskBacked { btree, .. } => {
                // Normalize values for consistent lookup (matches insertion-time normalization)
                // Convert SqlValue values to Key (Vec<SqlValue>) format
                let keys: Vec<Vec<SqlValue>> = values
                    .iter()
                    .map(|v| vec![normalize_for_comparison(v)])
                    .collect();

                // Safely acquire lock and call BTreeIndex::multi_lookup
                match acquire_btree_lock(btree) {
                    Ok(guard) => guard.multi_lookup(&keys).unwrap_or_else(|_| vec![]),
                    Err(e) => {
                        // Log error and return empty result set
                        log::warn!("BTreeIndex lock acquisition failed in multi_lookup: {}", e);
                        vec![]
                    }
                }
            }
        }
    }

    /// Get an iterator over all key-value pairs in the index
    ///
    /// # Returns
    /// Iterator yielding references to (key, row_indices) pairs
    ///
    /// # Note
    /// For in-memory indexes, iteration is in sorted key order (BTreeMap ordering).
    /// This method enables index scanning operations without exposing internal data structures.
    pub fn iter(&self) -> Box<dyn Iterator<Item = (&Vec<SqlValue>, &Vec<usize>)> + '_> {
        match self {
            IndexData::InMemory { data } => Box::new(data.iter()),
            IndexData::DiskBacked { .. } => {
                // TODO: Implement when DiskBacked is active
                unimplemented!("DiskBacked iteration not yet implemented")
            }
        }
    }

    /// Lookup exact key in the index
    ///
    /// # Arguments
    /// * `key` - Key to look up
    ///
    /// # Returns
    /// Reference to vector of row indices if key exists, None otherwise
    ///
    /// # Note
    /// This is the primary point-lookup API for index queries.
    pub fn get(&self, key: &[SqlValue]) -> Option<&Vec<usize>> {
        match self {
            IndexData::InMemory { data } => data.get(key),
            IndexData::DiskBacked { .. } => {
                // TODO: Implement when DiskBacked is active
                unimplemented!("DiskBacked lookup not yet implemented")
            }
        }
    }

    /// Check if a key exists in the index
    ///
    /// # Arguments
    /// * `key` - Key to check
    ///
    /// # Returns
    /// true if key exists, false otherwise
    ///
    /// # Note
    /// Used primarily for UNIQUE constraint validation.
    pub fn contains_key(&self, key: &[SqlValue]) -> bool {
        match self {
            IndexData::InMemory { data } => data.contains_key(key),
            IndexData::DiskBacked { .. } => {
                // TODO: Implement when DiskBacked is active
                unimplemented!("DiskBacked contains_key not yet implemented")
            }
        }
    }

    /// Get an iterator over all row index vectors in the index
    ///
    /// # Returns
    /// Iterator yielding references to row index vectors
    ///
    /// # Note
    /// This method is used for full index scans where we need all row indices
    /// regardless of the key values.
    pub fn values(&self) -> Box<dyn Iterator<Item = &Vec<usize>> + '_> {
        match self {
            IndexData::InMemory { data } => Box::new(data.values()),
            IndexData::DiskBacked { .. } => {
                // TODO: Implement when DiskBacked is active
                unimplemented!("DiskBacked values iteration not yet implemented")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_next_value_double() {
        assert_eq!(
            calculate_next_value(&SqlValue::Double(10.0)),
            Some(SqlValue::Double(11.0))
        );
        assert_eq!(
            calculate_next_value(&SqlValue::Double(-5.5)),
            Some(SqlValue::Double(-4.5))
        );
        assert_eq!(
            calculate_next_value(&SqlValue::Double(0.0)),
            Some(SqlValue::Double(1.0))
        );
    }

    #[test]
    fn test_calculate_next_value_integer() {
        assert_eq!(
            calculate_next_value(&SqlValue::Integer(42)),
            Some(SqlValue::Integer(43))
        );
        assert_eq!(
            calculate_next_value(&SqlValue::Integer(-10)),
            Some(SqlValue::Integer(-9))
        );
        assert_eq!(
            calculate_next_value(&SqlValue::Integer(0)),
            Some(SqlValue::Integer(1))
        );
    }

    #[test]
    fn test_calculate_next_value_smallint() {
        assert_eq!(
            calculate_next_value(&SqlValue::Smallint(100)),
            Some(SqlValue::Smallint(101))
        );
        assert_eq!(
            calculate_next_value(&SqlValue::Smallint(-1)),
            Some(SqlValue::Smallint(0))
        );
    }

    #[test]
    fn test_calculate_next_value_bigint() {
        assert_eq!(
            calculate_next_value(&SqlValue::Bigint(1_000_000_000)),
            Some(SqlValue::Bigint(1_000_000_001))
        );
        assert_eq!(
            calculate_next_value(&SqlValue::Bigint(-999)),
            Some(SqlValue::Bigint(-998))
        );
    }

    #[test]
    fn test_calculate_next_value_unsigned() {
        assert_eq!(
            calculate_next_value(&SqlValue::Unsigned(0)),
            Some(SqlValue::Unsigned(1))
        );
        assert_eq!(
            calculate_next_value(&SqlValue::Unsigned(999)),
            Some(SqlValue::Unsigned(1000))
        );
    }

    #[test]
    fn test_calculate_next_value_float() {
        // Use simpler values for f32 to avoid precision issues
        assert_eq!(
            calculate_next_value(&SqlValue::Float(3.0)),
            Some(SqlValue::Float(4.0))
        );
        assert_eq!(
            calculate_next_value(&SqlValue::Float(-2.5)),
            Some(SqlValue::Float(-1.5))
        );
    }

    #[test]
    fn test_calculate_next_value_real() {
        assert_eq!(
            calculate_next_value(&SqlValue::Real(7.5)),
            Some(SqlValue::Real(8.5))
        );
        assert_eq!(
            calculate_next_value(&SqlValue::Real(0.0)),
            Some(SqlValue::Real(1.0))
        );
    }

    #[test]
    fn test_calculate_next_value_numeric() {
        assert_eq!(
            calculate_next_value(&SqlValue::Numeric(99.99)),
            Some(SqlValue::Numeric(100.99))
        );
        assert_eq!(
            calculate_next_value(&SqlValue::Numeric(-10.5)),
            Some(SqlValue::Numeric(-9.5))
        );
    }

    #[test]
    fn test_calculate_next_value_non_numeric() {
        // Text types
        assert_eq!(calculate_next_value(&SqlValue::Varchar("abc".to_string())), None);
        assert_eq!(calculate_next_value(&SqlValue::Character("hello".to_string())), None);

        // NULL
        assert_eq!(calculate_next_value(&SqlValue::Null), None);

        // Boolean
        assert_eq!(calculate_next_value(&SqlValue::Boolean(true)), None);
        assert_eq!(calculate_next_value(&SqlValue::Boolean(false)), None);
    }

    #[test]
    fn test_normalize_for_comparison_numeric_types() {
        // All numeric types should normalize to Double
        assert_eq!(
            normalize_for_comparison(&SqlValue::Integer(42)),
            SqlValue::Double(42.0)
        );
        assert_eq!(
            normalize_for_comparison(&SqlValue::Smallint(10)),
            SqlValue::Double(10.0)
        );
        assert_eq!(
            normalize_for_comparison(&SqlValue::Bigint(1000)),
            SqlValue::Double(1000.0)
        );
        assert_eq!(
            normalize_for_comparison(&SqlValue::Unsigned(99)),
            SqlValue::Double(99.0)
        );
        assert_eq!(
            normalize_for_comparison(&SqlValue::Float(3.14)),
            SqlValue::Double(3.14f32 as f64)
        );
        assert_eq!(
            normalize_for_comparison(&SqlValue::Real(2.5)),
            SqlValue::Double(2.5)
        );
        assert_eq!(
            normalize_for_comparison(&SqlValue::Numeric(123.45)),
            SqlValue::Double(123.45)
        );
        assert_eq!(
            normalize_for_comparison(&SqlValue::Double(7.89)),
            SqlValue::Double(7.89)
        );
    }

    #[test]
    fn test_normalize_for_comparison_non_numeric() {
        // Non-numeric types should be returned as-is
        let text_val = SqlValue::Varchar("test".to_string());
        assert_eq!(normalize_for_comparison(&text_val), text_val);

        let null_val = SqlValue::Null;
        assert_eq!(normalize_for_comparison(&null_val), null_val);

        let bool_val = SqlValue::Boolean(true);
        assert_eq!(normalize_for_comparison(&bool_val), bool_val);
    }
}
