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
/// This is used in multi-column index range scans to efficiently bound the range scan.
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

                // Validate normalized bounds: if start > end, range is empty (SQL standard behavior)
                // This must happen AFTER normalization because comparisons between different numeric
                // types (e.g., Real vs Integer) might change after normalization to a common type (Double).
                // Example: col BETWEEN 10.5 AND 10 is empty because 10.5 > 10 after normalization.
                if let (Some(start_val), Some(end_val)) = (&normalized_start, &normalized_end) {
                    // Compare normalized values using Ord trait (SqlValue implements Ord via Comparison trait)
                    // If start > end, the range is empty and we should return no rows
                    if start_val > end_val {
                        return Vec::new();
                    }
                }

                // Detect if this is a multi-column index by checking first key
                // For multi-column indexes, we need special handling to ensure correct range matching
                let is_multi_column = data.keys().next().map(|k| k.len() > 1).unwrap_or(false);

                // For multi-column indexes with an exclusive start bound, we need to use
                // an inclusive bound with the next value to correctly exclude all composite keys.
                // Example: For col0 > 3 (like excluding [3, 5.6]), we use range [4.0, ...)
                // because [3, 5.6] >= [3] but [3, 5.6] < [4] in lexicographic ordering.
                let (start_key, adjusted_inclusive_start) = if is_multi_column && !inclusive_start {
                    if let Some(start_val) = normalized_start.as_ref() {
                        // Try to calculate next value for inclusive lower bound
                        if let Some(next_val) = calculate_next_value(start_val) {
                            (Some(vec![next_val]), true) // Use next value with inclusive bound
                        } else {
                            // Can't calculate next value, use original with exclusive bound
                            (Some(vec![start_val.clone()]), false)
                        }
                    } else {
                        (None, false)
                    }
                } else {
                    // Single-column index or inclusive start: use value as-is
                    (normalized_start.as_ref().map(|v| vec![v.clone()]), inclusive_start)
                };

                // For multi-column indexes with an inclusive end bound, we need to use
                // an exclusive bound with the next value to correctly capture all composite keys.
                // Example: To match all keys where col0=9 (like [9, 5.6]), we use range [9.0, 10.0)
                // because [9, 5.6] < [10] but [9, 5.6] > [9] in lexicographic ordering.
                let (end_key, adjusted_inclusive_end) = if is_multi_column && inclusive_end {
                    if let Some(end_val) = normalized_end.as_ref() {
                        // Try to calculate next value for exclusive upper bound
                        if let Some(next_val) = calculate_next_value(end_val) {
                            (Some(vec![next_val]), false) // Use next value with exclusive bound
                        } else {
                            // Can't calculate next value (e.g., for strings), use unbounded end
                            (None, false)
                        }
                    } else {
                        (None, false)
                    }
                } else {
                    // Single-column index or non-inclusive end: use value as-is
                    (normalized_end.as_ref().map(|v| vec![v.clone()]), inclusive_end)
                };

                // Validate adjusted bounds for multi-column indexes
                // After adjustment, bounds might be invalid even if original bounds were valid
                // Example: col3 > 9 AND col3 < 3 has original bounds (9 exc, 3 exc) where 9 > 3
                // but gets adjusted to (10 inc, 3 exc) which would still trigger BTreeMap panic
                if let (Some(start_key_val), Some(end_key_val)) = (&start_key, &end_key) {
                    if !start_key_val.is_empty() && !end_key_val.is_empty() {
                        let start_first = &start_key_val[0];
                        let end_first = &end_key_val[0];
                        // Check if adjusted bounds are valid based on inclusivity
                        let invalid = match (adjusted_inclusive_start, adjusted_inclusive_end) {
                            (true, true) => start_first > end_first,   // [a, b]: invalid if a > b
                            (true, false) => start_first >= end_first, // [a, b): invalid if a >= b
                            (false, true) => start_first >= end_first, // (a, b]: invalid if a >= b
                            (false, false) => start_first >= end_first, // (a, b): invalid if a >= b
                        };
                        if invalid {
                            return Vec::new();
                        }
                    }
                }

                // Build bounds for BTreeMap::range()
                // Note: BTreeMap<Vec<SqlValue>, _> requires &[SqlValue] (slice) for range bounds
                // Type annotation is needed to help Rust's type inference
                let start_bound: Bound<&[SqlValue]> = match start_key.as_ref() {
                    Some(key) if adjusted_inclusive_start => Bound::Included(key.as_slice()),
                    Some(key) => Bound::Excluded(key.as_slice()),
                    None => Bound::Unbounded,
                };

                let end_bound: Bound<&[SqlValue]> = match end_key.as_ref() {
                    Some(key) if adjusted_inclusive_end => Bound::Included(key.as_slice()),
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

                // Validate normalized bounds: if start > end, range is empty (SQL standard behavior)
                // This must happen AFTER normalization because comparisons between different numeric
                // types (e.g., Real vs Integer) might change after normalization to a common type (Double).
                if let (Some(start_val), Some(end_val)) = (&normalized_start, &normalized_end) {
                    // Compare normalized values using Ord trait
                    // If start > end, the range is empty and we should return no rows
                    if start_val > end_val {
                        return Vec::new();
                    }
                }

                // Convert SqlValue bounds to Key (Vec<SqlValue>) bounds
                // For single-column indexes, wrap in vec
                // For multi-column indexes, only first column is compared (same as InMemory)
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
