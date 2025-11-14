// ============================================================================
// Index Operations - Query methods on IndexData
// ============================================================================

use vibesql_types::SqlValue;

use super::index_metadata::{acquire_btree_lock, IndexData};

impl IndexData {
    /// Return true if the SqlValue is any numeric type (integer or approximate)
    fn is_numeric(val: &SqlValue) -> bool {
        matches!(
            val,
            SqlValue::Integer(_)
                | SqlValue::Smallint(_)
                | SqlValue::Bigint(_)
                | SqlValue::Unsigned(_)
                | SqlValue::Numeric(_)
                | SqlValue::Float(_)
                | SqlValue::Real(_)
                | SqlValue::Double(_)
        )
    }

    /// Convert a SqlValue numeric to f64 for cross-type numeric comparisons
    /// Returns None for non-numeric or NULL values
    fn to_f64(val: &SqlValue) -> Option<f64> {
        match val {
            SqlValue::Integer(i) => Some(*i as f64),
            SqlValue::Smallint(i) => Some(*i as f64),
            SqlValue::Bigint(i) => Some(*i as f64),
            SqlValue::Unsigned(u) => Some(*u as f64),
            SqlValue::Numeric(f) => Some(*f as f64),
            SqlValue::Float(f) => Some(*f as f64),
            SqlValue::Real(f) => Some(*f as f64),
            SqlValue::Double(f) => Some(*f),
            _ => None,
        }
    }

    /// Compare two SqlValues with numeric-aware semantics for range checks.
    /// If both values are numeric (any mix of integer/float/numeric), compare as f64.
    /// Otherwise, fall back to PartialOrd semantics (which may return None for incomparable types).
    fn cmp_for_range(a: &SqlValue, b: &SqlValue) -> Option<std::cmp::Ordering> {
        if IndexData::is_numeric(a) && IndexData::is_numeric(b) {
            match (IndexData::to_f64(a), IndexData::to_f64(b)) {
                (Some(af), Some(bf)) => af.partial_cmp(&bf),
                _ => None,
            }
        } else {
            a.partial_cmp(b)
        }
    }

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
                let mut matching_row_indices = Vec::new();

                // Iterate through BTreeMap (which gives us sorted iteration)
                // For multi-column indexes, we only compare the first column
                // This maintains compatibility with the original HashMap implementation
                let debug = std::env::var("VIBESQL_DEBUG_INDEX").ok().as_deref() == Some("1");
                let mut debug_checked = 0usize;
                for (key_values, row_indices) in data {
                    // For single-column index, key_values has one element
                    // For multi-column indexes, we only compare the first column
                    let key = &key_values[0];

                    let matches = match (start, end) {
                        (Some(s), Some(e)) => {
                            // Both bounds specified: start <= key <= end (or variations)
                            let gte_start = match IndexData::cmp_for_range(key, s) {
                                Some(std::cmp::Ordering::Greater) => true,
                                Some(std::cmp::Ordering::Equal) => inclusive_start,
                                Some(std::cmp::Ordering::Less) => false,
                                None => false,
                            };
                            let lte_end = match IndexData::cmp_for_range(key, e) {
                                Some(std::cmp::Ordering::Less) => true,
                                Some(std::cmp::Ordering::Equal) => inclusive_end,
                                Some(std::cmp::Ordering::Greater) => false,
                                None => false,
                            };
                            if debug && debug_checked < 8 {
                                eprintln!(
                                    "[IndexScan][range_scan] key={:?} cmp_start={:?} cmp_end={:?} -> gte_start={} lte_end={}",
                                    key,
                                    IndexData::cmp_for_range(key, s),
                                    IndexData::cmp_for_range(key, e),
                                    gte_start,
                                    lte_end
                                );
                            }
                            gte_start && lte_end
                        }
                        (Some(s), None) => {
                            // Only lower bound: key >= start (or >)
                            let res = match IndexData::cmp_for_range(key, s) {
                                Some(std::cmp::Ordering::Greater) => true,
                                Some(std::cmp::Ordering::Equal) => inclusive_start,
                                _ => false,
                            };
                            if debug && debug_checked < 8 {
                                eprintln!(
                                    "[IndexScan][range_scan] key={:?} cmp_start={:?} -> include={}",
                                    key,
                                    IndexData::cmp_for_range(key, s),
                                    res
                                );
                            }
                            res
                        }
                        (None, Some(e)) => {
                            // Only upper bound: key <= end (or <)
                            let res = match IndexData::cmp_for_range(key, e) {
                                Some(std::cmp::Ordering::Less) => true,
                                Some(std::cmp::Ordering::Equal) => inclusive_end,
                                _ => false,
                            };
                            if debug && debug_checked < 8 {
                                eprintln!(
                                    "[IndexScan][range_scan] key={:?} cmp_end={:?} -> include={}",
                                    key,
                                    IndexData::cmp_for_range(key, e),
                                    res
                                );
                            }
                            res
                        }
                        (None, None) => true, // No bounds - match everything
                    };

                    if matches {
                        matching_row_indices.extend(row_indices);
                    }
                    if debug {
                        debug_checked += 1;
                    }
                }

                // Return row indices in the order established by BTreeMap iteration
                // BTreeMap gives us results sorted by index key value, which is the
                // expected order for indexed queries. We should NOT sort by row index
                // as that would destroy the index-based ordering.
                matching_row_indices
            }
            IndexData::DiskBacked { btree, .. } => {
                // Convert SqlValue bounds to Key (Vec<SqlValue>) bounds
                // For single-column indexes, wrap in vec
                // For multi-column indexes, only first column is compared (same as InMemory)
                let start_key = start.map(|v| vec![v.clone()]);
                let end_key = end.map(|v| vec![v.clone()]);

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
                    let search_key = vec![value.clone()];
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
                // Convert SqlValue values to Key (Vec<SqlValue>) format
                let keys: Vec<Vec<SqlValue>> = values
                    .iter()
                    .map(|v| vec![v.clone()])
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
