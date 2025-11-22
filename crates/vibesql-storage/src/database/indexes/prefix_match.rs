// ============================================================================
// Prefix Match - Multi-column index prefix matching
// ============================================================================

use vibesql_types::SqlValue;

use super::index_metadata::IndexData;

impl IndexData {
    /// Lookup multiple values using prefix matching for multi-column indexes
    ///
    /// This method is designed for multi-column indexes where we want to match on the
    /// first column only. For example, with index on (a, b) and query `WHERE a IN (10, 20)`,
    /// this will find all rows where `a=10` OR `a=20`, regardless of the value of `b`.
    ///
    /// # Arguments
    /// * `values` - List of values for the first indexed column
    ///
    /// # Returns
    /// Vector of row indices where the first column matches any of the values
    ///
    /// # Implementation Notes
    /// This uses the existing `range_scan()` method with start==end (equality check),
    /// which already has built-in prefix matching support for multi-column indexes.
    /// See `range_scan()` implementation for the prefix matching logic.
    ///
    /// This solves the issue where `multi_lookup([10])` would fail to match index keys
    /// like `[10, 20]` because BTreeMap requires exact key matches.
    pub fn prefix_multi_lookup(&self, values: &[SqlValue]) -> Vec<usize> {
        // Deduplicate values to avoid returning duplicate rows
        // For example, WHERE a IN (10, 10, 20) should only look up 10 once
        let mut unique_values: Vec<&SqlValue> = values.iter().collect();
        unique_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        unique_values.dedup();

        let mut matching_row_indices = Vec::new();

        for value in unique_values {
            // Use range_scan with start==end (both inclusive) to trigger prefix matching
            // The range_scan() implementation automatically handles multi-column indexes
            // by iterating through all keys where the first column matches 'value'
            let range_indices = self.range_scan(
                Some(value), // start
                Some(value), // end (same as start for equality/prefix matching)
                true,        // inclusive_start
                true,        // inclusive_end
            );

            matching_row_indices.extend(range_indices);
        }

        matching_row_indices
    }
}
