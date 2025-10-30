//! Row partitioning for window functions
//!
//! Groups rows into partitions based on PARTITION BY expressions.

use ast::Expression;
use storage::Row;
use types::SqlValue;

/// A partition of rows for window function evaluation
#[derive(Debug, Clone)]
pub struct Partition {
    pub rows: Vec<Row>,
    /// Original indices of rows before partitioning/sorting
    pub original_indices: Vec<usize>,
}

impl Partition {
    pub fn new(rows: Vec<Row>) -> Self {
        let original_indices = (0..rows.len()).collect();
        Self { rows, original_indices }
    }

    pub fn with_indices(rows: Vec<Row>, original_indices: Vec<usize>) -> Self {
        Self { rows, original_indices }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Partition rows by PARTITION BY expressions
///
/// Groups rows into partitions based on partition expressions.
/// If no PARTITION BY clause, all rows go into a single partition.
pub fn partition_rows<F>(
    rows: Vec<Row>,
    partition_by: &Option<Vec<Expression>>,
    eval_fn: F,
) -> Vec<Partition>
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    // If no PARTITION BY, return all rows in single partition
    let Some(partition_exprs) = partition_by else {
        return vec![Partition::new(rows)];
    };

    if partition_exprs.is_empty() {
        return vec![Partition::new(rows)];
    }

    // Group rows by partition key values (use BTreeMap for deterministic ordering)
    // Track original indices through partitioning
    let mut partitions_map: std::collections::BTreeMap<Vec<String>, Vec<(usize, Row)>> =
        std::collections::BTreeMap::new();

    for (original_idx, row) in rows.into_iter().enumerate() {
        // Evaluate partition expressions for this row
        let mut partition_key = Vec::new();

        for expr in partition_exprs {
            let value = eval_fn(expr, &row).unwrap_or(SqlValue::Null);
            // Convert to string for grouping (handles NULL consistently)
            partition_key.push(format!("{:?}", value));
        }

        partitions_map.entry(partition_key).or_default().push((original_idx, row));
    }

    // Convert HashMap to Vec<Partition>, preserving original indices
    partitions_map.into_values()
        .map(|rows_with_indices| {
            let (indices, rows): (Vec<_>, Vec<_>) = rows_with_indices.into_iter().unzip();
            Partition::with_indices(rows, indices)
        })
        .collect()
}
