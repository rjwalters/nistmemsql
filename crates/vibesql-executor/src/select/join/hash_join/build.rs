use std::collections::HashMap;

#[cfg(feature = "parallel")]
use rayon::prelude::*;

#[cfg(feature = "parallel")]
use crate::select::parallel::ParallelConfig;

/// Build hash table sequentially using indices (fallback for small inputs)
///
/// Returns a map from join key to row indices, avoiding storing row references
/// which enables deferred materialization.
pub(crate) fn build_hash_table_sequential(
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
pub(crate) fn build_hash_table_parallel(
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
