//! Join cost estimation
//!
//! This module implements cost estimation for join operations. Cost estimates
//! guide the search algorithm in selecting optimal join orders by predicting
//! the expense of different join sequences.

use std::collections::{HashMap, HashSet};

use super::{JoinCost, JoinOrderContext};

impl JoinOrderContext {
    /// Extract table cardinalities from actual table statistics, adjusted by WHERE clause selectivity
    ///
    /// Uses real row counts from database tables and applies selectivity estimation
    /// for WHERE clause predicates that filter specific tables.
    pub(super) fn extract_cardinalities_with_selectivity(
        analyzer: &crate::select::join::reorder::JoinOrderAnalyzer,
        database: &vibesql_storage::Database,
        table_local_predicates: &HashMap<String, Vec<vibesql_ast::Expression>>,
    ) -> std::collections::HashMap<String, usize> {
        let mut cardinalities = std::collections::HashMap::new();

        for table_name in analyzer.tables() {
            // Get actual table row count from database
            let base_rows = database
                .get_table(table_name.as_str())
                .map(|t| t.row_count())
                .unwrap_or(10000); // Fallback for CTEs/subqueries

            // Apply selectivity estimation for local predicates on this table
            let estimated_rows = if let Some(predicates) = table_local_predicates.get(&table_name.to_lowercase()) {
                // Get table statistics for selectivity estimation
                let stats = database
                    .get_table(table_name.as_str())
                    .and_then(|t| t.get_statistics());

                if let Some(stats) = stats {
                    // Estimate combined selectivity of all local predicates
                    let mut selectivity = 1.0;
                    for pred in predicates {
                        let pred_sel = crate::optimizer::selectivity::estimate_selectivity(pred, stats);
                        selectivity *= pred_sel;
                    }
                    // Apply selectivity to base row count
                    std::cmp::max(1, (base_rows as f64 * selectivity) as usize)
                } else {
                    // No stats available, use heuristic: assume each predicate filters ~30%
                    let selectivity = 0.3_f64.powi(predicates.len() as i32);
                    std::cmp::max(1, (base_rows as f64 * selectivity) as usize)
                }
            } else {
                base_rows
            };

            // Debug logging
            if std::env::var("JOIN_REORDER_VERBOSE").is_ok() && base_rows != estimated_rows {
                eprintln!(
                    "[JOIN_REORDER] Table {} cardinality: {} -> {} (after WHERE filter)",
                    table_name, base_rows, estimated_rows
                );
            }

            cardinalities.insert(table_name.clone(), estimated_rows);
        }

        cardinalities
    }

    /// Extract table cardinalities from actual table statistics (legacy, no selectivity)
    ///
    /// Uses real row counts from database tables instead of hardcoded estimates.
    /// This enables effective pruning in the search algorithm.
    pub(super) fn extract_cardinalities(
        analyzer: &crate::select::join::reorder::JoinOrderAnalyzer,
        database: &vibesql_storage::Database,
    ) -> std::collections::HashMap<String, usize> {
        Self::extract_cardinalities_with_selectivity(analyzer, database, &HashMap::new())
    }

    /// Estimate cost of joining next_table to already-joined tables
    ///
    /// # Parameters
    /// - `current_cardinality`: Size of intermediate result after all previous joins
    /// - `joined_tables`: Set of tables already joined (used to check for join edges)
    /// - `next_table`: Table being added to the join
    pub(super) fn estimate_join_cost(
        &self,
        current_cardinality: usize,
        joined_tables: &HashSet<String>,
        next_table: &str,
    ) -> JoinCost {
        if joined_tables.is_empty() {
            // First table: just a scan with selectivity
            let cardinality = self.table_cardinalities.get(next_table).copied().unwrap_or(10000);
            return JoinCost::new(cardinality, 0);
        }

        // Use current intermediate result size as left side of join
        let left_cardinality = current_cardinality;

        let right_cardinality = self.table_cardinalities.get(next_table).copied().unwrap_or(10000);

        // Estimate join selectivity
        // If there's an equijoin condition, assume high selectivity (10%)
        // Otherwise assume lower selectivity (50%)
        let has_join_edge = self.has_join_edge(joined_tables, next_table);
        let selectivity = if has_join_edge { 0.1 } else { 0.5 };

        // Estimate output cardinality (cross product filtered by join condition)
        let output_cardinality = std::cmp::max(
            1,
            (left_cardinality as f64 * right_cardinality as f64 * selectivity) as usize,
        );

        // Estimate operations (roughly, cross product comparisons)
        let operations = (left_cardinality as u64) * (right_cardinality as u64);

        JoinCost::new(output_cardinality, operations)
    }

    /// Check if there's a join edge connecting the joined tables and next table
    pub(super) fn has_join_edge(&self, joined_tables: &HashSet<String>, next_table: &str) -> bool {
        for edge in &self.edges {
            if edge.involves_table(next_table) {
                for joined_table in joined_tables {
                    if edge.involves_table(joined_table) {
                        return true;
                    }
                }
            }
        }
        false
    }
}
