//! Greedy heuristic for large queries
//!
//! This module implements a polynomial-time approximation algorithm for join
//! order optimization when exhaustive search is impractical (8+ tables).

use std::collections::HashSet;

use super::{JoinCost, JoinOrderContext};

impl JoinOrderContext {
    /// Find optimal order using greedy heuristic
    ///
    /// This is a polynomial-time approximation algorithm for large queries where
    /// exhaustive search is impractical. It uses a greedy strategy:
    ///
    /// 1. Start with the smallest table (by row count)
    /// 2. At each step, choose the next table that:
    ///    a) Has a join condition with already-joined tables (if possible)
    ///    b) Produces the smallest intermediate result
    /// 3. If no joinable tables remain, pick the smallest unjoined table (Cartesian product)
    ///
    /// Time complexity: O(n²) where n = number of tables
    /// Space complexity: O(n)
    ///
    /// This produces good (though not necessarily optimal) join orders for large queries,
    /// avoiding the factorial explosion of exhaustive search.
    pub(super) fn find_optimal_order_greedy(&self) -> Vec<String> {
        if self.all_tables.is_empty() {
            return Vec::new();
        }

        let mut joined_tables = HashSet::new();
        let mut remaining_tables: HashSet<String> = self.all_tables.clone();
        let mut join_order = Vec::new();
        let mut current_cardinality: usize;

        // Step 1: Start with the smallest table (lowest cardinality)
        let first_table = remaining_tables
            .iter()
            .min_by_key(|table| self.table_cardinalities.get(*table).copied().unwrap_or(10000))
            .unwrap()
            .clone();

        current_cardinality = self.table_cardinalities.get(&first_table).copied().unwrap_or(10000);
        joined_tables.insert(first_table.clone());
        remaining_tables.remove(&first_table);
        join_order.push(first_table);

        // Step 2: Greedily add tables one at a time
        while !remaining_tables.is_empty() {
            let mut best_table: Option<String> = None;
            let mut best_cost = JoinCost::new(usize::MAX, u64::MAX);
            let mut best_has_edge = false;

            // Try each remaining table and pick the one with lowest cost
            for candidate in &remaining_tables {
                let has_edge = self.has_join_edge(&joined_tables, candidate);
                let cost = self.estimate_join_cost(current_cardinality, &joined_tables, candidate);

                // Prefer tables with join conditions (has_edge = true)
                // Among those, pick the one with lowest cost
                let is_better = match (has_edge, best_has_edge) {
                    (true, false) => true, // Join condition is better than Cartesian product
                    (false, true) => false, // Cartesian product is worse than join condition
                    _ => cost.total() < best_cost.total(), // Same join type, compare costs
                };

                if best_table.is_none() || is_better {
                    best_table = Some(candidate.clone());
                    best_cost = cost;
                    best_has_edge = has_edge;
                }
            }

            // Add the best table to the join order
            if let Some(table) = best_table {
                joined_tables.insert(table.clone());
                remaining_tables.remove(&table);
                join_order.push(table);
                // Update current cardinality to the result of this join
                current_cardinality = best_cost.cardinality;
            } else {
                // Shouldn't happen, but handle gracefully
                break;
            }
        }

        join_order
    }
}
