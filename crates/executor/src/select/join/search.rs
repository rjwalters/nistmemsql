//! Join order optimization via exhaustive search
//!
//! This module implements join order optimization using tree search to explore
//! different orderings. Unlike greedy heuristics that commit to the first choice,
//! search exhaustively considers possibilities and selects the ordering that
//! minimizes estimated cost.
//!
//! ## Algorithm
//!
//! Uses depth-first search with branch-and-bound pruning:
//! 1. Start with empty set of joined tables
//! 2. At each step, try adding each unjoined table
//! 3. Estimate cost of this join
//! 4. Prune branch if cost exceeds current best
//! 5. Continue recursively until all tables joined
//!
//! ## Example
//!
//! ```text
//! Query: SELECT * FROM t1, t2, t3, t4, t5
//! WHERE t1.id = t2.id AND t2.id = t3.id ...
//!
//! Search tree:
//!               {}
//!            /  |  | \
//!         {t1} {t2} {t3} {t4} {t5}
//!          / |  \
//!      {t1,t2}  {t1,t3}  ...
//!       /  \
//!    {t1,t2,t3} ...
//!
//! Prune branches where cumulative cost > best found so far
//! ```

use std::collections::HashSet;
use super::reorder::{JoinEdge, JoinOrderAnalyzer};

/// Represents the cost of joining a set of tables
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct JoinCost {
    /// Estimated number of intermediate rows
    pub cardinality: usize,
    /// Estimated comparison operations
    pub operations: u64,
}

impl JoinCost {
    pub fn new(cardinality: usize, operations: u64) -> Self {
        Self { cardinality, operations }
    }

    /// Estimate total cost as a comparable value
    /// Prioritizes reducing intermediate row count (cardinality)
    /// then comparison operations
    pub fn total(&self) -> u64 {
        // Weight cardinality heavily since it affects downstream joins
        // 1 additional row impacts all future joins
        self.cardinality as u64 * 1000 + self.operations
    }
}

/// State during join order search
#[derive(Debug, Clone)]
struct SearchState {
    /// Tables already joined
    joined_tables: HashSet<String>,
    /// Cumulative cost so far
    cost_so_far: JoinCost,
    /// Ordering of tables
    order: Vec<String>,
}

/// Performs join order optimization via exhaustive search
pub struct JoinOrderSearch {
    /// All tables in the query
    all_tables: HashSet<String>,
    /// Join edges (which tables connect)
    edges: Vec<JoinEdge>,
    /// Estimated rows for each table after local filters
    table_cardinalities: std::collections::HashMap<String, usize>,
}

impl JoinOrderSearch {
    /// Create a new join order search from an analyzer
    pub fn from_analyzer(analyzer: &JoinOrderAnalyzer) -> Self {
        Self {
            all_tables: analyzer.tables().clone(),
            edges: analyzer.edges().to_vec(),
            table_cardinalities: Self::extract_cardinalities(analyzer),
        }
    }

    /// Extract table cardinalities from analyzer
    /// (currently uses defaults, should integrate with statistics in future)
    fn extract_cardinalities(
        analyzer: &JoinOrderAnalyzer,
    ) -> std::collections::HashMap<String, usize> {
        let mut cardinalities = std::collections::HashMap::new();

        for table_name in analyzer.tables() {
            // Start with default estimate
            // Tables with local filters typically have lower cardinality
            // This is a placeholder - should use actual statistics
            cardinalities.insert(table_name, 10000);
        }

        cardinalities
    }

    /// Find optimal join order by exploring search space
    ///
    /// Returns list of table names in the order they should be joined
    pub fn find_optimal_order(&self) -> Vec<String> {
        if self.all_tables.is_empty() {
            return Vec::new();
        }

        let initial_state = SearchState {
            joined_tables: HashSet::new(),
            cost_so_far: JoinCost::new(0, 0),
            order: Vec::new(),
        };

        let mut best_cost = u64::MAX;
        let mut best_order = vec![];

        self.search_recursive(initial_state, &mut best_cost, &mut best_order);

        best_order
    }

    /// Recursive depth-first search with pruning
    fn search_recursive(
        &self,
        state: SearchState,
        best_cost: &mut u64,
        best_order: &mut Vec<String>,
    ) {
        // Base case: all tables joined
        if state.joined_tables.len() == self.all_tables.len() {
            let total_cost = state.cost_so_far.total();
            if total_cost < *best_cost {
                *best_cost = total_cost;
                *best_order = state.order.clone();
            }
            return;
        }

        // Pruning: if current cost exceeds best, don't explore further
        if state.cost_so_far.total() >= *best_cost {
            return;
        }

        // Try adding each unjoined table
        for next_table in &self.all_tables {
            if state.joined_tables.contains(next_table) {
                continue;
            }

            // Estimate cost of joining this table
            let join_cost = self.estimate_join_cost(&state.joined_tables, next_table);

            // Create new state with this table added
            let mut next_state = state.clone();
            next_state.joined_tables.insert(next_table.clone());
            next_state.cost_so_far = JoinCost::new(
                state.cost_so_far.cardinality + join_cost.cardinality,
                state.cost_so_far.operations + join_cost.operations,
            );
            next_state.order.push(next_table.clone());

            // Recursively search from this state
            self.search_recursive(next_state, best_cost, best_order);
        }
    }

    /// Estimate cost of joining next_table to already-joined tables
    fn estimate_join_cost(&self, joined_tables: &HashSet<String>, next_table: &str) -> JoinCost {
        if joined_tables.is_empty() {
            // First table: just a scan with selectivity
            let cardinality = self
                .table_cardinalities
                .get(next_table)
                .copied()
                .unwrap_or(10000);
            return JoinCost::new(cardinality, 0);
        }

        // Estimate cardinality of joined result
        let left_cardinality: usize = joined_tables
            .iter()
            .filter_map(|t| self.table_cardinalities.get(t))
            .sum();

        let right_cardinality = self
            .table_cardinalities
            .get(next_table)
            .copied()
            .unwrap_or(10000);

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
    fn has_join_edge(&self, joined_tables: &HashSet<String>, next_table: &str) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::select::join::reorder::JoinOrderAnalyzer;

    #[test]
    fn test_single_table_order() {
        let mut analyzer = JoinOrderAnalyzer::new();
        analyzer.register_tables(vec!["t1".to_string()]);

        let search = JoinOrderSearch::from_analyzer(&analyzer);
        let order = search.find_optimal_order();

        assert_eq!(order.len(), 1);
        assert_eq!(order[0], "t1");
    }

    #[test]
    fn test_two_table_order() {
        let mut analyzer = JoinOrderAnalyzer::new();
        analyzer.register_tables(vec!["t1".to_string(), "t2".to_string()]);

        // Add join edge t1 - t2
        analyzer.add_edge(JoinEdge {
            left_table: "t1".to_string(),
            left_column: "id".to_string(),
            right_table: "t2".to_string(),
            right_column: "id".to_string(),
        });

        let search = JoinOrderSearch::from_analyzer(&analyzer);
        let order = search.find_optimal_order();

        assert_eq!(order.len(), 2);
        // Both orderings are valid, just verify both tables are present
        assert!(order.contains(&"t1".to_string()));
        assert!(order.contains(&"t2".to_string()));
    }

    #[test]
    fn test_three_table_chain() {
        let mut analyzer = JoinOrderAnalyzer::new();
        analyzer.register_tables(vec![
            "t1".to_string(),
            "t2".to_string(),
            "t3".to_string(),
        ]);

        // Create chain: t1 - t2 - t3
        analyzer.add_edge(JoinEdge {
            left_table: "t1".to_string(),
            left_column: "id".to_string(),
            right_table: "t2".to_string(),
            right_column: "id".to_string(),
        });
        analyzer.add_edge(JoinEdge {
            left_table: "t2".to_string(),
            left_column: "id".to_string(),
            right_table: "t3".to_string(),
            right_column: "id".to_string(),
        });

        let search = JoinOrderSearch::from_analyzer(&analyzer);
        let order = search.find_optimal_order();

        assert_eq!(order.len(), 3);
        assert!(order.contains(&"t1".to_string()));
        assert!(order.contains(&"t2".to_string()));
        assert!(order.contains(&"t3".to_string()));
    }

    #[test]
    fn test_cost_comparison() {
        // Verify that cost model prefers chains with join edges
        let cost_with_edge = JoinCost::new(100, 1000);
        let cost_without_edge = JoinCost::new(500, 1000);

        assert!(cost_with_edge.total() < cost_without_edge.total());
    }

    #[test]
    fn test_search_prunes_bad_paths() {
        // Create scenario where different orderings have different costs
        let mut analyzer = JoinOrderAnalyzer::new();
        analyzer.register_tables(vec![
            "t1".to_string(),
            "t2".to_string(),
            "t3".to_string(),
        ]);

        // t1 - t2 - t3 chain
        analyzer.add_edge(JoinEdge {
            left_table: "t1".to_string(),
            left_column: "id".to_string(),
            right_table: "t2".to_string(),
            right_column: "id".to_string(),
        });
        analyzer.add_edge(JoinEdge {
            left_table: "t2".to_string(),
            left_column: "id".to_string(),
            right_table: "t3".to_string(),
            right_column: "id".to_string(),
        });

        let search = JoinOrderSearch::from_analyzer(&analyzer);
        let order = search.find_optimal_order();

        // Verify we get a valid ordering (all tables present)
        assert_eq!(order.len(), 3);
    }

    #[test]
    fn test_disconnected_tables() {
        // Tables with no join edges
        let mut analyzer = JoinOrderAnalyzer::new();
        analyzer.register_tables(vec![
            "t1".to_string(),
            "t2".to_string(),
            "t3".to_string(),
        ]);

        // No edges - will use cross product
        let search = JoinOrderSearch::from_analyzer(&analyzer);
        let order = search.find_optimal_order();

        // Still should return all tables in some order
        assert_eq!(order.len(), 3);
    }
}
