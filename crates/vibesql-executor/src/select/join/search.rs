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
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "parallel")]
use rayon::prelude::*;

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

/// Configuration for parallel join order search
#[derive(Debug, Clone)]
pub struct ParallelSearchConfig {
    /// Enable parallel BFS search (vs sequential DFS)
    pub enabled: bool,
    /// Maximum depth to explore with parallel BFS (tables with >max_depth use DFS)
    pub max_depth: usize,
    /// Maximum states per layer before pruning
    pub max_states_per_layer: usize,
    /// Prune states with cost > best * threshold
    pub pruning_threshold: f64,
}

impl Default for ParallelSearchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_depth: 6,
            max_states_per_layer: 1000,
            pruning_threshold: 1.5,
        }
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
    /// Configuration for parallel search
    config: ParallelSearchConfig,
}

impl JoinOrderSearch {
    /// Create a new join order search from an analyzer with real table statistics
    pub fn from_analyzer(
        analyzer: &JoinOrderAnalyzer,
        database: &vibesql_storage::Database,
    ) -> Self {
        Self {
            all_tables: analyzer.tables().clone(),
            edges: analyzer.edges().to_vec(),
            table_cardinalities: Self::extract_cardinalities(analyzer, database),
            config: ParallelSearchConfig::default(),
        }
    }

    /// Extract table cardinalities from actual table statistics
    ///
    /// Uses real row counts from database tables instead of hardcoded estimates.
    /// This enables effective pruning in the search algorithm.
    fn extract_cardinalities(
        analyzer: &JoinOrderAnalyzer,
        database: &vibesql_storage::Database,
    ) -> std::collections::HashMap<String, usize> {
        let mut cardinalities = std::collections::HashMap::new();

        for table_name in analyzer.tables() {
            // Get actual table row count from database
            let actual_rows = database
                .get_table(table_name.as_str())
                .map(|t| t.row_count())
                .unwrap_or(10000); // Fallback for CTEs/subqueries

            cardinalities.insert(table_name.clone(), actual_rows);
        }

        cardinalities
    }

    /// Find optimal join order by exploring search space
    ///
    /// Returns list of table names in the order they should be joined.
    /// Uses adaptive strategy selection: parallel BFS for 3-6 table queries with
    /// highly connected join graphs, DFS for small or large queries.
    pub fn find_optimal_order(&self) -> Vec<String> {
        if self.all_tables.is_empty() {
            return Vec::new();
        }

        // Use adaptive strategy selection
        if self.should_use_parallel_search() {
            self.find_optimal_order_parallel()
        } else {
            self.find_optimal_order_dfs()
        }
    }

    /// Determine whether to use parallel BFS or sequential DFS
    fn should_use_parallel_search(&self) -> bool {
        // Don't parallelize if disabled
        if !self.config.enabled {
            return false;
        }

        let num_tables = self.all_tables.len();

        // Don't parallelize small queries (< 3 tables)
        if num_tables < 3 {
            return false;
        }

        // Don't parallelize beyond depth limit (memory constraints)
        if num_tables > self.config.max_depth {
            return false;
        }

        // Parallel BFS beneficial for highly connected graphs
        // Calculate edge density: edges per table
        let edge_density = self.edges.len() as f64 / num_tables as f64;

        // High edge density suggests complex join graph → parallel beneficial
        // Threshold of 1.5 means we need at least 1-2 edges per table
        edge_density >= 1.5
    }

    /// Find optimal order using sequential DFS (original algorithm)
    fn find_optimal_order_dfs(&self) -> Vec<String> {
        let num_tables = self.all_tables.len();

        // For large queries (>= 8 tables), use greedy heuristic instead of exhaustive search
        // Exhaustive search becomes prohibitively expensive:
        // 8 tables: 40,320 permutations, 10 tables: 3,628,800, 18 tables: 6.4e15
        // Even with pruning, we hit iteration limits and get poor results
        if num_tables >= 8 {
            return self.find_optimal_order_greedy();
        }

        let initial_state = SearchState {
            joined_tables: HashSet::new(),
            cost_so_far: JoinCost::new(0, 0),
            order: Vec::new(),
        };

        let mut best_cost = u64::MAX;
        let mut best_order = vec![];
        let mut iterations = 0;

        // Maximum iterations to prevent pathological cases
        // For n tables, factorial complexity means:
        // 3 tables: 6 iterations, 4 tables: 24, 5 tables: 120, 6 tables: 720, 7 tables: 5,040
        // Cap at 10000 to allow more exploration for 7-table queries
        let max_iterations = 10000;

        self.search_recursive(
            initial_state,
            &mut best_cost,
            &mut best_order,
            &mut iterations,
            max_iterations,
        );

        // If we hit iteration limit without finding a complete ordering, fall back to greedy
        if best_order.is_empty() {
            return self.find_optimal_order_greedy();
        }

        best_order
    }

    /// Recursive depth-first search with pruning
    fn search_recursive(
        &self,
        state: SearchState,
        best_cost: &mut u64,
        best_order: &mut Vec<String>,
        iterations: &mut u32,
        max_iterations: u32,
    ) {
        // Early termination: check iteration limit
        *iterations += 1;
        if *iterations > max_iterations {
            return;
        }

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
            self.search_recursive(next_state, best_cost, best_order, iterations, max_iterations);
        }
    }

    /// Estimate cost of joining next_table to already-joined tables
    fn estimate_join_cost(&self, joined_tables: &HashSet<String>, next_table: &str) -> JoinCost {
        if joined_tables.is_empty() {
            // First table: just a scan with selectivity
            let cardinality = self.table_cardinalities.get(next_table).copied().unwrap_or(10000);
            return JoinCost::new(cardinality, 0);
        }

        // Estimate cardinality of joined result
        let left_cardinality: usize =
            joined_tables.iter().filter_map(|t| self.table_cardinalities.get(t)).sum();

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
    fn find_optimal_order_greedy(&self) -> Vec<String> {
        if self.all_tables.is_empty() {
            return Vec::new();
        }

        let mut joined_tables = HashSet::new();
        let mut remaining_tables: HashSet<String> = self.all_tables.clone();
        let mut join_order = Vec::new();

        // Step 1: Start with the smallest table (lowest cardinality)
        let first_table = remaining_tables
            .iter()
            .min_by_key(|table| self.table_cardinalities.get(*table).copied().unwrap_or(10000))
            .unwrap()
            .clone();

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
                let cost = self.estimate_join_cost(&joined_tables, candidate);

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
            } else {
                // Shouldn't happen, but handle gracefully
                break;
            }
        }

        join_order
    }

    /// Find optimal order using parallel BFS
    ///
    /// Explores all candidate orderings at each depth level using parallel iteration,
    /// with intelligent pruning to manage memory usage.
    fn find_optimal_order_parallel(&self) -> Vec<String> {
        // Initial state: empty set of joined tables
        let initial_state = SearchState {
            joined_tables: HashSet::new(),
            cost_so_far: JoinCost::new(0, 0),
            order: Vec::new(),
        };

        let mut current_layer = vec![initial_state];
        let best_cost = AtomicU64::new(u64::MAX);
        let mut best_order = vec![];

        // Iterate through depths (number of tables joined)
        for _depth in 0..self.all_tables.len() {
            if current_layer.is_empty() {
                break; // No more paths to explore
            }

            // Generate next layer in parallel
            let best_cost_snapshot = best_cost.load(Ordering::Relaxed);
            let next_layer: Vec<SearchState> = {
                #[cfg(feature = "parallel")]
                {
                    current_layer
                        .into_par_iter()
                        .flat_map(|state| self.expand_state_parallel(&state, best_cost_snapshot))
                        .collect()
                }
                #[cfg(not(feature = "parallel"))]
                {
                    current_layer
                        .into_iter()
                        .flat_map(|state| self.expand_state_parallel(&state, best_cost_snapshot))
                        .collect()
                }
            };

            // Prune layer to prevent memory explosion
            current_layer = self.prune_layer(next_layer, &best_cost, &mut best_order);
        }

        // If no solution found, return left-to-right ordering as fallback
        if best_order.is_empty() {
            return self.all_tables.iter().cloned().collect();
        }

        best_order
    }

    /// Expand a single state by trying all unjoined tables
    ///
    /// Returns a vector of next states, each representing adding one more table
    /// to the current join sequence. Prunes states that exceed the best known cost.
    fn expand_state_parallel(&self, state: &SearchState, best_cost: u64) -> Vec<SearchState> {
        self.all_tables
            .iter()
            .filter(|t| !state.joined_tables.contains(*t))
            .filter_map(|next_table| {
                // Estimate cost of joining this table
                let join_cost = self.estimate_join_cost(&state.joined_tables, next_table);
                let new_cost = JoinCost::new(
                    state.cost_so_far.cardinality + join_cost.cardinality,
                    state.cost_so_far.operations + join_cost.operations,
                );

                // Prune if cost exceeds best
                if new_cost.total() >= best_cost {
                    return None;
                }

                // Create new state
                let mut new_state = state.clone();
                new_state.joined_tables.insert(next_table.clone());
                new_state.cost_so_far = new_cost;
                new_state.order.push(next_table.clone());

                Some(new_state)
            })
            .collect()
    }

    /// Prune layer to prevent memory explosion
    ///
    /// Updates best solution from complete orderings, removes complete orderings
    /// from the layer, prunes states with poor cost, and limits layer size.
    fn prune_layer(
        &self,
        mut layer: Vec<SearchState>,
        best_cost: &AtomicU64,
        best_order: &mut Vec<String>,
    ) -> Vec<SearchState> {
        // Update best solution from complete orderings
        for state in &layer {
            if state.joined_tables.len() == self.all_tables.len() {
                let cost = state.cost_so_far.total();
                let current_best = best_cost.load(Ordering::Relaxed);

                // Try to update best cost atomically
                if cost < current_best {
                    // Compare-and-swap loop to handle concurrent updates
                    let mut current = current_best;
                    loop {
                        match best_cost.compare_exchange_weak(
                            current,
                            cost,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => {
                                // Successfully updated best cost
                                *best_order = state.order.clone();
                                break;
                            }
                            Err(actual) => {
                                // Another thread updated, check if we're still better
                                if cost >= actual {
                                    break; // No longer the best
                                }
                                current = actual;
                            }
                        }
                    }
                }
            }
        }

        // Remove complete orderings (no need to expand further)
        layer.retain(|s| s.joined_tables.len() < self.all_tables.len());

        // Prune states with poor cost relative to best
        let current_best = best_cost.load(Ordering::Relaxed);
        if current_best < u64::MAX {
            let threshold_cost = (current_best as f64 * self.config.pruning_threshold) as u64;
            layer.retain(|s| s.cost_so_far.total() < threshold_cost);
        }

        // Limit layer size to prevent memory issues
        if layer.len() > self.config.max_states_per_layer {
            // Sort by cost and keep best N states
            layer.sort_by_key(|s| s.cost_so_far.total());
            layer.truncate(self.config.max_states_per_layer);
        }

        layer
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

        let db = vibesql_storage::Database::new();
        let search = JoinOrderSearch::from_analyzer(&analyzer, &db);
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

        let db = vibesql_storage::Database::new();
        let search = JoinOrderSearch::from_analyzer(&analyzer, &db);
        let order = search.find_optimal_order();

        assert_eq!(order.len(), 2);
        // Both orderings are valid, just verify both tables are present
        assert!(order.contains(&"t1".to_string()));
        assert!(order.contains(&"t2".to_string()));
    }

    #[test]
    fn test_three_table_chain() {
        let mut analyzer = JoinOrderAnalyzer::new();
        analyzer.register_tables(vec!["t1".to_string(), "t2".to_string(), "t3".to_string()]);

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

        let db = vibesql_storage::Database::new();
        let search = JoinOrderSearch::from_analyzer(&analyzer, &db);
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
        analyzer.register_tables(vec!["t1".to_string(), "t2".to_string(), "t3".to_string()]);

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

        let db = vibesql_storage::Database::new();
        let search = JoinOrderSearch::from_analyzer(&analyzer, &db);
        let order = search.find_optimal_order();

        // Verify we get a valid ordering (all tables present)
        assert_eq!(order.len(), 3);
    }

    #[test]
    fn test_disconnected_tables() {
        // Tables with no join edges
        let mut analyzer = JoinOrderAnalyzer::new();
        analyzer.register_tables(vec!["t1".to_string(), "t2".to_string(), "t3".to_string()]);

        // No edges - will use cross product
        let db = vibesql_storage::Database::new();
        let search = JoinOrderSearch::from_analyzer(&analyzer, &db);
        let order = search.find_optimal_order();

        // Still should return all tables in some order
        assert_eq!(order.len(), 3);
    }

    #[test]
    fn test_star_join_pattern() {
        // Test star join: t1 is central hub, t2/t3/t4 all join to t1
        // This pattern exposed the bug where we only looked for conditions
        // between consecutive tables in the reordered sequence
        let mut analyzer = JoinOrderAnalyzer::new();
        analyzer.register_tables(vec![
            "t1".to_string(),
            "t2".to_string(),
            "t3".to_string(),
            "t4".to_string(),
        ]);

        // Star pattern: all join to t1 (but not to each other)
        //     t2
        //      |
        //  t3--t1--t4
        analyzer.add_edge(JoinEdge {
            left_table: "t1".to_string(),
            left_column: "id".to_string(),
            right_table: "t2".to_string(),
            right_column: "id".to_string(),
        });
        analyzer.add_edge(JoinEdge {
            left_table: "t1".to_string(),
            left_column: "id".to_string(),
            right_table: "t3".to_string(),
            right_column: "id".to_string(),
        });
        analyzer.add_edge(JoinEdge {
            left_table: "t1".to_string(),
            left_column: "id".to_string(),
            right_table: "t4".to_string(),
            right_column: "id".to_string(),
        });

        let db = vibesql_storage::Database::new();
        let search = JoinOrderSearch::from_analyzer(&analyzer, &db);
        let order = search.find_optimal_order();

        // Should return all 4 tables
        assert_eq!(order.len(), 4);
        assert!(order.contains(&"t1".to_string()));
        assert!(order.contains(&"t2".to_string()));
        assert!(order.contains(&"t3".to_string()));
        assert!(order.contains(&"t4".to_string()));

        // t1 should be early in the order since it's the hub that connects everything
        // (though exact position depends on cost estimates)
        let t1_pos = order.iter().position(|t| t == "t1").unwrap();

        // t1 should be in the first 2 positions (either first or second)
        // because it's the only table that can join to all others
        assert!(
            t1_pos <= 1,
            "Hub table t1 should be early in join order, found at position {}",
            t1_pos
        );
    }

    #[test]
    fn test_parallel_bfs_selection() {
        // Test that should_use_parallel_search selects correctly
        let mut analyzer = JoinOrderAnalyzer::new();

        // Test 1: Small query (2 tables) should use DFS
        analyzer.register_tables(vec!["t1".to_string(), "t2".to_string()]);
        analyzer.add_edge(JoinEdge {
            left_table: "t1".to_string(),
            left_column: "id".to_string(),
            right_table: "t2".to_string(),
            right_column: "id".to_string(),
        });

        let db = vibesql_storage::Database::new();
        let search = JoinOrderSearch::from_analyzer(&analyzer, &db);
        assert!(!search.should_use_parallel_search(), "Small queries should use DFS");

        // Test 2: Highly connected 5-table query should use parallel BFS
        let mut analyzer = JoinOrderAnalyzer::new();
        analyzer.register_tables(vec![
            "t1".to_string(),
            "t2".to_string(),
            "t3".to_string(),
            "t4".to_string(),
            "t5".to_string(),
        ]);

        // Create a highly connected graph (star + additional edges)
        // 8 edges / 5 tables = 1.6 edge density > 1.5 threshold
        analyzer.add_edge(JoinEdge {
            left_table: "t1".to_string(),
            left_column: "id".to_string(),
            right_table: "t2".to_string(),
            right_column: "id".to_string(),
        });
        analyzer.add_edge(JoinEdge {
            left_table: "t1".to_string(),
            left_column: "id".to_string(),
            right_table: "t3".to_string(),
            right_column: "id".to_string(),
        });
        analyzer.add_edge(JoinEdge {
            left_table: "t1".to_string(),
            left_column: "id".to_string(),
            right_table: "t4".to_string(),
            right_column: "id".to_string(),
        });
        analyzer.add_edge(JoinEdge {
            left_table: "t1".to_string(),
            left_column: "id".to_string(),
            right_table: "t5".to_string(),
            right_column: "id".to_string(),
        });
        analyzer.add_edge(JoinEdge {
            left_table: "t2".to_string(),
            left_column: "id".to_string(),
            right_table: "t3".to_string(),
            right_column: "id".to_string(),
        });
        analyzer.add_edge(JoinEdge {
            left_table: "t3".to_string(),
            left_column: "id".to_string(),
            right_table: "t4".to_string(),
            right_column: "id".to_string(),
        });
        analyzer.add_edge(JoinEdge {
            left_table: "t4".to_string(),
            left_column: "id".to_string(),
            right_table: "t5".to_string(),
            right_column: "id".to_string(),
        });
        analyzer.add_edge(JoinEdge {
            left_table: "t2".to_string(),
            left_column: "id".to_string(),
            right_table: "t5".to_string(),
            right_column: "id".to_string(),
        });

        let search = JoinOrderSearch::from_analyzer(&analyzer, &db);
        assert!(
            search.should_use_parallel_search(),
            "Highly connected 5-table query should use parallel BFS"
        );

        // Verify it produces valid ordering
        let order = search.find_optimal_order();
        assert_eq!(order.len(), 5);
    }

    #[test]
    fn test_parallel_bfs_produces_valid_ordering() {
        // Test that parallel BFS produces a valid ordering
        let mut analyzer = JoinOrderAnalyzer::new();
        analyzer.register_tables(vec![
            "t1".to_string(),
            "t2".to_string(),
            "t3".to_string(),
            "t4".to_string(),
        ]);

        // Star pattern with 3 edges (3/4 = 0.75, below threshold)
        // Force parallel BFS by adding more edges
        analyzer.add_edge(JoinEdge {
            left_table: "t1".to_string(),
            left_column: "id".to_string(),
            right_table: "t2".to_string(),
            right_column: "id".to_string(),
        });
        analyzer.add_edge(JoinEdge {
            left_table: "t1".to_string(),
            left_column: "id".to_string(),
            right_table: "t3".to_string(),
            right_column: "id".to_string(),
        });
        analyzer.add_edge(JoinEdge {
            left_table: "t1".to_string(),
            left_column: "id".to_string(),
            right_table: "t4".to_string(),
            right_column: "id".to_string(),
        });
        analyzer.add_edge(JoinEdge {
            left_table: "t2".to_string(),
            left_column: "id".to_string(),
            right_table: "t3".to_string(),
            right_column: "id".to_string(),
        });
        analyzer.add_edge(JoinEdge {
            left_table: "t3".to_string(),
            left_column: "id".to_string(),
            right_table: "t4".to_string(),
            right_column: "id".to_string(),
        });
        analyzer.add_edge(JoinEdge {
            left_table: "t2".to_string(),
            left_column: "id".to_string(),
            right_table: "t4".to_string(),
            right_column: "id".to_string(),
        });

        let db = vibesql_storage::Database::new();
        let search = JoinOrderSearch::from_analyzer(&analyzer, &db);

        // Verify parallel BFS is selected
        assert!(search.should_use_parallel_search());

        // Test both DFS and parallel BFS produce valid orderings
        let order_parallel = search.find_optimal_order_parallel();
        let order_dfs = search.find_optimal_order_dfs();

        // Both should contain all tables
        assert_eq!(order_parallel.len(), 4);
        assert_eq!(order_dfs.len(), 4);

        // Both should be valid orderings
        for table in &["t1", "t2", "t3", "t4"] {
            assert!(order_parallel.contains(&table.to_string()));
            assert!(order_dfs.contains(&table.to_string()));
        }
    }
}
