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

mod bfs;
mod cost;
mod dfs;
mod greedy;

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
        // Use saturating arithmetic to prevent overflow with large intermediate results
        (self.cardinality as u64).saturating_mul(1000).saturating_add(self.operations)
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
            max_depth: 8, // Support 8-way joins like TPC-H Q8
            max_states_per_layer: 1000,
            pruning_threshold: 1.5,
        }
    }
}

/// State during join order search
#[derive(Debug, Clone)]
pub(super) struct SearchState {
    /// Tables already joined
    pub joined_tables: HashSet<String>,
    /// Cumulative cost so far
    pub cost_so_far: JoinCost,
    /// Ordering of tables
    pub order: Vec<String>,
    /// Current intermediate result size (rows after all joins so far)
    pub current_cardinality: usize,
}

/// Context for join order search operations
///
/// Holds shared state and configuration used by all search strategies.
/// This internal struct encapsulates data needed by cost estimation,
/// DFS, BFS, and greedy algorithms.
pub(super) struct JoinOrderContext {
    /// All tables in the query
    pub all_tables: HashSet<String>,
    /// Join edges (which tables connect)
    pub edges: Vec<JoinEdge>,
    /// Estimated rows for each table after local filters
    pub table_cardinalities: std::collections::HashMap<String, usize>,
    /// Selectivity for each join edge based on column NDV (number of distinct values)
    /// Key is (left_table, right_table) normalized to lowercase
    pub edge_selectivities: std::collections::HashMap<(String, String), f64>,
    /// Configuration for parallel search
    pub config: ParallelSearchConfig,
}

/// Performs join order optimization via exhaustive search
pub struct JoinOrderSearch {
    context: JoinOrderContext,
}

impl JoinOrderSearch {
    /// Create a new join order search from an analyzer with real table statistics
    pub fn from_analyzer(
        analyzer: &JoinOrderAnalyzer,
        database: &vibesql_storage::Database,
    ) -> Self {
        Self::from_analyzer_with_predicates(analyzer, database, &std::collections::HashMap::new())
    }

    /// Create a new join order search with WHERE clause selectivity applied
    ///
    /// This version accounts for table-local predicates when estimating cardinalities,
    /// which helps choose better join orders for queries like TPC-H Q3 where filter
    /// predicates significantly reduce table sizes before joining.
    pub fn from_analyzer_with_predicates(
        analyzer: &JoinOrderAnalyzer,
        database: &vibesql_storage::Database,
        table_local_predicates: &std::collections::HashMap<String, Vec<vibesql_ast::Expression>>,
    ) -> Self {
        let edges = analyzer.edges().to_vec();
        let edge_selectivities = JoinOrderContext::compute_edge_selectivities(&edges, database);

        let context = JoinOrderContext {
            all_tables: analyzer.tables().clone(),
            edges,
            table_cardinalities: JoinOrderContext::extract_cardinalities_with_selectivity(
                analyzer,
                database,
                table_local_predicates,
            ),
            edge_selectivities,
            config: ParallelSearchConfig::default(),
        };

        Self { context }
    }

    /// Find optimal join order by exploring search space
    ///
    /// Returns list of table names in the order they should be joined.
    /// Uses adaptive strategy selection: parallel BFS for 3-6 table queries with
    /// highly connected join graphs, DFS for small or large queries.
    pub fn find_optimal_order(&self) -> Vec<String> {
        if self.context.all_tables.is_empty() {
            return Vec::new();
        }

        // Use adaptive strategy selection
        if self.should_use_parallel_search() {
            self.context.find_optimal_order_parallel()
        } else {
            self.context.find_optimal_order_dfs()
        }
    }

    /// Determine whether to use parallel BFS or sequential DFS
    fn should_use_parallel_search(&self) -> bool {
        // Don't parallelize if disabled
        if !self.context.config.enabled {
            return false;
        }

        let num_tables = self.context.all_tables.len();

        // Don't parallelize small queries (< 3 tables)
        if num_tables < 3 {
            return false;
        }

        // Don't parallelize beyond depth limit (memory constraints)
        if num_tables > self.context.config.max_depth {
            return false;
        }

        // Parallel BFS beneficial for highly connected graphs
        // Calculate edge density: edges per table
        let edge_density = self.context.edges.len() as f64 / num_tables as f64;

        // High edge density suggests complex join graph → parallel beneficial
        // Threshold of 1.5 means we need at least 1-2 edges per table
        edge_density >= 1.5
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
        let order_parallel = search.context.find_optimal_order_parallel();
        let order_dfs = search.context.find_optimal_order_dfs();

        // Both should contain all tables
        assert_eq!(order_parallel.len(), 4);
        assert_eq!(order_dfs.len(), 4);

        // Both should be valid orderings
        for table in &["t1", "t2", "t3", "t4"] {
            assert!(order_parallel.contains(&table.to_string()));
            assert!(order_dfs.contains(&table.to_string()));
        }
    }

    #[test]
    fn test_tpch_q3_star_schema_no_cross_join() {
        // Regression test for issue #2286: Join reordering optimizer chooses invalid
        // orders for star-schema queries, causing CROSS JOIN memory limits
        //
        // TPC-H Q3 has a star join pattern:
        //   customer ←→ orders ←→ lineitem
        //
        // Orders is the hub table. Customer and lineitem have NO direct join.
        // The optimizer should never choose an order like [customer, lineitem, orders]
        // which would require a CROSS JOIN between customer and lineitem.
        let mut analyzer = JoinOrderAnalyzer::new();
        analyzer.register_tables(vec![
            "customer".to_string(),
            "orders".to_string(),
            "lineitem".to_string(),
        ]);

        // Star pattern: orders is the hub
        analyzer.add_edge(JoinEdge {
            left_table: "customer".to_string(),
            left_column: "c_custkey".to_string(),
            right_table: "orders".to_string(),
            right_column: "o_custkey".to_string(),
        });
        analyzer.add_edge(JoinEdge {
            left_table: "lineitem".to_string(),
            left_column: "l_orderkey".to_string(),
            right_table: "orders".to_string(),
            right_column: "o_orderkey".to_string(),
        });

        let db = vibesql_storage::Database::new();
        let search = JoinOrderSearch::from_analyzer(&analyzer, &db);
        let order = search.find_optimal_order();

        // Verify we got all 3 tables
        assert_eq!(order.len(), 3);
        assert!(order.contains(&"customer".to_string()));
        assert!(order.contains(&"orders".to_string()));
        assert!(order.contains(&"lineitem".to_string()));

        // Validate: each table after the first must have a join edge to at least
        // one previously-joined table (no CROSS JOINs)
        for i in 1..order.len() {
            let current_table = &order[i];
            let previous_tables: HashSet<String> = order[0..i].iter().cloned().collect();

            let has_connection = search.context.has_join_edge(&previous_tables, current_table);

            assert!(
                has_connection,
                "Table {} at position {} has no join condition with previous tables {:?}. \
                 This would cause a CROSS JOIN and memory limit exceeded. \
                 Full order: {:?}",
                current_table, i, previous_tables, order
            );
        }

        // Note: customer and lineitem CAN be adjacent in orders like [orders, lineitem, customer]
        // because both connect to orders. The key is that each table after the first has
        // a connection to at least one previously-joined table, which we verified above.
    }
}
