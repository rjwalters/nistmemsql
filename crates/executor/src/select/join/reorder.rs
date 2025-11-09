//! Join order optimization through reordering
//!
//! Analyzes equijoin predicates and determines optimal join orders to minimize
//! cartesian product accumulation. This implements Phase 3.2 of the join optimization.
//!
//! Key insight: Instead of executing joins in FROM clause order, we analyze the
//! equijoin graph to find chains where early tables have high selectivity,
//! preventing cascading intermediate results.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use ast::{Expression, BinaryOperator};

/// A join edge representing a condition between two tables
#[derive(Debug, Clone)]
pub struct JoinEdge {
    pub left_table: String,
    pub right_table: String,
    pub condition: Expression,
    pub selectivity: Selectivity,
}

/// Estimated selectivity of a join condition
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub enum Selectivity {
    /// Very restrictive (a = 5)
    VeryHigh,
    /// High selectivity (a = b where both are keys)
    High,
    /// Medium selectivity (a = b)
    Medium,
    /// Low selectivity (unrestricted join)
    Low,
}

impl Selectivity {
    /// Score for ordering (higher = better to do first)
    fn score(&self) -> i32 {
        match self {
            Selectivity::VeryHigh => 100,
            Selectivity::High => 75,
            Selectivity::Medium => 50,
            Selectivity::Low => 10,
        }
    }
}

/// Analyzes join predicates and determines optimal execution order
pub struct JoinOrderAnalyzer {
    equijoin_predicates: Vec<Expression>,
    local_filter_predicates: Vec<(String, Expression)>, // (table, condition)
    all_tables: HashSet<String>,
}

impl JoinOrderAnalyzer {
    pub fn new(
        equijoin_predicates: Vec<ast::Expression>,
        local_filter_predicates: Vec<(String, ast::Expression)>,
        all_tables: HashSet<String>,
    ) -> Self {
        Self {
            equijoin_predicates,
            local_filter_predicates,
            all_tables,
        }
    }

    /// Analyze join predicates and return optimal join order
    /// 
    /// Returns a sequence of join operations: (left_tables, right_table, condition)
    /// where each join combines the left side with one new table from the right
    pub fn find_optimal_order(&self) -> Vec<(HashSet<String>, String, Expression)> {
        // Build join graph
        let edges = self.extract_join_edges();
        
        if edges.is_empty() {
            // No equijoin predicates - use default left-to-right order
            return self.default_order();
        }

        // Find connected components (join chains)
        let components = self.find_join_chains(&edges);

        // Reorder tables based on selectivity
        self.reorder_by_selectivity(&edges, &components)
    }

    /// Extract join edges from equijoin predicates
    fn extract_join_edges(&self) -> Vec<JoinEdge> {
        let mut edges = Vec::new();

        for pred in &self.equijoin_predicates {
            if let Some((left_table, right_table, selectivity)) = self.analyze_equijoin(pred) {
                edges.push(JoinEdge {
                    left_table,
                    right_table,
                    condition: pred.clone(),
                    selectivity,
                });
            }
        }

        edges
    }

    /// Analyze a single equijoin predicate
    /// Returns (left_table, right_table, selectivity) if it's a simple equijoin
    fn analyze_equijoin(&self, expr: &Expression) -> Option<(String, String, Selectivity)> {
        if let Expression::BinaryOp { left, op: BinaryOperator::Equal, right } = expr {
            let (left_table, left_is_col) = self.extract_column_table(left)?;
            let (right_table, right_is_col) = self.extract_column_table(right)?;

            // Ensure we have two different tables
            if left_table == right_table {
                return None;
            }

            let selectivity = match (left_is_col, right_is_col) {
                (true, true) => Selectivity::Medium,  // a = b (both columns)
                (true, false) | (false, true) => Selectivity::High, // Column = constant
                (false, false) => Selectivity::VeryHigh, // constant = constant (unusual)
            };

            // Normalize order: alphabetically for consistency
            if left_table < right_table {
                Some((left_table, right_table, selectivity))
            } else {
                Some((right_table, left_table, selectivity))
            }
        } else {
            None
        }
    }

    /// Extract table reference from an expression
    /// Returns (table_name, is_column) where is_column indicates if it's a column or constant
    fn extract_column_table(&self, expr: &Expression) -> Option<(String, bool)> {
        match expr {
            Expression::ColumnRef { table: Some(table), .. } => {
                Some((table.clone(), true))
            }
            Expression::ColumnRef { table: None, column: _ } => {
                // Unqualified column - try to find which table it belongs to
                // For now, we'll be conservative and skip these
                None
            }
            // Any other expression is a constant/expression
            _ => None,
        }
    }

    /// Find join chains - connected components where tables are linked by equijoins
    fn find_join_chains(&self, edges: &[JoinEdge]) -> Vec<Vec<String>> {
        let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();
        let mut visited = HashSet::new();
        let mut components = Vec::new();

        // Build adjacency list
        for edge in edges {
            adjacency
                .entry(edge.left_table.clone())
                .or_insert_with(Vec::new)
                .push(edge.right_table.clone());
            adjacency
                .entry(edge.right_table.clone())
                .or_insert_with(Vec::new)
                .push(edge.left_table.clone());
        }

        // Find connected components via DFS
        for table in &self.all_tables {
            if !visited.contains(table) {
                let mut component = Vec::new();
                self.dfs(table, &adjacency, &mut visited, &mut component);
                components.push(component);
            }
        }

        components
    }

    /// Depth-first search to find connected component
    fn dfs(
        &self,
        node: &str,
        adjacency: &HashMap<String, Vec<String>>,
        visited: &mut HashSet<String>,
        component: &mut Vec<String>,
    ) {
        visited.insert(node.to_string());
        component.push(node.to_string());

        if let Some(neighbors) = adjacency.get(node) {
            for neighbor in neighbors {
                if !visited.contains(neighbor) {
                    self.dfs(neighbor, adjacency, visited, component);
                }
            }
        }
    }

    /// Reorder tables within each component based on selectivity
    fn reorder_by_selectivity(
        &self,
        edges: &[JoinEdge],
        components: &[Vec<String>],
    ) -> Vec<(HashSet<String>, String, Expression)> {
        let mut result = Vec::new();

        for component in components {
            let ordered = self.order_component(component, edges);
            
            // Convert to (left_set, right_table, condition) tuples
            let mut left_set = HashSet::new();
            
            for i in 0..ordered.len() {
                if i == 0 {
                    // First table stands alone
                    left_set.insert(ordered[0].clone());
                } else {
                    // Find edge between left_set and this table
                    let right_table = &ordered[i];
                    
                    if let Some(edge) = self.find_connecting_edge(&left_set, right_table, edges) {
                        result.push((left_set.clone(), right_table.clone(), edge.condition.clone()));
                        left_set.insert(right_table.clone());
                    }
                }
            }
        }

        result
    }

    /// Order tables within a component
    fn order_component(&self, component: &[String], edges: &[JoinEdge]) -> Vec<String> {
        // Start with table that has highest local filter selectivity
        let mut ordered = Vec::new();
        let mut remaining: HashSet<_> = component.iter().cloned().collect();

        // Find starting table (highest local selectivity)
        let start = self.find_best_start(&remaining);
        ordered.push(start.clone());
        remaining.remove(&start);

        // Greedily add tables that connect to already-ordered tables
        while !remaining.is_empty() {
            let best_next = remaining
                .iter()
                .max_by_key(|table| {
                    self.calculate_table_score(table, &ordered, edges)
                })
                .cloned();

            if let Some(next) = best_next {
                ordered.push(next.clone());
                remaining.remove(&next);
            } else {
                // No more connections - add remaining (shouldn't happen in connected component)
                ordered.extend(remaining.iter().cloned());
                break;
            }
        }

        ordered
    }

    /// Find the best starting table (highest local filter selectivity)
    fn find_best_start(&self, tables: &HashSet<String>) -> String {
        tables
            .iter()
            .max_by_key(|table| {
                self.local_filter_predicates
                    .iter()
                    .filter(|(t, _)| t == *table)
                    .count()
            })
            .cloned()
            .unwrap_or_else(|| tables.iter().next().unwrap().clone())
    }

    /// Calculate join quality score for a table relative to already-ordered tables
    fn calculate_table_score(&self, table: &str, ordered: &[String], edges: &[JoinEdge]) -> i32 {
        let mut score = 0;

        // Bonus for edges connecting to already-ordered tables
        for edge in edges {
            if edge.left_table == table && ordered.contains(&edge.right_table)
                || edge.right_table == table && ordered.contains(&edge.left_table)
            {
                score += edge.selectivity.score();
            }
        }

        score
    }

    /// Find edge connecting a set of tables to a new table
    fn find_connecting_edge<'a>(
        &self,
        left_set: &HashSet<String>,
        right_table: &str,
        edges: &'a [JoinEdge],
    ) -> Option<&'a JoinEdge> {
        edges.iter().find(|edge| {
            (left_set.contains(&edge.left_table) && edge.right_table == right_table)
                || (left_set.contains(&edge.right_table) && edge.left_table == right_table)
        })
    }

    /// Default ordering when no join chains found - left-to-right
    fn default_order(&self) -> Vec<(HashSet<String>, String, ast::Expression)> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_chain_detection() {
        // Build a simple chain: t1 - t2 - t3
        let tables = vec!["t1".to_string(), "t2".to_string(), "t3".to_string()]
            .into_iter()
            .collect();
        let analyzer = JoinOrderAnalyzer::new(vec![], vec![], tables);

        let null_cond = Expression::Literal(SqlValue::Null);
        let edges = vec![
            JoinEdge {
                left_table: "t1".to_string(),
                right_table: "t2".to_string(),
                condition: null_cond.clone(),
                selectivity: Selectivity::Medium,
            },
            JoinEdge {
                left_table: "t2".to_string(),
                right_table: "t3".to_string(),
                condition: null_cond,
                selectivity: Selectivity::Medium,
            },
        ];

        let components = analyzer.find_join_chains(&edges);
        assert_eq!(components.len(), 1);
        assert_eq!(components[0].len(), 3);
    }

    #[test]
    fn test_multiple_components() {
        // Two separate chains: t1-t2 and t3-t4
        let tables = vec!["t1", "t2", "t3", "t4"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let analyzer = JoinOrderAnalyzer::new(vec![], vec![], tables);

        let null_cond = Expression::Literal(SqlValue::Null);
        let edges = vec![
            JoinEdge {
                left_table: "t1".to_string(),
                right_table: "t2".to_string(),
                condition: null_cond.clone(),
                selectivity: Selectivity::Medium,
            },
            JoinEdge {
                left_table: "t3".to_string(),
                right_table: "t4".to_string(),
                condition: null_cond,
                selectivity: Selectivity::Medium,
            },
        ];

        let components = analyzer.find_join_chains(&edges);
        assert_eq!(components.len(), 2);
    }

    #[test]
    fn test_selectivity_scoring() {
        // Verify selectivity scoring works correctly
        assert!(Selectivity::VeryHigh.score() > Selectivity::High.score());
        assert!(Selectivity::High.score() > Selectivity::Medium.score());
        assert!(Selectivity::Medium.score() > Selectivity::Low.score());
    }

    #[test]
    fn test_empty_equijoins() {
        // With no equijoin predicates, should return default order (empty)
        let tables = vec!["t1".to_string(), "t2".to_string()]
            .into_iter()
            .collect();
        let analyzer = JoinOrderAnalyzer::new(vec![], vec![], tables);
        
        let result = analyzer.find_optimal_order();
        assert_eq!(result.len(), 0);
    }
}
