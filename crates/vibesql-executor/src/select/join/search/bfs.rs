//! Parallel breadth-first search strategy
//!
//! This module implements the parallel BFS strategy for join order optimization.
//! It explores all candidate orderings at each depth level using parallel iteration,
//! with intelligent pruning to manage memory usage.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "parallel")]
use rayon::prelude::*;

use super::{JoinCost, JoinOrderContext, SearchState};

impl JoinOrderContext {
    /// Find optimal order using parallel BFS
    ///
    /// Explores all candidate orderings at each depth level using parallel iteration,
    /// with intelligent pruning to manage memory usage.
    pub(super) fn find_optimal_order_parallel(&self) -> Vec<String> {
        // Initial state: empty set of joined tables
        let initial_state = SearchState {
            joined_tables: BTreeSet::new(),
            cost_so_far: JoinCost::new(0, 0),
            order: Vec::new(),
            current_cardinality: 0,
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
        // Filter to unjoined tables
        let mut candidates: Vec<&String> = self.all_tables
            .iter()
            .filter(|t| !state.joined_tables.contains(*t))
            .collect();

        // Filter to connected candidates (unless this is the first table)
        if !state.joined_tables.is_empty() {
            let connected: Vec<&String> = candidates
                .iter()
                .filter(|t| self.has_join_edge(&state.joined_tables, t))
                .copied()
                .collect();

            // Only use connected candidates if any exist (avoid CROSS JOINs)
            if !connected.is_empty() {
                candidates = connected;
            }
        }

        candidates
            .into_iter()
            .filter_map(|next_table| {
                // Estimate cost of joining this table (using current intermediate result size)
                let join_cost = self.estimate_join_cost(state.current_cardinality, &state.joined_tables, next_table);
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
                // Update current cardinality to the result of this join
                new_state.current_cardinality = join_cost.cardinality;

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
