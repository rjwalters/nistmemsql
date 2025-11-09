# Join Reordering via Search Over Possibilities

**Date**: 2025-11-08  
**Phase**: 3.2.2 - Join Reordering  
**Concept**: Transform join reordering into a graph/tree search problem

## Core Insight

Instead of using greedy heuristics to pick the next table to join, we can:
1. **Model join orderings as a search space** (tree of possibilities)
2. **Use cost estimation** to prune unpromising branches
3. **Find optimal ordering** through exhaustive or branch-and-bound search

This is similar to how modern optimizers (PostgreSQL, SQLite, DuckDB) work.

## Problem with Current Approach

Current `JoinOrderAnalyzer` uses greedy heuristics:
- Start with most selective table
- Greedily follow edges to next table
- No backtracking, no cost comparison

**Issue**: For complex join graphs, greedy is suboptimal. Example:
```
t1 (1000 rows) -- filter to 100
t2 (100 rows)  -- filter to 50
t3 (1M rows)

Greedy:  t1 (filter) → join t2 → join t3 = 100 × 100 × 1M = 10B ops
Optimal: t2 → join t1 (filter) → join t3 = 50 × 100 × 1M = 5B ops
```

If greedy follows t1 edge first (even though t2 is smaller after filtering), we get wrong order.

## Solution: Tree Search for Join Ordering

### 1. Problem Formulation

A join ordering problem can be represented as:
- **State**: Set of joined tables {T1, T2, ..., Tk}
- **Actions**: Add next unjoined table T
- **Goal**: Join all tables in order that minimizes cost

### 2. Search Space Structure

```
                    {}
                    /
           /--------+--------\
          /         |         \
       {T1}      {T2}       {T3}
       /  \      /  \       /  \
   {T1,T2} {T1,T3} {T2,T1} {T2,T3} {T3,T1} {T3,T2}
      |       |      |       |      |        |
   {T1,T2,T3} ...
```

Each path represents a different join order.

### 3. Search Strategies

#### Option A: Dynamic Programming (Optimal)
- **Time**: O(3^n) for n tables (much better than n! but still exponential)
- **Approach**: Cache results of subproblems
- For each subset S of tables, compute best way to join them
- Use memoization: `best_cost[S] = min over all T in S of (best_cost[S-T] + cost(T))`

#### Option B: Branch and Bound (Practical)
- **Time**: O(?) but typically much faster in practice
- **Approach**: 
  1. Enumerate join orderings (DFS)
  2. Prune branches where current cost > best found so far
  3. Use lower bound estimates for early pruning

#### Option C: Greedy with Backtracking (Hybrid)
- **Time**: O(n^2) typically, O(n!) worst case
- **Approach**:
  1. Try greedy ordering
  2. If cost seems suboptimal, try alternatives
  3. Use heuristics to prune search space

### 4. Cost Model

To make search effective, need cost estimates for:

```rust
pub trait CostEstimator {
    /// Estimate rows after joining left and right
    fn estimate_join_cardinality(
        &self,
        left_rows: usize,
        right_rows: usize,
        join_selectivity: f64,  // 0.0-1.0
    ) -> usize;
    
    /// Estimate cost of a join operation
    fn estimate_join_cost(
        &self,
        left_rows: usize,
        right_rows: usize,
        join_type: JoinType,  // HASH vs NESTED_LOOP
    ) -> f64;
    
    /// Estimate rows after filter
    fn estimate_filter_cardinality(
        &self,
        input_rows: usize,
        selectivity: f64,
    ) -> usize;
}
```

## Architecture

### Phase 1: Search Space Enumeration (Foundation)

```rust
// crates/executor/src/select/join/search.rs

pub struct JoinSearchState {
    /// Tables already joined
    joined_tables: HashSet<String>,
    /// Current accumulated cost
    accumulated_cost: f64,
    /// Cardinality of current result
    cardinality: usize,
}

pub struct JoinOrderSearch {
    /// All tables in the query
    all_tables: HashSet<String>,
    /// Join edges (from reorder.rs)
    edges: Vec<JoinEdge>,
    /// Cost estimator
    cost_estimator: Box<dyn CostEstimator>,
}

impl JoinOrderSearch {
    pub fn find_optimal_order(&self) -> Vec<String> {
        // Enumerate search space, return best ordering
    }
    
    fn search_recursive(
        &self,
        state: JoinSearchState,
        best_so_far: &mut f64,
    ) -> Option<Vec<String>> {
        // Explore all possible next tables to join
        // Prune if current cost exceeds best found
        // Return best path found
    }
}
```

### Phase 2: Cost Estimation

```rust
// crates/executor/src/select/join/cost_estimator.rs

pub struct SimpleCardinalityEstimator {
    /// Statistics about tables (approximate row counts)
    table_stats: HashMap<String, TableStatistics>,
    /// Selectivity of join conditions
    join_selectivities: HashMap<(String, String), f64>,
}

pub struct TableStatistics {
    pub name: String,
    pub row_count: usize,
    pub column_stats: HashMap<String, ColumnStatistics>,
}

pub struct ColumnStatistics {
    pub min_value: Option<Value>,
    pub max_value: Option<Value>,
    pub null_count: usize,
    pub distinct_count: usize,
}
```

### Phase 3: Integration with Existing Join Execution

Current code structure:
```
execute_from (SELECT)
  ├─ execute_join_with_predicates (FROM with equijoins)
  └─ handle predicates via JoinOrderAnalyzer + ExpressionMapper
```

New integration:
```
execute_from (SELECT)
  ├─ analyze predicates → JoinOrderAnalyzer
  ├─ build search space → JoinOrderSearch
  ├─ find optimal order → best_ordering: Vec<String>
  └─ execute_from_reordered(best_ordering)
```

## Implementation Stages

### Stage 1: Search Enumeration (Current + Refactor)
**Time**: 2-3 hours
**Deliverables**:
- New `search.rs` module for join order enumeration
- Recursive search function with pruning
- Tests for various join graphs

**Code structure**:
```rust
pub struct JoinOrderSearch {
    tables: HashSet<String>,
    edges: Vec<JoinEdge>,
    // Start with simple cost model (rows only)
}

impl JoinOrderSearch {
    pub fn find_optimal_order(&self) -> Vec<String> {
        self.dfs_search(
            HashSet::new(),
            0.0,  // accumulated cost
            0.0,  // best cost so far (infinity)
        )
    }
    
    fn dfs_search(
        &self,
        joined: HashSet<String>,
        cost_so_far: f64,
        best_cost: f64,
    ) -> Vec<String> {
        if joined.len() == self.tables.len() {
            return joined.iter().cloned().collect();
        }
        
        let mut best_order = vec![];
        
        // Try adding each unjoined table
        for table in &self.tables {
            if joined.contains(table) { continue; }
            
            // Estimate cost of joining this table
            let join_cost = self.estimate_cost(&joined, table);
            
            // Prune if exceeds best
            if cost_so_far + join_cost > best_cost {
                continue;
            }
            
            // Recursively search
            let mut next_joined = joined.clone();
            next_joined.insert(table.clone());
            let order = self.dfs_search(next_joined, cost_so_far + join_cost, best_cost);
            
            if cost_so_far + join_cost < best_cost {
                best_cost = cost_so_far + join_cost;
                best_order = order;
            }
        }
        
        best_order
    }
}
```

### Stage 2: Cost Estimation Integration
**Time**: 3-4 hours
**Deliverables**:
- `CostEstimator` trait
- `SimpleCardinalityEstimator` implementation
- Table statistics collection

### Stage 3: Memoization (DP Optimization)
**Time**: 2-3 hours
**Deliverables**:
- Memoization cache using subset bitmasks
- O(3^n) algorithm via DP
- Benchmark comparing search strategies

### Stage 4: Integration with Execution
**Time**: 2-3 hours
**Deliverables**:
- Integration with `execute_from_reordered`
- Full end-to-end reordering
- Performance benchmarks

## Benefits Over Greedy

| Aspect | Greedy | Search |
|--------|--------|--------|
| **Optimality** | ~70% of optimal | 100% (exhaustive) |
| **Time** | O(n^2) | O(3^n) or O(n!) pruned |
| **Implementation** | Simple | More complex |
| **Practical** | Good for 3-4 tables | Good for ≤10 tables |

For typical queries (3-6 tables), search time is negligible.

## Risk & Mitigation

| Risk | Mitigation |
|------|------------|
| Exponential search time | Prune aggressively, cap table count |
| Invalid cost estimates | Start simple, improve incrementally |
| Breaks existing tests | All new code, integration gradual |
| Reordering changes semantics | Only reorder INNER joins, test thoroughly |

## Next Steps

1. **Validate with current infrastructure**: 
   - Do greedy and exhaustive search give different results?
   - How often is greedy suboptimal?

2. **Implement Stage 1**:
   - Add `search.rs` with basic DFS enumeration
   - Compare results with `JoinOrderAnalyzer`

3. **Measure impact**:
   - Run on select5.test scenarios
   - Benchmark different search strategies

4. **Decide on final approach**:
   - If search complexity manageable → proceed with stages 2-4
   - If too slow → hybrid strategy combining greedy + search

---

**Current Status**: Planning → Ready for implementation  
**Complexity**: Medium (new algorithms, careful cost model)  
**Payoff**: Potentially optimal join orders for multi-table queries
