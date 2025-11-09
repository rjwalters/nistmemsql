# Phase 3.2.2 Join Tree Reordering - Implementation Plan

**Status**: Planning  
**Complexity**: High (requires careful schema/expression tracking)  
**Target**: Implement dynamic join reordering to apply optimal order in actual execution

## Problem Statement

Phase 3.2.1 successfully detects optimal join orders via `JoinOrderAnalyzer::find_optimal_order()`, which returns:
```rust
Vec<(HashSet<String>, String, Expression)>  // (left_tables, right_table, condition)
```

But **actual execution** still follows the original FROM clause order. The challenge: applying the computed order requires dynamically reconstructing the join tree while tracking schema evolution.

### Core Issue: Schema Column Index Drift

```
Original FROM: ((t1 JOIN t2 ON cond1) JOIN t3 ON cond2) JOIN t4 ON cond3

Schema evolution during execution:
  t1:             [col1, col2]
  t1 ⋈ t2:       [col1, col2, col3, col4]
  t1 ⋈ t2 ⋈ t3:  [col1, col2, col3, col4, col5, col6]
  ...

Original condition:   t3.col5 = t4.col7
Must be rewritten as: (combined_idx=4) = (combined_idx=8)  when applied
```

## Current Architecture

### What Works (Phase 3.1+)
- **Hash join optimization**: Detects equijoins from WHERE clause, uses O(n+m) hash join instead of O(n*m) nested loop
- **Predicate pushdown**: Decomposes WHERE into table-local and equijoin predicates
- **Join analysis**: `JoinOrderAnalyzer` computes optimal order correctly

### What's Missing (Phase 3.2.2)
- **Dynamic join tree execution**: Apply the computed optimal order
- **Expression remapping**: Track schema changes, map expressions to new column indices
- **Intermediate result handling**: Cache/manage intermediate join results

## Proposed Solutions

### Option A: Expression Rewriting (Recommended - Medium Complexity)

**Approach**: Rewrite join conditions to use column indices instead of table-qualified references

```rust
struct ColumnIndexMapper {
    // Maps (table, column) -> current_index_in_combined_schema
    column_positions: HashMap<(String, String), usize>,
    left_table_count: usize,
}

impl ColumnIndexMapper {
    fn rewrite_expression(&self, expr: &Expression) -> Result<Expression, Error> {
        // Convert: t3.col5 = t4.col7
        // To: Expression::ColumnIdx(4) = Expression::ColumnIdx(8)
        // OR use positional expressions that work with any schema
    }
    
    fn after_join(&mut self, right_table: &str, right_schema: &TableSchema) {
        // Update all positions after adding new table
    }
}
```

**Advantages**:
- Cleaner separation between analysis and execution
- Expressions become self-contained (don't depend on schema state)
- Works with existing evaluator

**Challenges**:
- Need new `Expression::ColumnIdx(usize)` variant OR positional tracking
- Requires careful bookkeeping during schema evolution

### Option B: Deferred Reordering (Lower Risk - Lower Complexity)

**Approach**: Execute joins in original order, but reorder rows at specific points

```rust
// Instead of reordering joins themselves, reorder materialized results
fn execute_join_with_reordering(
    joins: Vec<JoinSpec>,
    optimal_order: Vec<JoinStep>,
) -> Result<FromResult, Error> {
    // Execute in original order first
    let mut result = execute_standard_joins(joins)?;
    
    // If optimal_order differs, reorder using computed selectivity
    if differs_from_standard(&optimal_order) {
        result = reorder_result_rows(result, &optimal_order)?;
    }
    
    Ok(result)
}
```

**Advantages**:
- No changes to current join execution
- Progressive benefit as more rows match early filters
- Lower implementation risk

**Challenges**:
- Only helps if tables are separate (doesn't help cascading joins)
- Memory impact less dramatic than true reordering

### Option C: Step-by-Step Reconstruction (High Complexity - Most Powerful)

**Approach**: Execute joins in computed order, reconstructing FROM clauses dynamically

```rust
enum FromStep {
    TableScan(String),              // Start with single table
    JoinWith(String, Expression),   // Add table with condition
}

fn execute_reordered_from(
    steps: Vec<FromStep>,
    cte_results: &CTEResults,
    db: &Database,
) -> Result<FromResult, Error> {
    let mut current = execute_first_table(&steps[0])?;
    
    for step in &steps[1..] {
        if let FromStep::JoinWith(table, cond) = step {
            let right = execute_table_scan(table)?;
            // Map condition to current schema before joining
            let remapped_cond = remap_expression(&cond, &current.schema, &right.schema)?;
            current = nested_loop_join(current, right, cond, db)?;
        }
    }
    
    Ok(current)
}

fn remap_expression(
    expr: &Expression,
    left_schema: &CombinedSchema,
    right_schema: &TableSchema,
) -> Result<Expression, Error> {
    // Complex: track all table references, map to combined schema indices
}
```

**Advantages**:
- True join order execution
- Full optimization benefit
- Foundation for cost-based optimizer

**Challenges**:
- Most complex implementation
- Need complete expression remapping system
- Risk of subtle bugs in expression evaluation

## Recommended Path: Hybrid Approach

### Phase 3.2.2a: Foundation (Low Risk)
Implement Option A infrastructure without yet reordering:
1. Create `ColumnIndexMapper` struct
2. Add expression analysis: identify all table column references
3. Add schema tracking during joins
4. **No behavior change**: Just track what we would remap

### Phase 3.2.2b: Selective Reordering (Medium Risk)
Apply Option C only for "safe" cases:
1. Linear join chains only: A ⋈ B ⋈ C (no branching)
2. All conditions are equijoins
3. No complex predicates or subqueries

This handles the most common case without full complexity.

### Phase 3.2.2c: General Reordering (High Risk)
Expand to arbitrary join trees:
1. Support branching joins: (A ⋈ B) ⋈ (C ⋈ D)
2. Handle complex conditions with full expression remapping
3. Integration with cost-based optimizer

## Implementation Details

### Phase 3.2.2a: Foundation

**New files**:
- `crates/executor/src/select/join/expression_mapper.rs`

```rust
pub struct ExpressionMapper {
    // Track which columns are where in combined schema
    column_map: HashMap<(String, String), usize>,
    table_offsets: HashMap<String, usize>,
}

impl ExpressionMapper {
    pub fn new() -> Self { ... }
    
    pub fn add_table(&mut self, table: &str, schema: &TableSchema) {
        // Update all offsets
    }
    
    pub fn resolve_column(&self, table: &str, col: &str) -> Option<usize> {
        self.column_map.get(&(table.to_lower(), col.to_lower())).copied()
    }
    
    pub fn analyze_expression(&self, expr: &Expression) -> Result<ExpressionAnalysis, Error> {
        // What columns does this expression reference?
        // Are they all resolvable in current schema?
    }
}

pub struct ExpressionAnalysis {
    pub tables_referenced: HashSet<String>,
    pub columns_used: Vec<(String, String)>,
    pub is_local_to_tables: HashSet<String>,  // true if all refs from one table
}
```

**Modified files**:
- `crates/executor/src/select/join/mod.rs`: Add ExpressionMapper parameter
- `crates/executor/src/select/join/nested_loop.rs`: Track schema evolution
- `crates/executor/src/select/scan.rs`: Pass mapper through join execution

### Phase 3.2.2b: Selective Reordering

**Conditions for reordering**:
```rust
fn can_reorder_safely(
    from: &FromClause,
    optimal_order: &Vec<JoinStep>,
) -> bool {
    // Check: is_linear_chain(from) && 
    //        all_equijoins(from) &&
    //        optimal_order != standard_order()
}

fn is_linear_chain(from: &FromClause) -> bool {
    match from {
        FromClause::Join { left, right, .. } => {
            // Right must be a table, left can be another join
            matches!(right.as_ref(), FromClause::Table { .. }) &&
            (matches!(left.as_ref(), FromClause::Table { .. }) ||
             is_linear_chain(left.as_ref()))
        }
        FromClause::Table { .. } => true,
        _ => false,
    }
}
```

**Execution**:
```rust
fn execute_reordered_from<F>(
    from: &FromClause,
    optimal_order: Vec<JoinStep>,
    cte_results: &HashMap<String, CteResult>,
    db: &Database,
    execute_subquery: F,
) -> Result<FromResult, Error> {
    // Execute joins in optimal_order instead of from.clone()
    
    // Step 1: Get first table
    let mut current = ...
    let mut mapper = ExpressionMapper::new();
    mapper.add_table(&first_table_name, &first_schema);
    
    // Step 2: Join each subsequent table
    for (left_tables, right_table, condition) in optimal_order {
        let right = execute_table_scan(&right_table, ...)?;
        
        // Try to use the condition, but if it doesn't map, skip reordering
        if let Ok(analysis) = mapper.analyze_expression(&condition) {
            if analysis.tables_referenced.iter().all(|t| {
                mapper.column_map.contains_key(&(t.clone(), "*".to_string()))
            }) {
                // All columns resolvable - safe to use condition
                current = nested_loop_join(current, right, ..., &condition)?;
                mapper.add_table(&right_table, &right_schema);
            }
        }
    }
    
    Ok(current)
}
```

## Testing Strategy

### Unit Tests
- `test_expression_mapper_resolution`: Verify column mapping correctness
- `test_mapper_after_join`: Verify offsets update correctly
- `test_linear_chain_detection`: Ensure safety check works
- `test_reordering_vs_standard`: Compare results (should be identical)

### Integration Tests
- 3-table select5 scenarios with different join orders
- 4-table multi-table joins
- Mixed equijoins and non-equijoins
- Comparison with hash join results

### Performance Tests
- Before/after benchmark on multi-table joins
- Memory usage comparison
- Query execution time with optimal vs. standard order

## Risk Assessment

| Component | Risk | Mitigation |
|-----------|------|------------|
| Expression mapping | Medium | Thorough unit tests, analysis phase before execution |
| Schema tracking | Medium | Clear data structure, explicit offset updates |
| Condition validation | Low | Graceful fallback to standard order if unsafe |
| Test coverage | High | Comprehensive test matrix for join combinations |

## Timeline Estimate

- **Phase 3.2.2a (Foundation)**: 3-4 hours (structure, no behavior change)
- **Phase 3.2.2b (Selective Reordering)**: 4-5 hours (linear chains, safe conditions)
- **Phase 3.2.2c (General)**: 6-8 hours (complex trees, full expression mapping)

**Total for full implementation**: ~12-17 hours of focused work

## Fallback/Deferred Approach

If complexity proves intractable:
1. **Keep Phase 3.2.1** (detection + analysis)
2. **Implement Option B** (post-join row reordering)
3. **Document findings** for future cost-based optimizer
4. **Ship with current Phase 3.1** (hash join is substantial benefit)

This still provides significant optimization (hash joins) while deferring reordering complexity.

## SQLite Reference

SQLite's query planner (`sqlite3Select` in select.c) uses:
- **Nested loop query plan**: Executes as a series of nested loops
- **Query flattening**: Rewrites subqueries to avoid separate execution
- **Index selection**: Uses cost model to choose table order and indices
- **Loop variables**: Tracks loop context through `sqlite3WhereBegin` API

Key insight: SQLite doesn't rewrite expressions; it adjusts the **execution context** (which table is outer/inner loop). Our approach should leverage similar context management.

---

**Next Steps**:
1. Review this plan for feasibility
2. Decide: Implement 3.2.2a/b/c or defer to 3.2.1 + 3.1 shipping
3. Create issue subtasks for each phase
4. Begin implementation if proceeding
