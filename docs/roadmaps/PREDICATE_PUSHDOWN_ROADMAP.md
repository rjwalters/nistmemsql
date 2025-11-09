# Predicate Pushdown Optimization - Implementation Roadmap

## Status: Phase 1 Complete - Infrastructure Ready

The predicate pushdown optimization infrastructure has been implemented in `crates/executor/src/optimizer/where_pushdown.rs`. This document outlines how to complete Phases 2-3 to fully solve the memory exhaustion issue in issue #1036.

## What is Predicate Pushdown?

Predicate pushdown is a query optimization technique that applies filter conditions (WHERE clause predicates) as early as possible during query execution, rather than after computing the entire result set.

### Problem: Current Behavior
```
SELECT * FROM t1, t2, t3, t4
WHERE a1 = 5 AND a1 = b2 AND a2 = b3 AND a3 = b4

Current execution:
1. Compute FROM: t1 × t2 × t3 × t4 = 10,000 rows (10^4)
2. Apply WHERE filter to all 10,000 rows
3. Returns ~35 rows

Memory usage: Full 10,000 rows kept in memory (potentially much larger for 64-table joins)
```

### Solution: With Predicate Pushdown
```
Optimized execution:
1. Apply table-local predicate during t1 scan: a1 = 5 → 1 row
2. JOIN with t2 using equijoin: a1 = b2 → ~1 row
3. JOIN with t3 using equijoin: a2 = b3 → ~1 row
4. JOIN with t4 using equijoin: a3 = b4 → ~1 row
5. Returns ~1 row

Memory usage: Minimal - only filtered rows kept in memory at each stage
```

## Phase 1: Infrastructure (COMPLETE)

### Files Created
- `crates/executor/src/optimizer/where_pushdown.rs` - Core predicate analysis and decomposition

### Functionality Implemented
1. **Predicate Decomposition** (`decompose_where_predicates`)
   - Converts WHERE clause into CNF (Conjunctive Normal Form)
   - Splits AND-separated conditions into individual predicates
   - Handles nested AND/OR expressions

2. **Predicate Classification** (`classify_predicate`)
   - **TableLocal**: References only one table → Can be pushed to table scan
   - **Equijoin**: Two tables with equality → Can be pushed to join operation
   - **Complex**: Multiple tables or non-equality → Must remain in post-join WHERE

3. **Table Reference Extraction** (`extract_table_references`)
   - Recursively finds all table names referenced in expressions
   - Handles qualified columns (table.column) and unqualified columns
   - Works with all expression types (binary ops, functions, CASE, etc.)

4. **Predicate Filtering Functions**
   - `get_table_local_predicates()` - Extract table-specific filters
   - `get_equijoin_predicates()` - Extract join conditions
   - `get_post_join_predicates()` - Extract remaining complex filters
   - `combine_with_and()` - Combine multiple expressions with AND

5. **Unit Tests** (4 comprehensive tests)
   - Test CNF decomposition
   - Test table-local predicate classification
   - Test equijoin predicate extraction
   - Test predicate filtering functions

## Phase 2: Scanner Integration (NOT STARTED)

### Objective
Apply table-local predicates during table scan, before any joins.

### Files to Modify
- `crates/executor/src/select/scan.rs` - Add WHERE clause support
- `crates/executor/src/select/executor/execute.rs` - Pass WHERE info from above

### Implementation Steps

1. **Extend `execute_from_clause` signature**
   ```rust
   pub(super) fn execute_from_clause<F>(
       from: &ast::FromClause,
       cte_results: &HashMap<String, CteResult>,
       database: &storage::Database,
       where_clause: Option<&ast::Expression>,  // NEW
       execute_subquery: F,
   ) -> Result<FromResult, ExecutorError>
   ```

2. **Modify `execute_table_scan`**
   ```rust
   fn execute_table_scan(
       table_name: &str,
       alias: Option<&String>,
       cte_results: &HashMap<String, CteResult>,
       database: &storage::Database,
       where_clause: Option<&ast::Expression>,  // NEW
   ) -> Result<FromResult, ExecutorError> {
       // ... existing code ...
       
       // NEW: Extract and apply table-local predicates
       if let Some(where_clause) = where_clause {
           let predicates = decompose_where_predicates(where_clause);
           let table_local = get_table_local_predicates(&predicates, table_name);
           if let Some(local_where) = combine_with_and(table_local) {
               // Apply local WHERE to rows after scanning
               rows = apply_where_filter(..., &local_where, ...)?;
           }
       }
       
       Ok(FromResult { schema, rows })
   }
   ```

3. **Modify `execute_from` in SelectExecutor**
   ```rust
   pub(super) fn execute_from(
       &self,
       from: &ast::FromClause,
       cte_results: &HashMap<String, CteResult>,
       where_clause: Option<&ast::Expression>,  // NEW
   ) -> Result<FromResult, ExecutorError> {
       execute_from_clause(from, cte_results, self.database, where_clause, |q| self.execute(q))
   }
   ```

4. **Modify `execute_with_ctes` in SelectExecutor**
   ```rust
   } else if let Some(from_clause) = &stmt.from {
       // Pass WHERE clause to execute_from for potential pushdown
       let from_result = self.execute_from(from_clause, cte_results, stmt.where_clause.as_ref())?;
       self.execute_without_aggregation(stmt, from_result)?
   }
   ```

5. **Threading through JOIN execution**
   - `execute_join` needs to pass WHERE clause through recursive calls
   - Both left and right sides should receive WHERE clause for filtering

### Expected Memory Impact
- Tables with local predicates: 10 rows → 1 row (90% reduction)
- Intermediate JOIN results: 100 rows → 10 rows (90% reduction)
- select5.test 64-table case: 10^64 potential rows → ~35 actual rows

## Phase 3: Join-Level Pushdown (NOT STARTED)

### Objective
Apply equijoin predicates directly in join operations, not as separate WHERE filtering.

### Files to Modify
- `crates/executor/src/select/join/mod.rs` - Receive equijoin predicates
- `crates/executor/src/select/join/nested_loop.rs` - Use join predicates
- `crates/executor/src/select/join/hash_join.rs` - Improve hash join utilization

### Implementation Steps

1. **Modify `nested_loop_join` signature**
   ```rust
   pub(super) fn nested_loop_join(
       left: FromResult,
       right: FromResult,
       join_type: &ast::JoinType,
       condition: &Option<ast::Expression>,
       additional_equijoins: &[Expression],  // NEW - from WHERE clause
       database: &storage::Database,
   ) -> Result<FromResult, ExecutorError>
   ```

2. **Combine join conditions**
   - Merge `condition` with equijoins from WHERE clause
   - Both represent join predicates and should be applied during join

3. **Optimize hash join selection**
   - Better detection of equijoin opportunities from WHERE clause
   - Use hash join more frequently when equijoins are available

4. **Memory estimation improvements**
   - Current: `check_join_size_limit` uses pessimistic estimate (100 MB per row)
   - Improved: Account for join selectivity from known equijoin conditions
   - Example: `t1 × t2 ON t1.a = t2.b` typically returns 1/N of Cartesian product

## Testing Strategy

### Unit Tests (Already Exist)
- `where_pushdown.rs` tests cover predicate decomposition

### Integration Tests (To Add)
```rust
#[test]
fn test_table_local_predicate_reduces_memory() {
    // Create t1, t2, t3, t4 with 10 rows each
    // Query: ... WHERE a1 = 5 AND ...
    // Assert memory usage << Cartesian product memory
}

#[test]
fn test_select5_basic_multi_table_join() {
    // Run simplified version of select5.test queries
    // Assert queries complete successfully with reasonable memory
}

#[test]
fn test_equijoin_predicate_from_where_clause() {
    // WHERE clause contains equijoin condition not in ON clause
    // Assert joins use equijoin predicate from WHERE
}
```

### Conformance Tests
- Run `third_party/sqllogictest/test/select5.test`
- Verify memory usage < 1GB (down from 6.48GB)
- Verify all result rows are correct

## Performance Expectations

For select5.test pathological query with 64 tables:
- **Before**: Memory exhaustion at 6.48 GB, never completes
- **After Phase 2 (Table pushdown)**: Each table filtered from 10 → ~1 rows, massive memory savings
- **After Phase 3 (Join pushdown)**: Equijoins applied during joins, intermediate results small

Estimated final memory: < 100 MB

## Backward Compatibility

All changes are backward compatible:
- Infrastructure is additive (new functions, not modifying existing behavior)
- Phase 2 threading through WHERE is internal refactoring
- Query results and semantics unchanged
- All existing tests continue to pass

## Implementation Notes

1. **Unqualified Column References**: If WHERE clause uses unqualified columns (e.g., `a1` instead of `t1.a1`), they can't be classified at the optimization level. They must still work due to schema resolution during execution.

2. **Complex Expressions**: Expressions like `t1.a + t2.b > 10` are classified as Complex and remain in post-join WHERE. This is correct - they require data from multiple tables.

3. **NULL Handling**: Equijoin conditions must account for SQL semantics where NULL ≠ NULL. This is handled correctly by existing join implementations.

4. **OR Conditions**: WHERE clauses with OR at the top level (e.g., `a = 5 OR b = 10`) cannot be pushed down without duplication. Currently treated as Complex, which is conservative but correct.

5. **Set Operations**: CTEs, UNION, INTERSECT, EXCEPT should be unaffected by predicate pushdown since they operate at higher levels.

## Contribution Guidelines

When implementing the next phase:
1. Start with Phase 2 (scanner integration) for maximum memory impact
2. Write comprehensive unit tests before modifying execution paths
3. Use `decompose_where_predicates()` in new code - don't reimplement
4. Update this roadmap as progress is made
5. Consider performance impact on simple queries (single table) - avoid regressions

## References

- Issue #1036: "Predicate Pushdown: select5.test Memory Exhaustion (6.48 GB)"
- Related: SQL query optimization, predicate pushdown, cost-based optimizer design
- Similar implementations: PostgreSQL, MySQL, SQLite query planners
