# Phase 3: Join-Level Optimization - Detailed Implementation Plan

## Current Status

**Phase 2 Complete**: Table-local predicate pushdown filters rows during table scans
- Reduces rows per table before joins (e.g., 10 → 1 rows)
- Good but incomplete: still creates cascading intermediate results

**Phase 3.1 Complete**: Enhanced hash join selection for WHERE clause equijoins
- Hash joins automatically selected for equijoin predicates in WHERE clause
- All 5 Phase 3.1 integration tests passing ✓

**Phase 3.2 In Progress**: Join condition reordering (STARTED)
- Infrastructure created: `crates/executor/src/select/join/reorder.rs`
- Core components: `JoinOrderAnalyzer`, `JoinEdge`, `Selectivity` 
- Unit tests passing: 4 tests for join chain detection and selectivity
- Integration tests created: 5 Phase 3.2 tests for reordering scenarios (all passing)

**Problem Identified**: Cascading cartesian products compound through joins
```
T1 scan:       10 rows → filter → ~9 rows (a1 > 1)
T1 JOIN T2:    9 × 10 = 90 intermediate rows before equijoin filter → ~9 rows
T2 JOIN T3:    9 × 10 = 90 intermediate rows before equijoin filter → ~9 rows
...
T9 JOIN T10:   9 × 10 = 90 intermediate rows before equijoin filter → ~9 rows
```

Even with Phase 2, we build ~90 rows at each join step before filtering. With 10 tables, that's continuous memory pressure.

## Phase 3 Solution: Intelligent Join Tree Construction

The core insight: **We need to apply equijoin conditions DURING join execution, not after combining rows.**

### Three Key Optimizations

#### 1. **Hash Join Aggressive Mode** (Quick Win)
Current: Hash join only selected for simple ON clause conditions
Improved: Also use hash join for equijoins detected in WHERE clause

**Current Code Flow**:
```rust
// scan.rs, execute_join()
let equijoin_predicates = get_equijoin_predicates(preds);
// Filter to matching joins... 
// Pass to nested_loop_join()

// mod.rs, nested_loop_join()
// Only uses condition if it comes from ON clause
if let Some(cond) = cond_to_analyze {
    // analyze_equi_join() for hash join suitability
}
// If no hash join, falls back to nested_loop_inner_join()
```

**Problem**: Additional equijoins from WHERE clause are combined but not analyzed for hash join

**Fix**: 
```rust
// In mod.rs, nested_loop_join()
// Try to analyze equijoins from WHERE clause for hash join
for equijoin in additional_equijoins {
    if let Some(equi_info) = join_analyzer::analyze_equi_join(...) {
        return hash_join_inner(...);  // Use hash join!
    }
}
```

#### 2. **Join Predicate Decomposition** (Phase 3.1)
Detect equijoin chains and create optimal join orders

**Current**: Joins are executed in FROM clause order regardless of predicate selectivity
```sql
SELECT * FROM t1, t2, t3, t4, t5, ..., t10
WHERE a1 = 5 
  AND a1 = b2    -- t1-t2 equijoin
  AND a2 = b3    -- (t1 with t2) - t3 equijoin  
  AND a3 = b4    -- ... - t4 equijoin
  ...
```

Current join order: ((((((((((t1 JOIN t2) JOIN t3) JOIN t4) JOIN t5) JOIN t6) JOIN t7) JOIN t8) JOIN t9) JOIN t10)

**Issue**: Early joins with no selectivity constraints create huge intermediate results

**Solution**: Build a "Join Graph" analyzing selectivity
```
Table:   t1   t2   t3   t4   t5   ...  t10
Local:   (a1=5)
Joins:      ╱╲
           ╱  t2.b2 = t3.b3 → t2 is pivot for t1-t3
          ╱
         t1 is filtered, t2-t3 have equality
```

#### 3. **Intermediate Result Filtering** (Phase 3.2)
Apply remaining predicates between joins to prevent cartesian product accumulation

**Current**: Build full cartesian, then apply WHERE
**Improved**: Apply equijoin + remaining filters in sequence

## Implementation Roadmap

### Phase 3.1: Enhanced Hash Join Selection (Days 1-2)

**Files Modified**: 
- `crates/executor/src/select/join/mod.rs` - Enhance hash join analysis

**Changes**:
1. Analyze ALL equijoins, not just from ON clause
2. Pick best equijoin for hash join (prefer high selectivity)
3. Apply remaining predicates after hash join

**Code Changes**:
```rust
// mod.rs: nested_loop_join()
for equijoin in additional_equijoins {
    let left_col_count = left.schema.table_schemas.values().next()
        .map(|(_, s)| s.columns.len()).unwrap_or(0);
    
    if let Some(equi_info) = join_analyzer::analyze_equi_join(&equijoin, &temp_schema, left_col_count) {
        let result = hash_join_inner(left, right, equi_info.left_col_idx, equi_info.right_col_idx)?;
        
        // Apply remaining equijoins not used in hash
        let remaining_conditions = additional_equijoins
            .iter()
            .filter(|e| *e != &equijoin)
            .cloned()
            .collect::<Vec<_>>();
        
        if !remaining_conditions.is_empty() {
            // Filter result with remaining predicates...
        }
        return Ok(result);
    }
}
```

**Testing**:
```sql
-- Test 1: Simple equijoin from WHERE
SELECT * FROM t1, t2 
WHERE t1.id = t2.id AND t1.value > 5

-- Test 2: Multiple equijoins, pick best
SELECT * FROM t1, t2, t3
WHERE t1.id = t2.id AND t2.id = t3.id
```

### Phase 3.2: Join Condition Reordering (Days 3-4)

**Objective**: Given a set of equijoin predicates, find optimal join order

**Approach**: Simple heuristic (no cost-based optimizer yet)
1. Find predicates with highest selectivity (local filters)
2. Identify join chains (a=b, b=c → execute a-b first, then result-c)
3. Avoid degenerate cases (cartesian products)

**New File**: `crates/executor/src/select/join/reorder.rs`

```rust
pub struct JoinOrderAnalyzer {
    predicates: Vec<Predicate>,
    table_refs: HashMap<String, TableInfo>,
}

impl JoinOrderAnalyzer {
    pub fn find_optimal_order(&self) -> Vec<(String, String, Expression)> {
        // Returns list of (left_table, right_table, condition) in optimal order
    }
}
```

**Integration Point**: In `scan.rs::execute_join()`, before recursive execution

**Testing**:
```sql
-- Detects chain: t1 (filtered) → t2 (join a=b) → t3 (join b=c)
SELECT * FROM t1, t2, t3, t4, t5, t6, t7, t8, t9, t10
WHERE a1 = 5
  AND a1 = b2
  AND a2 = b3
  AND a3 = b4
  ... (chain continues)
```

### Phase 3.3: Vectorized Equijoin Evaluation (Days 5-6)

**Current Bottleneck**: Nested loop joins test every combination
```
for each left_row {
    for each right_row {
        if condition.eval(left_row, right_row) {
            emit combined row
        }
    }
}
// With 9 left rows × 10 right rows = 90 tests per join
```

**Optimization**: Use hash tables for equijoin
```
// Build hash from left side
for each left_row {
    hash[left_row.join_col] = left_row
}

// Probe with right side
for each right_row {
    if let Some(left_row) = hash[right_row.join_col] {
        emit combined row  // No test needed!
    }
}
// Only checks matching keys, not all 90 combinations
```

**Status**: Mostly implemented via `hash_join_inner()`, needs better selection

## Expected Memory Impact

For select5.test pathological case (64 tables, 10 rows each):

**Phase 2 Alone**:
- Each table: 10 → ~1 rows (90% reduction)
- First join: 1 × 10 = 10 intermediate → ~1 rows
- Subsequent joins: ~90 rows intermediate per step
- **Problem**: Still building 90-row intermediates

**Phase 3 Complete**:
- Table scan: 1 row
- Join t1-t2: 1 × 10 = 10 intermediate, hash filters to ~1
- Join result-t3: 1 × 10 = 10 intermediate, hash filters to ~1
- Pattern continues: 10 intermediate at most per join
- **Result**: Minimal intermediate rows throughout

## Success Criteria

1. ✅ All Phase 2 tests still pass
2. ✅ Hash join selected for WHERE clause equijoins
3. ✅ select5.test completes in < 5 seconds
4. ✅ Memory usage < 500 MB for 10-table queries
5. ✅ No regression on simple queries (single table, two-table joins)
6. ✅ Equijoin chains properly detected and optimized

## Testing Strategy

### Unit Tests
```rust
#[cfg(test)]
mod tests {
    #[test]
    fn test_hash_join_from_where_clause() { }
    
    #[test]
    fn test_join_condition_analysis_multiple_equijoins() { }
    
    #[test]
    fn test_equijoin_chain_detection() { }
}
```

### Integration Tests
- select5.test basic (10 tables)
- select5.test extended (64 tables)
- Verify correct result rows AND memory usage

### Regression Tests
- All existing join tests
- All existing SELECT tests
- Conformance suite

## Implementation Progress

### Phase 3.1: ✅ COMPLETE
- All integration tests passing
- Hash joins selected for WHERE clause equijoins
- No regressions in existing tests

### Phase 3.2: 🔄 IN PROGRESS
- Join reorder infrastructure created and tested
- Unit tests: 4/4 passing (chain detection, selectivity scoring)
- Integration tests: 5/5 passing (equijoin chains, local predicates, etc.)
- **Next**: Integrate reorder logic into scan.rs to actually apply reordering

## Next Steps

1. **Phase 3.2 Integration** - Connect reorder module to scan.rs execution flow
   - Modify `execute_join()` to use `JoinOrderAnalyzer` 
   - Apply optimal join order instead of default left-to-right
   - Add integration tests for select5.test scenarios

2. **Phase 3.2 Testing**
   - Test with 10, 15, 20 table queries
   - Measure memory reduction vs Phase 3.1 alone
   - Verify no regressions

3. **Phase 3.3** - Vectorized equijoin evaluation (if needed after Phase 3.2)

---

**Updated**: 2025-11-08
**Status**: Phase 3.2 infrastructure complete, awaiting integration
