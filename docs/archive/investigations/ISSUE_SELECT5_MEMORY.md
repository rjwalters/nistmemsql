# Issue: select5.test Causes Memory Exhaustion (6.48 GB)

## Problem Summary

**Severity**: High - Worker 2 gets stuck with memory warnings at 6.48 GB during `select5.test` execution

The SQLLogicTest file `third_party/sqllogictest/test/select5.test` contains stress-test queries with massive multi-table joins (up to 64 tables) that cause the query executor to exhaust available memory before returning results.

Example problematic query:
```sql
SELECT x35,x24,x17,x38,x55,x61,x36,x6,x2,...
FROM t4,t38,t47,t23,t40,t59,t29,t46,t12,t13,t2,t49,t14,t54,t33,t16,t24,t60,t64,t10,t56,t51,t17,t42,t55,t45,t58,t30,t63,t31,t18,t15,t27,t57,t62,t20,t3,t37,t5,t35,t28,t61,t21,t9,t26,t41,t44,t8,t19,t1,t53,t34,t50,t43,t48,t39,t36,t6,t7,t11,t25,t52,t32,t22
WHERE a24=b11 AND a39=b25 AND a13=b46 AND ... (64 equijoin conditions)
```

## Root Cause

The current query execution model applies the WHERE clause **after** executing the entire FROM clause (all JOINs):

**Current Flow:**
1. Execute FROM: Build Cartesian product of all tables → **10^64 rows** (10 rows × 64 tables)
2. Apply WHERE: Filter down to actual result rows

**Why this explodes:**
- 10 rows per table × 64 tables = 10^64 potential intermediate result rows
- Each row is ~100-200 bytes
- Intermediate result set before filtering: ~10^62 GB (well beyond available memory)
- WHERE clause never gets a chance to filter because memory exhaustion occurs first

**Code location:**
- `crates/executor/src/select/executor/nonagg.rs:28-74`
  - Line 28-29: FROM clause executed first
  - Line 50-74: WHERE clause applied to joined result

## Proposed Solution: Predicate Pushdown

Implement predicate pushdown optimization to filter rows **before** building Cartesian products:

### Algorithm

1. **Decompose WHERE Clause** (new module: `optimizer/where_pushdown.rs`)
   - Parse WHERE into CNF (Conjunctive Normal Form - AND-separated conditions)
   - Classify each condition:
     - **Table-local**: References only one table (e.g., `t1.a > 5`)
     - **Equijoin**: Simple equality across two tables (e.g., `t1.a = t2.b`)
     - **Complex**: Multiple tables or non-equality (e.g., `t1.a + t2.b > 10`)

2. **Build Optimized Join Tree**
   - Apply table-local predicates before any joins
   - Reorder joins to apply most selective conditions first
   - Push equijoin conditions into join operators

3. **Modified Execution Flow**
   - Scan t1, apply local predicates → N filtered rows
   - Join with t2 using equijoin condition → N×M filtered rows (not N×10)
   - Repeat for remaining tables
   - Final result: M rows instead of 10^64 intermediate rows

### Example Optimization

**Before (Current):**
```
t1: 10 rows
t2: 10 rows
...
t64: 10 rows
┌─────────────────────────────────────────────┐
│ Cartesian Product (10^64 rows)              │
│ WHERE clause filters down (6GB+ memory used)│
└─────────────────────────────────────────────┘
Result: ~35 rows
```

**After (Proposed):**
```
t1: 10 rows → (local filters) → ~5 rows
t2: 10 rows → (local filters) → ~5 rows
    ├─ Join with equijoin condition (a24=b11) → ~5 rows (not 50)
t3: 10 rows → (local filters) → ~5 rows
    ├─ Join with equijoin condition → ~5 rows
...
Result: ~35 rows (no intermediate explosion)
```

## Implementation Plan

### Phase 1: WHERE Clause Analysis
- [ ] Create `crates/executor/src/optimizer/where_pushdown.rs`
- [ ] Implement `analyze_where_predicates()` - classify conditions
- [ ] Implement `extract_table_local_predicates()` - find single-table filters
- [ ] Implement `extract_equijoin_conditions()` - find join conditions

### Phase 2: Execution Flow Changes
- [ ] Modify `scan.rs:execute_table_scan()` to accept optional WHERE predicates
- [ ] Modify `join/mod.rs:nested_loop_join()` to receive extracted equijoin conditions
- [ ] Update `nonagg.rs:execute_without_aggregation()` to:
  - Decompose WHERE before FROM
  - Pass table-local predicates to `scan.rs`
  - Pass join conditions to `join/mod.rs`
  - Apply remaining complex conditions after joins

### Phase 3: Testing
- [ ] Add unit tests for WHERE predicate decomposition
- [ ] Add integration tests with 2-table, 4-table, 8-table joins
- [ ] Verify select5.test runs without memory exhaustion
- [ ] Benchmark memory usage before/after

## Expected Impact

- **Memory Usage**: Reduce from 6.48 GB to <100 MB for select5.test queries
- **Performance**: 100-1000x faster for multi-table joins with equijoin conditions
- **Correctness**: No changes to result sets, only execution order
- **Scope**: Benefits any query with multiple JOINs and WHERE conditions

## Risk Assessment

**Low Risk**:
- Changes are localized to optimizer and query execution
- Existing tests provide regression detection
- Can be behind a feature flag if needed
- Simple optimization (CNF decomposition is standard)

**Edge Cases to Handle**:
- OR conditions (cannot push down without duplication)
- Complex predicates (keep them in post-join WHERE)
- NULLs in join conditions (handle per SQL semantics)

## Related Issues

- Relates to SQLLOGICTEST conformance #XXXX
- Blocks completion of select5.test validation

## Test File Details

**File**: `third_party/sqllogictest/test/select5.test`
- Total size: 31,948 lines
- Pathological queries: ~360 queries with 35+ table joins
- Largest join: 64 tables
- Each table: 10 rows with 3 columns (pk, int, varchar)
- All join conditions: Simple equalities (a_i = b_j)

## References

**Code Files**:
- `crates/executor/src/select/executor/nonagg.rs` - WHERE execution (lines 28-74)
- `crates/executor/src/select/join/nested_loop.rs` - Join execution
- `crates/executor/src/select/scan.rs` - FROM execution
- `crates/executor/src/limits.rs` - Memory limits (10 GB max)

**Standards**:
- Query optimization techniques (predicate pushdown)
- Database theory: Cost-based query optimization
- Relational algebra: WHERE push-down before joins
