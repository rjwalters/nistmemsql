# Issue #1807 Test Suite: Multi-column IN Clause Optimization

This test suite documents the issues and expected behavior for multi-column index IN clause queries.

## Problem Summary

When using an `IN` clause on the first column of a multi-column index, queries return 0 rows instead of the expected results.

Example:
```sql
CREATE INDEX idx_test ON test (a, b);
INSERT INTO test VALUES (1, 10, 20), (2, 10, 30);
SELECT pk FROM test WHERE a IN (10);  -- Returns 0 rows (WRONG!)
```

Expected: Returns rows 1 and 2
Actual (without fix): Returns 0 rows

## Root Causes

### 1. BTreeMap Ordering Issue
The index uses `BTreeMap<Vec<SqlValue>, Vec<usize>>` for storage. When searching for `a IN (10)`:
- We create a search key `[10]`
- Index contains keys `[10, 20]` and `[10, 30]`
- BTreeMap ordering: `[10] < [10, 20]` (shorter vectors compare as less)
- `range_scan([10]..=[10])` doesn't match `[10, 20]` because they're not equal

**Solution**: Implement prefix matching - when start == end with inclusive bounds, iterate and compare only the first column.

### 2. Predicate Pushdown Incompatibility
The index optimization runs in `execute_without_aggregation()` which expects row indices that match positions in the FROM result. But predicate pushdown in `execute_from_with_where()` filters rows early, changing the indices.

**Conflict**:
- Index optimization needs ALL rows to preserve correct row indices
- Predicate pushdown filters rows early for performance
- These are fundamentally incompatible in the current architecture

**Temporary Fix**: Disable predicate pushdown globally (lines 81-86 in execute.rs)
**Performance Impact**: ALL queries lose predicate pushdown optimization
**Proper Solution**: Move index optimization to scan/FROM level

### 3. Inefficient Algorithm
Current prefix matching implementation uses O(n) iteration through all index entries instead of O(log n) BTreeMap range operations.

**Issue**: The temporary fix iterates through the entire BTreeMap
**Proper Solution**: Calculate correct upper bound for BTreeMap::range()
Example: `range([10, MIN]..=[10, MAX])` or `range([10]..=[11])`

## Test Files

### `multicolumn_in_basic.test`
Basic functionality tests for IN clauses on multi-column indexes.
- Single value IN clause
- Multiple value IN clause
- Non-existent values
- Multiple columns in SELECT

### `multicolumn_in_distinct.test`
Tests with DISTINCT to force materialized execution path.
Demonstrates that the fix works in `execute_without_aggregation()`.

### `multicolumn_in_predicate_pushdown.test`
Documents the predicate pushdown incompatibility.
Shows queries that would benefit from predicate pushdown but can't have it with the temporary fix.

### `multicolumn_in_edge_cases.test`
Edge cases and corner scenarios:
- NULL values in indexed columns
- NULL in IN clause value list
- Duplicate values in IN clause
- Large IN clauses
- Combining NULL checks with IN

### `multicolumn_in_btreemap_ordering.test`
Technical tests demonstrating BTreeMap ordering behavior.
- Prefix matching on multi-column indexes
- Result ordering (index order vs row order)
- Three-column index scenarios

## Current Status (PR #1845)

**Implemented**:
✅ Prefix matching in `range_scan()` (index_operations.rs lines 69-83)
✅ Multi-column index support in `try_index_for_in_expr()` (where_filter.rs lines 581-597)
✅ Disabled predicate pushdown (execute.rs lines 81-86)

**Issues Identified by Judge**:
❌ **Critical #1**: Predicate pushdown disabled globally (performance regression)
❌ **Critical #2**: O(n) iteration instead of O(log n) BTreeMap operations
❌ **Critical #3**: DiskBacked code path unhandled

**Architectural Issues**:
- Index optimization relies on absolute row indices
- Predicate pushdown changes row indices
- These are fundamentally incompatible in current design
- Proper fix requires moving index optimization to scan level

## Running These Tests

With the temporary fix (predicate pushdown disabled):
```bash
SQLLOGICTEST_FILES="tests/issue-1807/multicolumn_in_basic.test" cargo test --release run_sqllogictest_file
```

Without the fix (on main branch):
```bash
# Tests will FAIL with 0 rows returned
git checkout main
SQLLOGICTEST_FILES="tests/issue-1807/multicolumn_in_basic.test" cargo test --release run_sqllogictest_file
```

## Future Work

### Short-term (Band-aid)
1. Implement selective predicate pushdown (only disable for multi-column IN queries)
2. Optimize prefix matching to use BTreeMap::range() with calculated bounds
3. Handle DiskBacked code path

### Long-term (Proper Solution)
1. Move index optimization from execute_without_aggregation to execute_from/scan level
2. Apply index optimization during table scanning, before row indices are assigned
3. Re-enable full predicate pushdown
4. Maintain index-based result ordering

### Performance Optimization
1. Use BTreeMap::range() with proper bounds instead of full iteration
2. Calculate upper bound for prefix matching: `[10, MAX_VALUE]` or `[11]`
3. Leverage BTreeMap's O(log n + k) complexity for range scans

## References

- Issue: #1807
- PR: #1845
- Related files:
  - `crates/vibesql-storage/src/database/indexes/index_operations.rs`
  - `crates/vibesql-executor/src/select/executor/index_optimization/where_filter.rs`
  - `crates/vibesql-executor/src/select/executor/execute.rs`
