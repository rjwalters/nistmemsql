# Investigation Report: Issue #1838 - index/orderby Test Failures

## Summary
Investigation into 31 failing tests in the `index/orderby` test category. Tests involve queries with both WHERE clauses (using index optimization) and ORDER BY clauses.

## Key Findings

### 1. Test Execution Results
- **Manual tests**: ✅ PASS - Simple test cases matching error patterns work correctly
- **Actual sqllogictest files**: ❌ FAIL - Tests fail when run via `./scripts/sqllogictest`
- **Error details**: Unable to obtain detailed error output from test runner

### 2. Code Analysis
- **ORDER BY optimization**: DISABLED in PR #1818 - currently returns `None` and falls back to `apply_order_by()`
- **WHERE index optimization**: RE-ENABLED in PR #1824 after being disabled in PR #1749
- **Current state**: WHERE filtering uses index optimization, ORDER BY uses standard sorting

### 3. Related Work
- PR #1749: Disabled both WHERE and ORDER BY index optimizations due to row loss bugs
- PR #1818: Permanently disabled ORDER BY optimization (architectural flaw)
- PR #1824: Re-enabled WHERE optimization with multi-column index support
- Uncommitted changes exist in main workspace for additional multi-column index fixes

### 4. Test Patterns
From `index/orderby/10/slt_good_0.test`:
- Creates tables with multi-column indexes (e.g., `idx_tab2_0 ON tab2 (col0 DESC, col4 DESC)`)
- Runs queries like: `SELECT pk FROM tab1 WHERE col0 = value ORDER BY 1 DESC`
- Tests combination of indexed WHERE filtering + ORDER BY

## Root Cause Hypothesis

The failures likely occur due to:
1. **Multi-column index edge cases** not handled correctly by WHERE optimization
2. **Incorrect row ordering** when index scan returns rows in index order that doesn't match ORDER BY requirements
3. **Row loss** similar to issues fixed in PR #1749, but specific to multi-column indexes

## Attempted Fixes

### Applied Uncommitted Changes
Applied patches from main workspace:
- Split `find_index_for_where()` into single-column and multi-column variants
- Added prefix matching logic for multi-column indexes in `range_scan()`

**Result**: Test still fails - uncommitted changes don't resolve the issue

## Recommendations

### Immediate Actions
1. **Get detailed error output**: Need access to actual query results vs expected results
2. **Enable test debugging**: Use test framework's verbose/debug mode to see specific failures
3. **Compare with SQLite**: Run same queries against SQLite to verify expected behavior

### Investigation Priorities
1. **Multi-column index handling**: Focus on queries using multi-column indexes
2. **Index scan ordering**: Verify rows returned from index scans match ORDER BY requirements
3. **Composite key handling**: Check if composite index keys are compared correctly

### Long-term Solutions
1. **Comprehensive test coverage**: Add unit tests for index + ORDER BY combinations
2. **Architecture review**: Consider if index optimization should respect ORDER BY direction
3. **Integration tests**: Create minimal reproducible test cases for each failure mode

## Test Cases Created

File: `test_orderby_issue.sql`
- Simple queries matching error patterns from issue
- All queries return correct results
- Suggests issue is specific to sqllogictest data patterns or multi-column indexes

## Files Modified

### Applied Patches (from main workspace)
- `crates/vibesql-executor/src/select/executor/index_optimization/where_filter.rs`
  - Split `find_index_for_where` into single/multi-column variants
- `crates/vibesql-storage/src/database/indexes/index_operations.rs`
  - Added prefix matching for multi-column IN clauses

### Status
These changes compile but don't fix the test failures.

## Next Steps for Resolver

1. Run tests with detailed error output capture
2. Identify specific query/data combinations that fail
3. Create minimal reproducible test case
4. Debug index scan vs ORDER BY interaction
5. Implement and test fix
6. Verify all 31 tests pass

## References

- Issue: #1838
- Related Issues: #1814, #1823, #1824, #1829, #1836
- Related PRs: #1749, #1818, #1824
- Test Directory: `third_party/sqllogictest/test/index/orderby/`

---

*Investigation conducted by Builder agent*
*Date: 2025-11-15*
