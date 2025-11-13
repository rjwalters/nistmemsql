# Index Ordering Validation Results - Issue #1449

**Issue**: [#1449](https://github.com/rjwalters/vibesql/issues/1449)
**PR Tested**: [#1442](https://github.com/rjwalters/vibesql/pull/1442) - "fix: Preserve index ordering in range_scan and multi_lookup"
**Validation Date**: November 12, 2025
**Commit Tested**: f216fb30 (includes PR #1442)

## Executive Summary

**Result**: ❌ **VALIDATION FAILED**

PR #1442 claimed to fix 50+ SQLLogicTest failures in `index/between/` and `index/commute/` test categories. This validation confirms that **ALL tests in both categories still fail** after the PR was merged.

## Test Results

### BETWEEN Tests

| Category | Total Files | Passed | Failed | Pass Rate |
|----------|-------------|--------|--------|-----------|
| index/between/1/ | 1 | 0 | 1 | 0% |
| index/between/10/ | 6 | 0 | 6 | 0% |
| index/between/100/ | 5 | 0 | 5 | 0% |
| index/between/1000/ | 1 | 0 | 1 | 0% |
| **TOTAL** | **13** | **0** | **13** | **0%** |

### COMMUTE Tests

| Category | Total Files | Passed | Failed | Pass Rate |
|----------|-------------|--------|--------|-----------|
| index/commute/10/ | 35 | 0 | 35 | 0% |
| index/commute/100/ | 13 | 0 | 13 | 0% |
| index/commute/1000/ | 4 | 0 | 4 | 0% |
| **TOTAL** | **52** | **0** | **52** | **0%** |

### Combined Results

| Metric | Value |
|--------|-------|
| **Total Test Files** | 65 |
| **Total Passed** | 0 |
| **Total Failed** | 65 |
| **Pass Rate** | **0%** |

## Failure Analysis

### Sample Failures

#### BETWEEN Test Failure Example
```
File: index/between/10/slt_good_0.test
SQL: SELECT pk FROM tab1 WHERE col3 > 23 AND ((col0 <= 96)) OR ...
Status: ❌ FAILED
Error: query result mismatch - hash mismatch
```

#### COMMUTE Test Failure Example
```
File: index/commute/100/slt_good_8.test
SQL: SELECT pk FROM tab1 WHERE col3 > 795
Status: ❌ FAILED
Error: query result mismatch - hash mismatch
```

### Failure Pattern

All failures show the same pattern:
- Query executes successfully
- Results are returned
- Result hash does NOT match expected hash
- This indicates incorrect result ordering or incorrect result set

## What PR #1442 Changed

From the PR description, #1442 made these changes to `crates/vibesql-storage/src/database/indexes.rs`:

1. **Removed `sort_unstable()` from `range_scan()`**
   - Previously sorted results by row index
   - Now preserves BTreeMap iteration order (sorted by index key value)

2. **Removed `sort_unstable()` from `multi_lookup()`**
   - Previously sorted results by row index
   - Now preserves BTreeMap iteration order (sorted by index key value)

3. **Added unit tests**
   - Tests verify ordering behavior at the storage layer
   - Unit tests PASS, but integration tests FAIL

## Root Cause Analysis

The failure of ALL index/between and index/commute tests suggests:

1. **Unit tests don't cover the full query path**: The storage-level unit tests pass, but the full SQL query execution path still produces incorrect results.

2. **Additional sorting may be happening elsewhere**: The query executor or result processing may be applying additional sorting that overrides the index ordering.

3. **The fix may be incomplete**: There may be other locations in the codebase where row-index sorting is applied to index-based query results.

4. **ORDER BY clauses may be involved**: These tests may include ORDER BY clauses that interact with index ordering in unexpected ways.

## Recommendations

### Immediate Actions

1. **Investigate the full query execution path**
   - Trace a failing query from SQL parsing → execution → result return
   - Identify where incorrect ordering is introduced
   - Check for additional `sort()` calls in executor or result processing

2. **Examine test expectations**
   - Verify that test expectations are correct
   - Understand what ordering the tests actually expect
   - Check if ORDER BY clauses are present in failing queries

3. **Add integration tests**
   - Create end-to-end tests that match the SQLLogicTest scenarios
   - Test full SQL query execution, not just storage-layer operations
   - Ensure tests cover the complete execution path

### Investigation Questions

1. Where else in the codebase might results be sorted?
2. Do the failing queries include ORDER BY clauses?
3. How do ORDER BY clauses interact with index scans?
4. Are there any caching or result processing steps that might reorder results?

## Files for Reference

- **Validation Log**: `validation_run.log` (contains full test output)
- **Storage Changes**: `crates/vibesql-storage/src/database/indexes.rs`
- **Related Issue**: #1422 (original bug report)
- **Related PR**: #1442 (attempted fix)

## Next Steps

1. **Do NOT close issue #1449** - validation shows the problem is not resolved
2. **Reopen or create new issue** for the persistent failures
3. **Investigate deeper** into the query execution path
4. **Consider reverting PR #1442** if it doesn't actually improve the situation
5. **Add comprehensive integration tests** before attempting another fix

## Conclusion

PR #1442's fix was insufficient to resolve the index ordering issues. While the storage-level changes may be correct, the full query execution path still produces incorrect results for index-based queries with BETWEEN and comparison operators.

**Validation Status**: ❌ **FAILED - All 65 tests still failing**

---

*This validation was performed as part of Issue #1449's acceptance criteria. The results indicate that PR #1442 did not successfully resolve the claimed 50+ test failures.*
