# SQLLogicTest Index Test Analysis

**Date**: 2025-11-10
**Issue**: #1198
**Current Pass Rate**: 47/214 (21.96%)
**Target Pass Rate**: ≥80% (171+ tests)
**Gap**: 124 tests need to pass

## Executive Summary

Analysis of the 214 SQLLogicTest index tests reveals systematic issues across multiple categories. The most critical areas are:

1. **orderby_nosort** (147 failures, 0% pass rate) - ORDER BY optimization without explicit sorting
2. **in** (39 failures, 0% pass rate) - IN operator with indexed columns
3. **commute** (123 failures, 21.2% pass rate) - Commutative property tests
4. **orderby** (62 failures, 33.3% pass rate) - ORDER BY with indexed columns

These four categories account for 371 of the 563 total failures (65.9%).

## Test Results by Category

Based on test results database analysis (as of 2025-11-10 08:52:56):

| Category       | Pass | Fail | Total | Pass Rate |
|----------------|------|------|-------|-----------|
| **orderby_nosort** | 0    | 147  | 147   | **0.0%**  |
| **in**             | 0    | 39   | 39    | **0.0%**  |
| **commute**        | 33   | 123  | 156   | 21.2%     |
| **orderby**        | 31   | 62   | 93    | 33.3%     |
| random         | 6    | 72   | 78    | 7.7%      |
| between        | 3    | 36   | 39    | 7.7%      |
| delete         | 3    | 39   | 42    | 7.1%      |
| view           | 3    | 45   | 48    | 6.2%      |
| **TOTAL**      | **79**   | **563**  | **642**   | **12.3%**     |

**Note**: The database contains 642 test executions for 214 unique test files, suggesting multiple test runs or multi-case test files.

## High-Impact Fix Opportunities

### Priority 1: orderby_nosort (147 failures)

**Impact**: Fixing this category alone could improve pass rate by ~23 percentage points.

**Test Description**: These tests verify that ORDER BY clauses can be optimized away when indexes already provide the desired ordering.

**Example Test File**: `third_party/sqllogictest/test/index/orderby_nosort/10/slt_good_14.test`

**Likely Issues**:
- Query optimizer not detecting index-provided ordering
- Missing implementation of ORDER BY elimination optimization
- Index traversal order not matching ORDER BY requirements
- ASC/DESC index handling in optimization logic

**Investigation Steps**:
1. Run a sample `orderby_nosort` test with detailed logging
2. Check if query planner recognizes index ordering
3. Verify index scan direction (ASC vs DESC)
4. Compare with passing `orderby` tests (33.3% pass rate)

### Priority 2: IN operator (39 failures)

**Impact**: Fixing this could add ~6 percentage points to pass rate.

**Test Description**: Tests for IN operator with indexed columns.

**Example Test File**: `third_party/sqllogictest/test/index/in/10/slt_good_1.test`

**Likely Issues**:
- IN operator not utilizing indexes for lookups
- Multiple key lookup implementation missing or broken
- Result ordering issues with IN + indexed columns
- NULL handling in IN operator with indexes

**Investigation Steps**:
1. Check if IN operator generates index lookups
2. Verify index scan vs table scan in execution plan
3. Test IN with single value vs multiple values
4. Check NULL handling in IN clauses

### Priority 3: commute (123 failures)

**Impact**: High volume of failures, but 21.2% already passing suggests partial implementation.

**Test Description**: Tests verifying commutative property of operations with indexes (e.g., `a = b` vs `b = a`).

**Example Test File**: `third_party/sqllogictest/test/index/commute/10/slt_good_24.test`

**Likely Issues**:
- Predicate normalization not handling all cases
- Column order in multi-column indexes
- Index selection when column order varies
- JOIN condition ordering with indexes

**What's Working**: 33/156 tests pass (21.2%), indicating basic commutative handling exists.

**Investigation Steps**:
1. Compare passing vs failing test patterns
2. Check predicate rewriting logic
3. Verify multi-column index usage
4. Test both single and multi-column index scenarios

### Priority 4: orderby (62 failures)

**Impact**: Best performing category (33.3%), focused fixes could push to 50%+.

**Test Description**: ORDER BY clauses with indexed columns.

**Example Test File**: `third_party/sqllogictest/test/index/orderby/10/slt_good_14.test`

**Likely Issues** (remaining 62 failures):
- Complex ORDER BY expressions
- Mixed ASC/DESC ordering
- NULL positioning (NULLS FIRST/LAST)
- ORDER BY with non-indexed columns

**What's Working**: 31/93 tests pass, basic ORDER BY + index integration works.

**Investigation Steps**:
1. Categorize failing tests by ORDER BY complexity
2. Test simple vs compound ORDER BY clauses
3. Verify NULL ordering behavior
4. Check DESC index support

## Lower Priority Categories

### between (36 failures, 7.7% pass rate)

**Test Description**: BETWEEN operator with indexed columns.

**Likely Issues**:
- Range scan implementation
- Inclusive vs exclusive bounds
- NULL handling in ranges

### delete (39 failures, 7.1% pass rate)

**Test Description**: DELETE statements with indexed columns.

**Likely Issues**:
- Index maintenance during DELETE
- WHERE clause optimization with indexes
- Multi-index consistency

### view (45 failures, 6.2% pass rate)

**Test Description**: Views with indexed base tables.

**Likely Issues**:
- View-through index optimization
- Index hints not propagating through views
- Materialization vs inline expansion

### random (72 failures, 7.7% pass rate)

**Test Description**: Randomized tests covering various index scenarios.

**Likely Issues**:
- Combination of multiple patterns above
- Edge cases and corner cases
- Complex multi-table scenarios

## Historical Context

### Recent Improvements

From git history:

**PR #1195** (2025-11-10): Removed duplicate hash logic from SQLLogicTest adapters
- Fixed: Database adapter and library were both hashing results
- Impact: Improved pass rate from 3.86% to significant improvement
- Files: `tests/sqllogictest_runner.rs`, `tests/sqllogictest/db_adapter.rs`

**PR #1194** (2025-11-10): Used IndexSet for DISTINCT to preserve insertion order
- Fixed: DISTINCT not preserving insertion order
- Impact: ORDER BY + DISTINCT tests now passing
- Files: executor module

**Combined Impact**: Pass rate improved from ~7.5% to 21.96% (nearly 3x improvement)

## Recommended Implementation Strategy

### Phase 1: Quick Wins (Target: +10 percentage points)

**Focus**: IN operator (0% → 50%+)
1. Implement basic IN operator index lookup
2. Test with single-column indexes
3. Handle NULL cases

**Estimated Impact**: +3-5 percentage points

### Phase 2: Major Impact (Target: +20 percentage points)

**Focus**: orderby_nosort (0% → 30%+)
1. Implement ORDER BY elimination optimization
2. Detect when index provides required ordering
3. Handle single-column ORDER BY first
4. Add multi-column support

**Estimated Impact**: +7-10 percentage points

### Phase 3: Incremental Improvements (Target: +15 percentage points)

**Focus**: Improve existing categories
1. commute: 21% → 40% (+10 passing)
2. orderby: 33% → 50% (+16 passing)
3. between: 8% → 30% (+8 passing)

**Estimated Impact**: +5-8 percentage points each

### Phase 4: Remaining Categories (Target: reach 80%+)

**Focus**: delete, view, random
1. Fix systematic issues identified in Phases 1-3
2. Address category-specific problems
3. Handle edge cases

## Testing Strategy

### Verification Approach

For each fix:

1. **Unit Test**: Create regression test for specific issue
2. **Category Test**: Run full category test suite
3. **Cross-Category Test**: Verify no regressions in other categories
4. **Full Suite**: Run all 214 index tests

### Test Execution Commands

```bash
# Run all index tests (subset by time budget)
SQLLOGICTEST_TIME_BUDGET=300 cargo test test_sqllogictest_suite --release -- --nocapture

# Query results by category
./scripts/query_test_results.py --query "
  SELECT subcategory, status, COUNT(*)
  FROM test_files
  WHERE category='index'
  GROUP BY subcategory, status
  ORDER BY subcategory, status
"

# Analyze failure patterns
./scripts/analyze_test_failures.py
```

### Regression Detection

After each change:

```bash
# Check for regressions
./scripts/detect_test_regressions.py

# Compare pass rates
./scripts/query_test_results.py --preset by-category | grep index
```

## Next Steps

1. ✅ **Analysis Complete**: This document
2. ⏳ **Create Sub-Issues**: Break down high-impact categories into targeted issues
3. ⏳ **Prototype Fix**: Start with IN operator (smallest, 0% pass rate, clear scope)
4. ⏳ **Measure Impact**: Run tests and update pass rate metrics
5. ⏳ **Iterate**: Apply learnings to next category

## Success Metrics

- **Minimum Acceptable**: 80% pass rate (171/214 tests passing, +124 tests)
- **Stretch Goal**: 90% pass rate (193/214 tests passing, +146 tests)
- **Current Progress**: 47/214 (21.96%)

## Files to Investigate

Based on recent fixes and test infrastructure:

- **Query Execution**: `executor/src/select.rs` (ORDER BY logic)
- **Index Operations**: `storage/src/btree_index.rs` (index traversal)
- **Query Optimizer**: `executor/src/optimizer/` (if it exists)
- **Test Adapter**: `tests/sqllogictest_runner.rs` (result formatting)
- **DB Adapter**: `tests/sqllogictest/db_adapter.rs` (query execution)

## Appendix: Test File Locations

```
third_party/sqllogictest/test/index/
├── between/       (39 tests, 7.7% pass)
├── commute/       (156 tests, 21.2% pass)
├── delete/        (42 tests, 7.1% pass)
├── in/            (39 tests, 0.0% pass)
├── orderby/       (93 tests, 33.3% pass)
├── orderby_nosort/(147 tests, 0.0% pass)
├── random/        (72 tests, 7.7% pass)
└── view/          (48 tests, 6.2% pass)
```

Total: 214 unique test files (642 test executions recorded)

---

**Analysis prepared by**: Claude Code Builder Agent
**For**: Issue #1198 - Improve SQLLogicTest pass rate: 167 failing index/* tests
**Repository**: vibesql
