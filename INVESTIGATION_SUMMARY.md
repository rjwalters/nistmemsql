# Random/* SQLLogicTest Failures - Investigation Summary

**Issue**: #1919
**Date**: 2025-11-16
**Current Status**: In Progress

## Executive Summary

Investigation of the random/* test category reveals that **significant progress has already been made** through recent PRs. The pass rate has improved from 3.3% to 23.3% (91/391 tests passing). The remaining 300 failures require further analysis.

## Current State

### Overall Statistics
- **Total random tests**: 391
- **Passing**: 91 (23.3%)
- **Failing**: 300 (76.7%)
- **Goal**: >50% pass rate (~195 passing tests)
- **Tests needed**: ~104 more passing tests

### Breakdown by Subcategory
| Subcategory | Passing | Failing | Total | Pass Rate |
|-------------|---------|---------|-------|-----------|
| expr        | 103     | 17      | 120   | 85.8%     |
| groupby     | 10      | 4       | 14    | 71.4%     |
| select      | 30      | 97      | 127   | 23.6%     |
| aggregates  | 2       | 128     | 130   | 1.5%      |

## Major Issues Already Fixed

### 1. BETWEEN NULL Handling (PR #1846, #1907)
**Impact**: ~92% of original random/aggregates failures
**Status**: ✅ Fixed (mostly)

- Fixed BETWEEN NULL returning FALSE instead of NULL
- Fixed NOT BETWEEN NULL in WHERE clauses
- Implemented proper SQL three-valued logic

### 2. Aggregate NULL Handling (PR #1801, #1876)
**Impact**: ~7-8% of original failures
**Status**: ✅ Fixed

- SUM/AVG now return NULL for empty sets (not 0)
- NULL properly propagates through aggregate expressions
- Fixed CAST(NULL AS SIGNED) arithmetic

### 3. Other NULL-Related Fixes
**Status**: ✅ Fixed

- #1871: IN/NOT IN with CASE aggregates returning NULL
- #1864: Simple CASE NULL comparison semantics
- #1878: GROUP BY and HAVING NULL handling

## Remaining Failures Analysis

### High-Performing Categories
1. **random/expr**: 85.8% pass rate - mostly working! ✅
2. **random/groupby**: 71.4% pass rate - mostly working! ✅

### Problem Categories
1. **random/aggregates**: 1.5% pass rate (128 failures) ⚠️
   - Despite fixes, still very low pass rate
   - May have additional edge cases or different issues

2. **random/select**: 23.6% pass rate (97 failures) ⚠️
   - Significant number of failures
   - May involve JOIN, subquery, or complex query issues

## Next Steps

### Immediate Actions
1. ✅ Complete this analysis
2. 🔄 Run sample tests from each category to identify current failure patterns
3. ⏳ Categorize remaining failures by error type
4. ⏳ Identify high-impact fixes (bugs affecting multiple tests)
5. ⏳ Implement fixes
6. ⏳ Verify pass rate improvement

### Investigation Plan
```bash
# Test samples to identify current patterns
SQLLOGICTEST_FILE="third_party/sqllogictest/test/random/aggregates/slt_good_0.test" \
    cargo test --release -p vibesql --test sqllogictest_runner run_single_test_file

SQLLOGICTEST_FILE="third_party/sqllogictest/test/random/select/slt_good_0.test" \
    cargo test --release -p vibesql --test sqllogictest_runner run_single_test_file
```

## Key Findings

### What's Working
- ✅ NULL handling in aggregates (SUM, AVG, etc.)
- ✅ BETWEEN NULL three-valued logic
- ✅ Most expression evaluation (85.8% pass rate!)
- ✅ Most GROUP BY / HAVING queries (71.4% pass rate!)

### What's Not Working
- ⚠️ random/aggregates still has very low pass rate despite fixes
- ⚠️ random/select has significant failures (likely JOIN/subquery issues)
- ⚠️ Need to identify new failure patterns post-fixes

### Hypothesis for Remaining Failures
Based on the low random/aggregates pass rate despite BETWEEN NULL and aggregate NULL fixes:

1. **Edge cases** in the fixes (corner cases not covered)
2. **Other SQL features** that are broken (not BETWEEN/aggregate related)
3. **Complex interactions** between features
4. **Test harness issues** or test file problems

## Resources

### Documentation Reviewed
- `RANDOM_TEST_ANALYSIS.md` - Baseline analysis (3.3% pass rate)
- `docs/testing/aggregate_architecture_analysis.md` - Architecture review
- `docs/testing/random_aggregates_errors.md` - Error pattern analysis (pre-fixes)
- Issue #1841 - Overall SQLLogicTest coverage tracking

### Related Issues & PRs
- #1846 - BETWEEN NULL in HAVING (MERGED)
- #1907 - NOT BETWEEN NULL in WHERE (MERGED)
- #1801 - NULL aggregate returns (MERGED)
- #1876 - Aggregate NULL values (CLOSED)
- #1878 - GROUP BY/HAVING NULL (CLOSED)
- #1871 - IN/NOT IN with CASE aggregates (MERGED)

### Database Analysis
- Database: `~/.vibesql/test_results/sqllogictest_results.sql`
- Last updated: 2025-11-16 07:23
- Contains test results and status for all SQLLogicTest files

## Recommendations

### Priority 1: Identify Current Failures
Run tests and capture errors to understand what's actually failing NOW (post-fixes):
- Sample from random/aggregates (128 failures)
- Sample from random/select (97 failures)

### Priority 2: Pattern Recognition
Group failures by:
- Error message type
- SQL feature involved
- Affected test count

### Priority 3: High-Impact Fixes
Focus on:
- Errors affecting 10+ tests
- Issues in random/select (97 failures - high impact)
- Edge cases in random/aggregates

## Status
- [x] Historical analysis complete
- [x] Recent fixes documented
- [ ] Current failure patterns identified
- [ ] High-impact issues fixed
- [ ] Pass rate >50% achieved

---
*Last updated: 2025-11-16*
