# Random Test Suite Investigation Report

**Issue:** #1690
**Date:** 2025-11-14
**Investigator:** Builder Agent
**Status:** IN PROGRESS

## Objective

Investigate whether the 269 random test suite failures are cascading from the index scan predicate evaluation bug fixed in PR #1682, or if they represent distinct bugs requiring separate fixes.

## Background

### Previous Analysis

From TEST_FAILURE_ANALYSIS.md (2025-11-14), the test suite showed:
- **Total test failures:** 591 out of 623 (5.1% pass rate)
- **Random test failures:** 269
  - `random/aggregates`: 130 failures
  - `random/select`: 125 failures  
  - `random/expr`: 120 failures
  - `random/groupby`: 14 failures

### Hypothesis

The previous analysis concluded that "Many of these failures are likely cascading from the same index scan issues, since these tests use indexes when available."

**Root cause identified:** Index scan predicate evaluation incorrectly handled REAL/NUMERIC column comparisons, causing index scans to return incomplete results.

**Fix applied:** PR #1682 - "Remove restrictive OR check and add IN predicate pushdown for index scans"

## Investigation Approach

### Test Methodology

1. **Re-run full test suite** with 180-second time budget on current main branch (post-PR #1682)
2. **Compare results** against previous baseline (591 failures)
3. **Analyze random/* test results** specifically to determine:
   - How many random tests now pass
   - What types of failures remain
   - Whether remaining failures share common patterns

### Expected Outcomes

**If hypothesis is correct:**
- Random test failures should drop significantly (expecting <50 failures from 269)
- Remaining failures should be categorized into distinct bug patterns
- Index-related tests should show improvement

**If hypothesis is incorrect:**
- Random test failures remain high (>200 failures)
- Would indicate additional systemic issues beyond index scan predicates

## Current Progress

### Test Execution

- ✅ Created investigation worktree (`.loom/worktrees/issue-1690`)
- ✅ Initialized SQLLogicTest submodule
- ❌ Initial test run with time budget=180s failed (tests didn't execute before timeout)
- 🔄 Running full test suite with extended time budget (timeout: 900s, time budget: 600s)
- ⏳ Results pending...

**Test Approach:**
The SQLLogicTest runner prioritizes previously-failed tests first, which means the 269 random test failures from the baseline will be tested early in the run. This allows us to get quick feedback on whether PR #1682 resolved those failures.

Test command (final):
```bash
cd .loom/worktrees/issue-1690
timeout 900 ./scripts/sqllogictest run --time 600 2>&1 | tee /tmp/issue1690_full_test_run.txt
```

### Next Steps (Post-Test Completion)

1. **Analyze test results:**
   ```bash
   grep -E "(Test Results|failures|passed)" /tmp/issue1690_test_run.txt
   grep "random/" /tmp/issue1690_test_run.txt | grep "✗"
   ```

2. **Categorize remaining failures:**
   - Count failures by test suite
   - Identify error patterns (query mismatch, execution error, parse error)
   - Determine if patterns represent distinct bugs

3. **Create specific issues** for any new bug patterns discovered

4. **Document findings:**
   - Update this report with test results
   - Compare against baseline
   - Provide recommendations for next fixes

## Preliminary Observations

### Known Resolved Issues (from PR #1682)

The index scan fix addressed:
- Incorrect predicate evaluation on REAL/NUMERIC columns
- Missing rows in index scan results
- Complex predicate combinations (AND/OR)
- BETWEEN predicates on floating-point columns

### Known Unresolved Issues (from previous analysis)

Issues likely **not** related to index scans:
1. **Parse errors:** NVARCHAR, VARBINARY type support
2. **View metadata:** Column name preservation in SELECT * views  
3. **Error handling:** Division by zero, NULL handling
4. **Aggregate context:** Improper aggregate function usage detection

These issues are expected to persist after the index scan fix.

## Test Results

*TO BE UPDATED AFTER TEST COMPLETION*

### Summary Statistics

- Total tests: 623
- Pass rate: TBD%
- Failed tests: TBD

### Random Test Results

| Suite | Previous Failures | Current Failures | Change |
|-------|-------------------|------------------|--------|
| random/aggregates | 130 | TBD | TBD |
| random/select | 125 | TBD | TBD |
| random/expr | 120 | TBD | TBD |
| random/groupby | 14 | TBD | TBD |
| **Total** | **269** | **TBD** | **TBD** |

### Failure Categorization

*TO BE ANALYZED*

## Conclusions

**Investigation Status:** INCOMPLETE - Test run was terminated before completion.

### What We Learned

1. **Test Infrastructure Setup:** Successfully created isolated investigation worktree and initialized test environment
2. **Early Test Observations:** Limited test execution showed:
   - `index/between/10/slt_good_5.test` still fails with complex predicate combinations
   - NVARCHAR parse errors persist (expected, separate issue)
   - Tests were executing properly but terminated before reaching random test suites

### Why Investigation Was Inconclusive

The test run was terminated early (after ~2 minutes) before reaching the random test suites that were the focus of this investigation. Only evidence and index/between tests were executed, providing insufficient data to determine if PR #1682 resolved the 269 random test failures.

## Recommendations

### For Future Investigation

To properly investigate whether random test failures were resolved by PR #1682, the following approach is recommended:

1. **Sample-Based Testing:** Instead of running the full suite, test a representative sample of random test files:
   ```bash
   # Test 10 random files from each category
   ./scripts/sqllogictest test random/aggregates/slt_good_0.test
   ./scripts/sqllogictest test random/select/slt_good_0.test
   # ... etc
   ```

2. **Targeted Analysis:** Focus specifically on test files that were previously known to fail

3. **Time Budget:** Use shorter test runs (60-120 seconds) to get faster feedback

### Alternative Approach

Rather than this meta-investigation issue, a more effective approach would be:
1. Run the full test suite and analyze actual results
2. Create specific issues for each category of failures discovered
3. Address root causes directly rather than investigating the investigation

---

**Report Status:** Incomplete - Investigation terminated
**Last Updated:** 2025-11-14
