# Investigation Report: Issue #1689
# Core SELECT Test Failures (select1-4.test)

**Date:** 2025-11-14
**Investigator:** Builder Agent
**Status:** Initial investigation complete, root cause still unknown

---

## Executive Summary

Initial investigation into the select1-4.test failures revealed that the original hypothesis was **incorrect**. These tests do NOT use IN predicates and are NOT caused by the index scan bug in issue #1685. The actual root cause remains unknown and requires deeper investigation into subquery execution, aggregations, or other query execution paths.

## Key Findings

### 1. Original Hypothesis Was Wrong ❌

**Claim:** Failures caused by IN predicate index scan bug (#1685)

**Reality:** 
```bash
$ grep -c ' IN (' third_party/sqllogictest/test/select*.test
select1.test: 0 IN predicates  
select2.test: 0 IN predicates
select3.test: 0 IN predicates
select4.test: 0 IN predicates
```

**Conclusion:** Issue #1689 is NOT blocked by #1685

### 2. Test Characteristics

| Test | Size | Indexes | Primary Features |
|------|------|---------|------------------|
| select1.test | 12,188 lines | 0 | Correlated subqueries, CASE, EXISTS |
| select2.test | 11,218 lines | 0 | Multi-table queries, JOINs |
| select3.test | 40,769 lines | 0 | Complex WHERE, aggregations |
| select4.test | 48,300 lines | 16 | Set operations (UNION, EXCEPT, INTERSECT) |

**Total:** 112,475 lines of test coverage

### 3. Actual Query Patterns Found

The tests contain complex SQL features:

- **Correlated subqueries:** `(SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)`
- **Nested subqueries in WHERE:** `WHERE c>(SELECT avg(c) FROM t1)`
- **CASE statements:** Complex conditional logic
- **EXISTS clauses:** Existential subqueries
- **BETWEEN predicates:** Range comparisons  
- **Complex ORDER BY:** Multi-column sorting
- **Hash validation:** Results compared by hash

**Example query from select1.test:**
```sql
SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2+c*3,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       c,
       b-c
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
    OR a>b
    OR d NOT BETWEEN 110 AND 150
 ORDER BY 5,3,6,1,2,4
```

### 4. Test Status Confirmed

All four tests are failing:
- ✗ select1.test
- ✗ select2.test
- ✗ select3.test
- ✗ select4.test

## Possible Root Causes (Hypotheses to Test)

1. **Subquery Execution Bug**
   - Correlated subqueries returning wrong results
   - Nested subqueries not properly isolated
   - Context not properly passed to subqueries

2. **Aggregation Issues**
   - COUNT(*), AVG(), SUM() incorrect results
   - Window context issues
   - Grouping bugs

3. **ORDER BY Problems**
   - Multi-column sorting incorrect
   - NULL handling in sorts
   - Tie-breaking logic

4. **CASE Statement Evaluation**
   - Condition evaluation order
   - Type coercion in CASE
   - NULL handling

5. **EXISTS Clause Handling**
   - Existential check logic
   - Short-circuit evaluation
   - Correlation with outer query

6. **Result Comparison Issues**
   - Hash generation bugs
   - Row ordering before hashing
   - Type representation in hashing

7. **Query Optimizer**
   - Incorrect query plans
   - Optimization bugs for complex queries
   - Subquery flattening issues

## Investigation Methodology

### Steps Completed ✓

1. ✓ Verified test files exist and their sizes
2. ✓ Checked for IN predicates (found none)
3. ✓ Counted CREATE INDEX statements
4. ✓ Analyzed query patterns in test files  
5. ✓ Confirmed all tests are failing
6. ✓ Updated issue description with corrected information
7. ✓ Removed `loom:blocked` label (not blocked by #1685)

### Next Steps Required

1. **Capture Specific Failures**
   - Run with SQLLOGICTEST_VERBOSE=1
   - Extract first failing query from each test
   - Get actual vs. expected results

2. **Create Minimal Reproductions**
   - Isolate single failing query
   - Create standalone test case
   - Determine minimum complexity to reproduce

3. **Isolate Feature Area**
   - Test subqueries in isolation
   - Test aggregations in isolation
   - Test ORDER BY in isolation
   - Identify which feature is buggy

4. **Debug Root Cause**
   - Add logging to executor
   - Step through failing query execution
   - Identify exact point of failure

5. **Implement Fix**
   - Fix identified bug
   - Add regression tests
   - Verify all four tests pass

## Recommendations

### Immediate Actions

1. **Continue Investigation:** Don't wait for #1685, proceed independently
2. **Capture Verbose Output:** Get detailed failure info from one test
3. **Focus on select1.test First:** Smallest file, likely simplest failures

### Investigation Strategy

**Option A: Systematic Feature Testing**
- Test each SQL feature in isolation
- Create small reproduction cases
- Identify which feature is broken

**Option B: Verbose Output Analysis**
- Run select1.test with full verbosity
- Analyze first few failures in detail
- Find common pattern

**Recommended:** Start with Option B, fall back to Option A if no pattern emerges

### Resource Estimates

- **Minimal investigation (capture failures):** 30-60 minutes
- **Root cause identification:** 2-4 hours
- **Fix implementation:** 1-3 hours  
- **Verification and testing:** 1-2 hours

**Total estimate:** 4-10 hours of focused work

## Files Modified

- None yet - investigation only

## Git Branch

- Branch: `feature/issue-1689`
- Worktree: `.loom/worktrees/issue-1689`
- Status: Investigation in progress

## Related Issues

- #1674 - SQLLogicTest Suite: Path to 100% Pass Rate (blocked by this)
- #1690 - Random test suite failures (may share root cause)
- ~~#1685 - Index scan predicate bug (NOT RELATED)~~

## Conclusion

The original hypothesis that these failures were caused by IN predicate issues was incorrect. These are complex SQL tests involving subqueries, aggregations, and advanced query features. The root cause is unknown and requires systematic investigation starting with capturing detailed failure output.

The issue has been unblocked and updated with correct information. Investigation should continue to identify and fix the actual bug(s) causing these failures.

---

**Next investigator:** Should start by running select1.test with SQLLOGICTEST_VERBOSE=1 to capture detailed failure information, then create minimal reproduction cases.
