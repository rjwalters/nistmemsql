# SQLLogicTest 100% Conformance - Punchlist

**Status**: 83/623 passing (13.3%) - 530 failing - 10 untested
**Goal**: 623/623 (100%)
**Last Updated**: 2025-11-08

## Quick Start

Test one file at a time to identify and fix root causes:

```bash
# Test first failing file
./scripts/test_one_file.sh index/delete/10/slt_good_0.test

# Or by test pattern
./scripts/test_one_file.sh index::delete::10::slt_good_0

# Run all tests (takes ~2 hours on 64 workers)
./scripts/run_remote_sqllogictest.sh
```

## Current Status by Category

| Category | Total | Passing | Failing | Pass Rate |
|----------|-------|---------|---------|-----------|
| **index** | 214 | 75 | 132 | 35.0% |
| **evidence** | 12 | 6 | 6 | 50.0% |
| **random** | 391 | 2 | 386 | 0.5% |
| **ddl** | 1 | 0 | 1 | 0.0% |
| **other** | 5 | 0 | 5 | 0.0% |
| **TOTAL** | **623** | **83** | **530** | **13.3%** |

## What's Passing ✓

These categories are mostly working and should guide fixes:

### Index Tests (75 passing)
- ✓ `index/between/*` (12 files) - BETWEEN clause working
- ✓ `index/in/*` (11 files) - IN clause working
- ✓ `index/commute/*` (35 files) - Commutative operations working

### Evidence Tests (6 passing)
- ✓ Basic language features (some files passing)

## What's Failing ✗

### High-Impact Categories to Fix

**1. Index/Delete (13 files failing)**
- Pattern: `index/delete/*/slt_good_*.test`
- Likely Issue: NOT operator in WHERE clauses
- Effort: Medium
- Priority: HIGH

**2. Index/OrderBy (129 files failing)**
- Pattern: `index/orderby*/*/slt_good_*.test`
- Likely Issue: Result ordering or hash mismatches
- Effort: Medium to High
- Priority: VERY HIGH (largest failing category in index tests)

**3. Random Tests (386 files failing)**
- Pattern: `random/*/slt_good_*.test`
- Likely Issues:
  - Decimal formatting (7.000 vs 7)
  - Multi-row formatting
  - Missing aggregate functions
  - Type mismatches
- Effort: High (complex queries)
- Priority: MEDIUM (after fixing index tests)

### Known Issues to Address

From `SQLLOGICTEST_ISSUES.md`:

1. **Aggregate functions** (448 occurrences)
   - Aggregate functions outside aggregation context
   - COUNT/SUM/MAX/MIN not properly scoped

2. **NOT operator** (192 occurrences)
   - NOT in WHERE clauses not supported
   - Affects DELETE statements

3. **Type mismatches** (64 occurrences)
   - Boolean values in arithmetic operations
   - Division by zero handling

## Testing Strategy

### Phase 1: Fix Index Tests (Current Focus)
These are most likely to succeed with targeted fixes:

1. **Test index/delete/* files** - Identify NOT operator issue
2. **Test index/orderby/* files** - Identify ordering/hash issues
3. **Test other index/* files** - Validate fixes don't break passing tests

Expected impact: +50-75 tests passing (improving from 13.3% to 25-40%)

### Phase 2: Fix Random Tests
Once index tests are stable, tackle random tests with:

1. Decimal formatting fixes (issue #956)
2. Multi-row formatting fixes (issue #957)
3. Aggregate function context fixes

Expected impact: +200-300 tests passing (reaching 60-80% pass rate)

### Phase 3: Remaining Issues
- Type coercion improvements
- Missing SQL functions
- Parse error handling

Expected impact: Final push to 100%

## Punchlist Files

Generated artifacts in `target/`:

- `sqllogictest_punchlist.csv` - Full list of all 623 test files with status
- `sqllogictest_punchlist.json` - Same data in JSON format
- `sqllogictest_cumulative.json` - Aggregated test results from last run

## Tools

### Generate Punchlist
```bash
python3 scripts/generate_punchlist.py
```

Scans all test files and generates prioritized lists.

### Test One File
```bash
./scripts/test_one_file.sh [test_file_or_pattern]
```

Quick way to test individual files and see what breaks.

Examples:
```bash
./scripts/test_one_file.sh index/delete/10/slt_good_0.test
./scripts/test_one_file.sh index::delete::10::slt_good_0
./scripts/test_one_file.sh  # Shows first 10 failing tests
```

### Analyze Results
```bash
python3 scripts/analyze_test_failures.py
```

Identifies high-impact fix opportunities through failure clustering and pattern analysis.

## Workflow for Fixing One Category

1. **Identify failing test**:
   ```bash
   ./scripts/test_one_file.sh index/delete/10/slt_good_0.test
   ```

2. **Read error message** - Note the specific failure reason

3. **Locate code** - Use grep/finder to locate relevant code:
   ```bash
   # Search for NOT operator handling
   grep -r "NOT" crates/parser/src/*.rs
   grep -r "WHERE" crates/executor/src/*.rs
   ```

4. **Fix the issue** - Implement the necessary change

5. **Test the fix**:
   ```bash
   ./scripts/test_one_file.sh index/delete/10/slt_good_0.test
   ```

6. **Check for regressions** - Ensure passing tests still pass:
   ```bash
   ./scripts/test_one_file.sh index/between/10/slt_good_0.test
   ```

7. **Test all in category** - Once a few pass:
   ```bash
   cargo test --test sqllogictest_suite --release -- index::delete
   ```

## Next Steps

1. **Pick first issue**: Start with `index/delete/*` or `index/orderby/*`
2. **Run test**: `./scripts/test_one_file.sh index/delete/10/slt_good_0.test`
3. **Read error**: Understand what specific feature is missing
4. **Fix code**: Implement support for that feature
5. **Verify**: Re-run test, check no regressions
6. **Move to next**: Repeat until category is fixed

## Progress Tracking

After each major fix category, run full suite to update punchlist:

```bash
# Full suite (2 hours, 64 workers)
./scripts/run_remote_sqllogictest.sh

# Update punchlist
python3 scripts/generate_punchlist.py
```

Then update this document with new pass rate.

## References

- **Main Roadmap**: `SQLLOGICTEST_ROADMAP.md`
- **Issues & Analysis**: `SQLLOGICTEST_ISSUES.md`
- **Testing Guide**: `TESTING.md`
- **GitHub Issues**: #956-963 for specific issues

---

**Strategy**: Fix root causes in high-impact categories, not individual test files.
Each category fixed = 10-50 tests passing instead of fixing 1 test at a time.
