# SQLLogicTest 100% Conformance - Punchlist Setup

This directory now has a complete punchlist system for achieving 100% SQLLogicTest conformance.

## Current Status

- **Total Test Files**: 623
- **Passing**: 83 (13.3%)
- **Failing**: 530 (85.1%)
- **Untested**: 10 (1.6%)

## Files Generated

### Primary Punchlist Documents
1. **`PUNCHLIST_100_CONFORMANCE.md`** - Master strategic guide
   - Overview of current status
   - Category breakdown with pass rates
   - Known issues from investigation
   - Testing strategy and workflow
   - How to fix issues systematically

2. **`target/sqllogictest_punchlist.csv`** - Full test list (CSV format)
   - All 623 test files
   - Status: PASS, FAIL, or UNTESTED
   - Category and subcategory
   - Priority ranking (failing tests first)
   - Easy to import into spreadsheet/tools

3. **`target/sqllogictest_punchlist.json`** - Full test list (JSON format)
   - Programmatic access to test data
   - Can be processed by scripts

### Reference Documents
- **`SQLLOGICTEST_ROADMAP.md`** - High-level roadmap to 100%
- **`SQLLOGICTEST_ISSUES.md`** - Known issues from investigation
- **`TESTING.md`** - Comprehensive testing strategy

## Scripts Created

### 1. `scripts/generate_punchlist.py`
**Purpose**: Generate/regenerate the punchlist from test files

**Usage**:
```bash
python3 scripts/generate_punchlist.py
```

**What it does**:
- Scans all 623 test files in `third_party/sqllogictest/test/`
- Loads existing test results from `target/sqllogictest_cumulative.json`
- Marks each file as PASS, FAIL, or UNTESTED
- Generates CSV and JSON outputs
- Provides summary statistics

**When to use**:
- After running full test suite to update status
- To see current breakdown by category
- To refresh punchlist data

### 2. `scripts/test_one_file.sh`
**Purpose**: Quick testing of individual files to identify issues

**Usage**:
```bash
./scripts/test_one_file.sh index/delete/10/slt_good_0.test
./scripts/test_one_file.sh index::delete::10::slt_good_0
./scripts/test_one_file.sh  # Shows first 10 failing tests
```

**What it does**:
- Tests a single SQLLogicTest file
- Supports multiple input formats (path, test pattern)
- Shows specific error messages
- Suggests next file to test if current one passes

**When to use**:
- When debugging a specific test failure
- To understand what feature is missing
- To verify a fix works

### 3. `scripts/test_file_direct.sh`
**Purpose**: Direct testing without cargo test framework

**Usage**:
```bash
./scripts/test_file_direct.sh index/delete/10/slt_good_0.test
```

**Note**: Requires `sqllogictest` CLI tool installed. Falls back to showing file contents if not available.

## Quick Start Workflow

### 1. Review the Punchlist
```bash
cat PUNCHLIST_100_CONFORMANCE.md
```
This gives you the strategic overview.

### 2. Check Current Status
```bash
head -20 target/sqllogictest_punchlist.csv
grep "FAIL" target/sqllogictest_punchlist.csv | head -20
```

### 3. Pick a Failing Test
Start with index tests (higher success rate potential):
```bash
./scripts/test_one_file.sh index/delete/10/slt_good_0.test
```

### 4. Read the Error
The test output will show:
- The SQL that failed
- What was expected vs what we got
- The specific error (missing feature, formatting issue, etc.)

### 5. Find and Fix Code
Use finder/grep to locate relevant code:
```bash
grep -r "NOT" crates/parser/src/
grep -r "DELETE" crates/executor/src/
```

### 6. Test Again
```bash
./scripts/test_one_file.sh index/delete/10/slt_good_0.test
```

### 7. Check for Regressions
Test a file that was passing to ensure fix didn't break it:
```bash
./scripts/test_one_file.sh index/between/10/slt_good_0.test
```

## Priority Order for Fixing

Based on impact and likelihood of success:

### Phase 1: Index Tests (Start here)
**Expected improvement**: 13.3% → 30-40%

1. **index/delete*** (13 files) - Medium effort
   - Issue: NOT operator in WHERE clauses
   - Once fixed: All delete tests should pass

2. **index/orderby*** (129 files) - Medium to high effort
   - Issue: Result ordering or hash mismatches
   - Largest failing category in index tests
   - Once fixed: Major pass rate improvement

3. **Other index/** (20 files) - Low to medium effort
   - Various subcategories with failures
   - Test each to ensure fixes don't break things

### Phase 2: Random Tests
**Expected improvement**: 40% → 70-80%

1. Decimal formatting issues (issue #956)
2. Multi-row formatting (issue #957)
3. Aggregate function context

### Phase 3: Remaining Issues
**Expected improvement**: 80% → 100%

1. Type coercion improvements
2. Missing SQL functions (NULLIF, COALESCE, etc.)
3. Parse error handling

## Category Breakdown

```
Category          Total  Passing  Failing  Pass%
────────────────────────────────────────────────
index              214      75      132    35.0%
  ├─ between        12      12        0   100%  ✓
  ├─ commute        35      35        0   100%  ✓
  ├─ in             11      11        0   100%  ✓
  ├─ delete         13       0       13     0%  ✗
  ├─ orderby       127       0      127     0%  ✗
  └─ other          16      17        5    77%  ~
────────────────────────────────────────────────
evidence           12       6        6    50.0%
random            391       2      386     0.5%  ✗✗✗
ddl                 1       0        1     0.0%
other               5       0        5     0.0%
────────────────────────────────────────────────
TOTAL             623      83      530   13.3%
```

## Known Issues

From previous investigation (`SQLLOGICTEST_ISSUES.md`):

1. **NOT operator** (192 occurrences)
   - NOT in WHERE clauses not supported
   - Affects DELETE statements
   - **Fix location**: `crates/executor/src/where_clause.rs` or similar

2. **Aggregate functions** (448 occurrences)
   - Functions outside aggregation context
   - COUNT/SUM/MAX/MIN not properly scoped
   - **Fix location**: `crates/executor/src/aggregate.rs`

3. **Type mismatches** (64 occurrences)
   - Boolean in arithmetic operations
   - **Fix location**: `crates/types/src/operations.rs`

4. **Decimal formatting** (56 occurrences)
   - Formatting as "7.000" vs "7"
   - **Fix location**: `crates/executor/src/result_formatter.rs`

## Testing the Full Suite

When you want to see overall progress after fixes:

```bash
# Full suite (2 hours, requires 64 workers)
./scripts/run_remote_sqllogictest.sh

# Update punchlist
python3 scripts/generate_punchlist.py

# See updated status
cat PUNCHLIST_100_CONFORMANCE.md
```

## Success Criteria

Each phase is successful when:

- ✓ Pick one failing test
- ✓ Understand the root cause
- ✓ Fix the code
- ✓ All tests in that category pass
- ✓ No regressions in other categories
- ✓ Full suite shows improvement

## Tools & Documentation

**In this repo**:
- `scripts/generate_punchlist.py` - Generate punchlist
- `scripts/test_one_file.sh` - Test one file
- `PUNCHLIST_100_CONFORMANCE.md` - Strategic guide
- `SQLLOGICTEST_ROADMAP.md` - Detailed roadmap
- `SQLLOGICTEST_ISSUES.md` - Known issues

**Useful commands**:
```bash
# See all failing tests in one category
grep "orderby.*FAIL" target/sqllogictest_punchlist.csv

# Count by category
grep FAIL target/sqllogictest_punchlist.csv | cut -d, -f2 | sort | uniq -c

# Find test files with specific error
grep -l "NOT" third_party/sqllogictest/test/index/delete/*/slt_good_*.test | head -5
```

## Next Action

1. Read `PUNCHLIST_100_CONFORMANCE.md`
2. Run `./scripts/test_one_file.sh` for first failing test
3. Find the specific error in output
4. Search code for relevant feature
5. Implement the fix
6. Test again to verify

Let's get to 100% conformance! 🚀
