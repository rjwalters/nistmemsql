# SQLLogicTest Punchlist - Complete Manifest

**Status**: ✓ Complete and ready for use
**Current**: 83/623 tests passing (13.3%)
**Target**: 623/623 (100%)
**Built**: 2025-11-08

## All Deliverables

### 1. Strategic Documents

#### QUICK_START.md (159 lines)
- **Purpose**: 2-minute overview to get you started
- **Contains**: Overview, quick commands, examples
- **Read First**: Yes
- **Time**: 2 minutes

#### PUNCHLIST_100_CONFORMANCE.md (218 lines)
- **Purpose**: Master strategic guide for achieving 100%
- **Contains**: Status breakdown, known issues, workflow, priority order
- **Read First**: Second (after QUICK_START)
- **Time**: 10 minutes

#### PUNCHLIST_README.md (268 lines)
- **Purpose**: Complete documentation and setup guide
- **Contains**: Tool documentation, testing strategy, troubleshooting
- **Read First**: Third (for detailed reference)
- **Time**: 15 minutes

### 2. Data Artifacts

#### target/sqllogictest_punchlist.csv (34 KB)
- **Type**: CSV format
- **Contains**: All 623 test files with metadata
- **Columns**: file, category, subcategory, status, priority
- **Sortable**: Yes (import into any spreadsheet)
- **Usage**: Track progress, filter by category/status

#### target/sqllogictest_punchlist.json (102 KB)
- **Type**: JSON format
- **Contains**: Same data as CSV plus summary statistics
- **Usage**: Programmatic access, scripting, tool integration
- **Schema**: 
  - `summary`: Pass/fail counts and pass rate
  - `by_category`: Per-category statistics
  - `files`: Array of file objects with status

### 3. Testing Tools

#### scripts/test_one_file.sh (2.4 KB)
- **Purpose**: Quick test of individual files
- **Usage**: `./scripts/test_one_file.sh <test_file>`
- **Input Formats**: 
  - Relative: `index/delete/10/slt_good_0.test`
  - Pattern: `index::delete::10::slt_good_0`
  - Full path: `/path/to/test.test`
- **Output**: Pass/fail status with next test suggestion
- **Status**: ✓ Ready to use

#### scripts/generate_punchlist.py (9.6 KB)
- **Purpose**: Generate/refresh punchlist from test results
- **Usage**: `python3 scripts/generate_punchlist.py`
- **Input**: Scans test directory and loads results
- **Output**: CSV and JSON punchlist files
- **When to use**: After running full test suite
- **Status**: ✓ Ready to use

#### scripts/test_file_direct.sh (optional)
- **Purpose**: Direct CLI testing without cargo framework
- **Requires**: sqllogictest CLI tool
- **Status**: Optional (fallback if sqllogictest installed)

### 4. Reference Guides

#### .loom/punchlist_guide.md
- **Purpose**: Builder-specific quick reference
- **Contains**: Job description, common patterns, support info
- **Usage**: Keep handy while implementing fixes
- **Status**: ✓ Ready

## Quick Statistics

```
Total Test Files:     623
✓ Passing:            83 (13.3%)
✗ Failing:           530 (85.1%)
? Untested:          10 (1.6%)

By Category:
  index              214 (75 pass, 35.0%)
  evidence            12 (6 pass, 50.0%)
  random             391 (2 pass, 0.5%)
  ddl                  1 (0 pass, 0.0%)
  other               5 (0 pass, 0.0%)
```

## How to Use This System

### Step 1: Understand the Situation (5 minutes)
```bash
cat QUICK_START.md
```

### Step 2: Review Your Strategy (10 minutes)
```bash
cat PUNCHLIST_100_CONFORMANCE.md
```

### Step 3: Pick a Test and Start
```bash
./scripts/test_one_file.sh index/delete/10/slt_good_0.test
```

### Step 4: Implement Fix
Based on error message from test output.

### Step 5: Verify Fix
```bash
./scripts/test_one_file.sh index/delete/10/slt_good_0.test
```

### Step 6: Check for Regressions
```bash
./scripts/test_one_file.sh index/between/10/slt_good_0.test  # a passing test
```

### Step 7: Move to Next Test
Follow suggestion from test_one_file.sh or pick from punchlist.

### Step 8: Track Progress (After Major Fix)
```bash
python3 scripts/generate_punchlist.py
cat target/sqllogictest_punchlist.json | grep -A2 '"summary"'
```

## Focus Areas

### Phase 1: Index Tests (Highest ROI)
**Goal**: 13.3% → 30-40% pass rate
**Target Categories**:
1. `index/delete/*` - 13 failing files, medium effort
2. `index/orderby/*` - 127 failing files, major opportunity
3. Other `index/*` - 20+ files, various issues

### Phase 2: Random Tests
**Goal**: 40% → 70-80% pass rate
**Issues**:
- Decimal formatting
- Multi-row formatting
- Aggregate functions

### Phase 3: Remaining
**Goal**: 80% → 100% pass rate
**Issues**:
- Edge cases
- Missing functions
- Type coercion

## Expected Timeline

| Phase | Start | Duration | Target | Tests |
|-------|-------|----------|--------|-------|
| 1 | Now | 2-5 days | 30-40% | +110-150 |
| 2 | Week 2 | 1-2 weeks | 70-80% | +200-250 |
| 3 | Week 3-4 | Few days | 100% | +80-120 |

## Common Issues You'll Encounter

### NOT Operator (index/delete)
**Error**: "Unary operator NOT not supported"
**Location**: Likely crates/executor/src/
**Solution**: Add support for NOT in WHERE clauses

### Ordering/Hash (index/orderby)
**Error**: "Result hashing differs" or incorrect row order
**Location**: crates/executor/src/ (query execution)
**Solution**: Fix ORDER BY implementation or result hashing

### Formatting (random/*)
**Error**: "7.000 vs 7" or wrong multi-row format
**Location**: crates/executor/src/result_formatter.rs
**Solution**: Adjust value formatting logic

### Aggregates (random/*)
**Error**: "Aggregate functions should be evaluated in aggregation context"
**Location**: crates/executor/src/aggregate.rs
**Solution**: Improve aggregate function scoping

## Success Criteria

✓ Phase is complete when:
- All tests in target category pass
- No regressions in other categories
- Full test suite shows expected improvement
- Documentation is updated

## Support Resources

**In this repo**:
- SQLLOGICTEST_ROADMAP.md - Detailed roadmap
- SQLLOGICTEST_ISSUES.md - Investigation results
- TESTING.md - Testing methodology

**For debugging**:
- Review test file: `cat third_party/sqllogictest/test/.../slt_good_X.test`
- Understand the SQL being tested
- Search code for related functionality
- Check SQLLOGICTEST_ISSUES.md for known patterns

## File Locations

```
Repository Root/
├── QUICK_START.md (read first!)
├── PUNCHLIST_100_CONFORMANCE.md (read second!)
├── PUNCHLIST_README.md (read third!)
├── PUNCHLIST_MANIFEST.md (this file)
├── SQLLOGICTEST_ROADMAP.md
├── SQLLOGICTEST_ISSUES.md
├── TESTING.md
├── scripts/
│   ├── test_one_file.sh (use this!)
│   ├── generate_punchlist.py
│   └── test_file_direct.sh
├── target/
│   ├── sqllogictest_punchlist.csv (all tests)
│   ├── sqllogictest_punchlist.json (programmatic)
│   └── sqllogictest_cumulative.json (test results)
└── .loom/
    └── punchlist_guide.md
```

## Next Action

```bash
# Start here
cat QUICK_START.md

# Then test your first file
./scripts/test_one_file.sh
```

---

**Built**: 2025-11-08
**Status**: Complete and ready for use
**Version**: 1.0
