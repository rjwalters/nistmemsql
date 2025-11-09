# SQLLogicTest Punchlist - Quick Start

**83/623 tests passing (13.3%) → Goal: 623/623 (100%)**

## 30-Second Overview

We have 623 SQLLogicTest files. 83 pass, 530 fail. We built a punchlist system to systematically test and fix them one at a time.

## Start Now

```bash
# 1. Read the strategy
cat PUNCHLIST_100_CONFORMANCE.md

# 2. Test the first failing file
./scripts/test_one_file.sh index/delete/10/slt_good_0.test

# 3. Read the error - it tells you what's missing
# 4. Find the code that needs fixing
# 5. Fix it and test again
```

## The Punchlist Files

| File | Purpose |
|------|---------|
| **PUNCHLIST_100_CONFORMANCE.md** | Strategic guide - read this first |
| **PUNCHLIST_README.md** | Complete documentation |
| **target/sqllogictest_punchlist.csv** | All 623 tests (sortable) |
| **scripts/test_one_file.sh** | Test individual files |
| **scripts/generate_punchlist.py** | Refresh after fixes |

## Categories & Status

```
index     214 files  75 pass  35.0%   (best opportunity)
evidence   12 files   6 pass  50.0%
random    391 files   2 pass   0.5%   (hardest)
ddl         1 file    0 pass   0.0%
other       5 files   0 pass   0.0%
```

## Phase 1: Fix Index Tests (13% → 30-40%)

Start with `index/delete/*` and `index/orderby/*` categories.

```bash
# Test a delete file
./scripts/test_one_file.sh index/delete/10/slt_good_0.test

# Test an orderby file
./scripts/test_one_file.sh index/orderby/10/slt_good_0.test
```

These show specific errors (NOT operator, ordering issues).

## Phase 2: Fix Random Tests (40% → 70-80%)

Most failing tests are here. Common issues:
- Decimal formatting (7.000 vs 7)
- Multi-row formatting
- Aggregate functions

## Common Workflow

1. **Test a file** → `./scripts/test_one_file.sh [test_file]`
2. **Read error** → Shows what SQL failed and why
3. **Find code** → `grep -r "FEATURE" crates/`
4. **Fix code** → Implement the missing feature
5. **Test again** → Verify fix works
6. **Check regressions** → Test previously passing file
7. **Move on** → Next file in the list

## Example: Fixing a Test

```bash
# Test a file
./scripts/test_one_file.sh index/delete/10/slt_good_0.test

# Output shows: "Unary operator NOT not supported"
# Search for NOT handling
grep -r "NOT" crates/executor/src/

# Find the code that needs to support NOT in WHERE clauses
# Implement the fix
# nano crates/executor/src/where_clause.rs

# Test again
./scripts/test_one_file.sh index/delete/10/slt_good_0.test

# Check we didn't break BETWEEN tests
./scripts/test_one_file.sh index/between/10/slt_good_0.test

# Great! Move to next file
```

## Helpful Commands

```bash
# See all failing tests
grep "FAIL" target/sqllogictest_punchlist.csv

# Count by subcategory
grep "FAIL" target/sqllogictest_punchlist.csv | cut -d, -f2,3 | sort | uniq -c

# Find tests with "delete" in name
grep "delete" target/sqllogictest_punchlist.csv

# Get first 10 failing index tests
grep "index.*FAIL" target/sqllogictest_punchlist.csv | head -10

# View a test file content
head -50 third_party/sqllogictest/test/index/delete/10/slt_good_0.test
```

## Progress Tracking

After fixing one category:

```bash
# Regenerate punchlist
python3 scripts/generate_punchlist.py

# See updated numbers
cat target/sqllogictest_punchlist.json | grep "pass_rate"

# Update the main doc
# (edit PUNCHLIST_100_CONFORMANCE.md with new numbers)
```

## Documents

- **PUNCHLIST_100_CONFORMANCE.md** - Full strategic guide
- **PUNCHLIST_README.md** - Complete documentation
- **SQLLOGICTEST_ROADMAP.md** - Detailed roadmap with issue numbers
- **SQLLOGICTEST_ISSUES.md** - Known issues from investigation
- **TESTING.md** - Testing strategy and methodology

## Success Criteria

Each phase is done when:
- ✓ All tests in that category pass
- ✓ No regressions in other categories
- ✓ Full test suite shows improvement

## Next Action

```bash
# Read the full guide
cat PUNCHLIST_100_CONFORMANCE.md

# Then test your first file
./scripts/test_one_file.sh index/delete/10/slt_good_0.test
```

---

**Goal**: 623/623 passing by systematically fixing root causes.
**Strategy**: Test one file at a time. Most failures cluster around same issues.
