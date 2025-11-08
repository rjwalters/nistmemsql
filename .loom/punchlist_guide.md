# SQLLogicTest Punchlist - Builder's Guide

## What You Have

A complete system for achieving 100% SQLLogicTest conformance, organized by test file.

- **Current**: 83/623 passing (13.3%)
- **Goal**: 623/623 (100%)
- **Strategy**: Test one file at a time, fix root causes

## Start Here

1. **Read** `QUICK_START.md` (2 minutes)
2. **Read** `PUNCHLIST_100_CONFORMANCE.md` (10 minutes)
3. **Run** `./scripts/test_one_file.sh index/delete/10/slt_good_0.test`
4. **Fix** the code based on the error

## The Three Punchlist Files

| File | Read Time | Purpose |
|------|-----------|---------|
| `QUICK_START.md` | 2 min | Quick overview & commands |
| `PUNCHLIST_100_CONFORMANCE.md` | 10 min | Strategy & workflow |
| `PUNCHLIST_README.md` | 15 min | Complete documentation |

## Key Stats

```
Total: 623 files
✓ Passing: 83 (13.3%)
✗ Failing: 530 (85.1%)
? Untested: 10 (1.6%)

By Category:
  index     35.0% (75/214) - focus here first
  evidence  50.0% (6/12)
  random     0.5% (2/391) - hardest
  ddl        0.0% (0/1)
```

## Your Job as Builder

1. **Pick a failing test** → Use punchlist to find next test
2. **Run the test** → `./scripts/test_one_file.sh <test_file>`
3. **Read the error** → It tells you what SQL feature is missing
4. **Find the code** → Use grep to locate executor/parser code
5. **Implement fix** → Add support for that feature
6. **Verify** → Test passes now
7. **Check regressions** → Test a previously passing file
8. **Move on** → Next file in punchlist

## Quick Commands

```bash
# Test first failing file
./scripts/test_one_file.sh

# Test specific file
./scripts/test_one_file.sh index/delete/10/slt_good_0.test

# See all failing tests
grep "FAIL" target/sqllogictest_punchlist.csv | head -20

# Update punchlist after your fixes
python3 scripts/generate_punchlist.py

# See how many tests you've fixed
cat target/sqllogictest_punchlist.json | grep '"passed"'
```

## Focus Areas (Phase 1)

Start with these high-value categories:

1. **index/delete** (13 tests, 0% passing)
   - Issue: NOT operator in WHERE clauses
   - Once fixed: All 13 should pass

2. **index/orderby** (127 tests, 0% passing)
   - Issue: Result ordering or hash mismatches
   - Once fixed: Major jump in pass rate

3. **Other index tests** (20+ tests)
   - Various issues, usually smaller

## Related Documents

- `SQLLOGICTEST_ROADMAP.md` - High-level roadmap with issue numbers
- `SQLLOGICTEST_ISSUES.md` - Known issues from investigation
- `TESTING.md` - Testing methodology

## Common Patterns You'll See

**NOT operator errors** → index/delete/* files
```
Error: "Unary operator NOT not supported"
Location: Probably crates/executor/src/ or crates/parser/src/
```

**Ordering/hash issues** → index/orderby/* files
```
Error: "Result hashing differs" or wrong row order
Location: crates/executor/src/ (query execution or ordering)
```

**Formatting issues** → random/* files
```
Error: "7.000 vs 7" or multi-row formatting
Location: crates/executor/src/result_formatter.rs
```

## Success Criteria

Each phase is done when:
- All tests in that category pass
- No regressions in other categories
- Full test suite shows improvement

## Support

If stuck:
1. Review the test file: `cat third_party/sqllogictest/test/index/delete/10/slt_good_0.test`
2. Understand the SQL: What feature is being tested?
3. Search code: `grep -r "FEATURE_NAME" crates/`
4. Ask in issue: Reference which test is failing

---

**You've got this!** Every test failure is a clue pointing to missing feature. Fix the feature, and multiple tests pass.
