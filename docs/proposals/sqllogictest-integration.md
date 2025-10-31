# SQLLogicTest Integration Proposal

## Current State

We have a working SQLLogicTest runner infrastructure in `tests/sqllogictest_runner.rs` with:
- ✅ Complete `AsyncDB` implementation for our database
- ✅ Proper type mapping (INTEGER, FLOAT, TEXT)
- ✅ Support for all statement types (CREATE, INSERT, UPDATE, DELETE, etc.)
- ✅ 2 hand-written baseline tests (arithmetic, basic SELECT)

**Problem**: We claim "7M+ tests" but only have 2 tests. We need to import real test suites.

## Proposed Solution

### Option 1: Import Official SQLite Test Suite (Recommended)

**Source**: https://sqlite.org/sqllogictest/
- Contains the original SQLLogicTest suite
- Millions of tests covering core SQL functionality
- Well-established, stable test cases

**Implementation**:
```bash
# Add as git submodule
git submodule add https://github.com/sqlite/sqllogictest third_party/sqllogictest-sqlite
```

**Test Runner**:
```rust
// tests/sqllogictest_sqlite.rs
#[tokio::test]
async fn run_sqlite_test_suite() {
    let mut tester = sqllogictest::Runner::new(|| async { Ok(NistMemSqlDB::new()) });

    // Run all .test files from SQLite suite
    let test_files = glob::glob("third_party/sqllogictest-sqlite/**/*.test")
        .expect("Failed to read test files");

    for entry in test_files {
        let path = entry.unwrap();
        let contents = std::fs::read_to_string(&path).unwrap();

        // Run test with proper error reporting
        match tester.run_script(&contents) {
            Ok(_) => println!("✓ {}", path.display()),
            Err(e) => eprintln!("✗ {} - {}", path.display(), e),
        }
    }
}
```

### Option 2: Use DuckDB's Extended Test Suite

**Source**: https://github.com/duckdb/duckdb (test/sql directory)
- More modern, actively maintained
- Includes advanced SQL features
- Better organized by feature area

**Challenges**:
- Uses DuckDB-specific extensions
- May need filtering for standard SQL-only tests

### Option 3: Use sqllogictest-bin CLI Tool

**Source**: https://crates.io/crates/sqllogictest-bin

**Implementation**:
```bash
# Install CLI tool
cargo install sqllogictest-bin

# Create test directory structure
mkdir -p tests/sqllogictest-files/
# Add .slt test files here

# Run tests via CLI
sqllogictest './tests/sqllogictest-files/**/*.slt'
```

**Advantages**:
- No code changes needed
- Can use `--override` flag to auto-update expected results
- Easy to run ad-hoc tests

## Recommended Approach

**Hybrid Strategy**:

1. **Phase 1: Add SQLite Core Tests** (Small, curated set)
   - Import ~100-500 core tests from SQLite
   - Focus on SQL:1999 Core features
   - Helps validate basic correctness
   - Achievable in 1-2 hours

2. **Phase 2: Organize by Feature Area**
   ```
   tests/sqllogictest-files/
   ├── select/
   │   ├── basic.slt
   │   ├── joins.slt
   │   └── subqueries.slt
   ├── dml/
   │   ├── insert.slt
   │   └── update.slt
   ├── ddl/
   │   ├── create_table.slt
   │   └── constraints.slt
   └── functions/
       ├── string.slt
       ├── math.slt
       └── datetime.slt
   ```

3. **Phase 3: Incremental Addition**
   - Add tests as we implement features
   - Use `--override` to generate expected results
   - Track pass rate over time

4. **Phase 4: Full Suite Integration** (Optional)
   - Add full SQLite suite as submodule
   - Run nightly to catch regressions
   - Report coverage metrics

## Implementation Tasks

### Immediate (1-2 hours)
- [ ] Add `glob` crate dependency
- [ ] Create `tests/sqllogictest-files/` directory structure
- [ ] Write 50-100 basic .slt test files covering Core SQL
- [ ] Update test runner to load files from directory
- [ ] Generate test results report

### Short-term (1 day)
- [ ] Add SQLite test suite as git submodule
- [ ] Filter to SQL:1999 Core compatible tests
- [ ] Create test discovery and execution harness
- [ ] Add pass/fail reporting with test IDs
- [ ] Document how to add new test files

### Medium-term (1 week)
- [ ] Categorize tests by SQL:1999 feature ID
- [ ] Create test matrix (feature × status)
- [ ] Add CI integration for SQLLogicTest suite
- [ ] Track pass rate over time
- [ ] Compare results with sqltest suite

## Benefits

1. **Broader Coverage**: Millions of test cases vs 739 sqltest cases
2. **Different Perspective**: Tests real-world SQL patterns, not just BNF grammar
3. **Regression Detection**: Catch bugs that slip through custom tests
4. **Industry Standard**: SQLLogicTest is used by SQLite, DuckDB, PostgreSQL variants
5. **Complementary**: Works alongside sqltest (grammar) and custom tests (features)

## Metrics to Track

```
SQLLogicTest Status:
- Total tests: 1,234,567 (example)
- Passing: 1,100,000 (89.1%)
- Failing: 100,000 (8.1%)
- Errors: 34,567 (2.8%)

By Category:
- SELECT: 95% (500k/525k)
- INSERT/UPDATE/DELETE: 92% (200k/217k)
- JOINs: 88% (150k/170k)
- Subqueries: 85% (100k/117k)
```

## Next Steps

1. **Decide on approach**: Which option(s) to implement?
2. **Set pass rate target**: What % is acceptable for first integration?
3. **Prioritize test categories**: Which SQL features to test first?
4. **Integration timeline**: When to merge into CI?

## Questions to Answer

1. Do we want full SQLite suite or curated subset?
2. Should we auto-skip tests for unimplemented features?
3. How to handle test failures - block CI or report only?
4. Should we track SQLLogicTest pass rate separately from sqltest?

## Resources

- **sqllogictest-rs**: https://github.com/risinglightdb/sqllogictest-rs
- **SQLite Tests**: https://sqlite.org/sqllogictest/
- **DuckDB Tests**: https://github.com/duckdb/duckdb/tree/main/test/sql
- **Our Test Runner**: `tests/sqllogictest_runner.rs`
