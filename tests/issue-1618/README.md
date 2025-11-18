# Issue #1618: Index Scan Returns 0 Rows After Database Pooling

## Summary

**FIXED!** Critical bug where indexes were not being populated during `INSERT...SELECT` operations due to bulk transfer optimization bypassing index maintenance.

## Root Cause - IDENTIFIED & FIXED

The bug was in **bulk_transfer.rs:254** where the bulk transfer optimization for `INSERT...SELECT` was calling `table.insert()` directly instead of `db.insert_row()`:

**Before (BROKEN):**
```rust
dest_table_mut.insert(row)?;  // Does NOT update indexes!
```

**After (FIXED):**
```rust
db.insert_row(dest_table, row)?;  // Updates indexes correctly!
```

This affected ALL `INSERT...SELECT` queries, which is the common pattern in SQLLogicTest files.

## Key Findings

### What Works ✅
- Storage layer (`Database::reset()`) - all unit tests pass
- Fresh database execution - vibesql CLI returns correct results
- Index creation on empty tables
- Index updates during insertion
- Thread-local pooling pattern (in unit tests)

### What Fails ❌
- SQLLogicTest suite after database pooling
- Only affects SELECT queries using indexes
- DELETE queries using indexes work correctly

### Critical Pattern

The bug manifests with this specific sequence:

```sql
-- 1. Create empty table
CREATE TABLE tab1(pk INTEGER PRIMARY KEY, col0 INTEGER);

-- 2. Create index on EMPTY table (BEFORE data!)
CREATE INDEX idx_col0 ON tab1 (col0);

-- 3. Insert data (AFTER index creation)
INSERT INTO tab1 SELECT * FROM tab0;

-- 4. Query using index
SELECT pk FROM tab1 WHERE col0 > 250;
-- Expected: N rows matching condition
-- Actual: 0 rows (ONLY after database reset!)
```

**Fresh database**: Returns correct results
**Pooled database (after reset)**: Returns 0 rows

## Investigation Summary

Since all storage-level tests pass but sqllogictest fails, the bug is NOT in `Database::reset()` itself. It must be in:

1. **Executor caching** - Query plan cache or schema cache not invalidated
2. **Test runner state** - `VibeSqlDB` maintaining stale state across reset
3. **Expression evaluator** - Cached state persisting across database resets

## Test Files

### Integration Tests
- **`tests/test_issue_1618.rs`** - **Executor-level reproduction test**
  - Uses InsertExecutor and SelectExecutor (like SQLLogicTest does)
  - Thread-local database pooling with reset()
  - Pattern: empty table → create index → INSERT...SELECT → query
  - **This test should reproduce the bug!**

### SQL Test Files
- `test_index.sql` - Index created AFTER data insertion (works)
- `test_index_before_data.sql` - Index created BEFORE data insertion (works in fresh DB)
- `test_index_insert_select.sql` - Uses INSERT...SELECT pattern (works in fresh DB)

### Unit Tests (Storage Layer)
Located in `crates/vibesql-storage/src/database/tests/indexes.rs`:
- `test_index_scan_after_database_reset()` - Two-cycle reset pattern
- `test_thread_local_pool_pattern()` - Exact pooling pattern from db_adapter.rs

**Both unit tests PASS**, confirming storage layer works correctly.

## How to Reproduce

### Running the Executor-Level Test (Should Reproduce Bug)
```bash
cargo test test_executor_with_pooling -- --nocapture
# This test uses the executor layer with pooling
# Expected: FAIL on cycle 2 (reproduces issue #1618)
```

### Using vibesql CLI (Fresh DB - Works)
```bash
./target/release/vibesql < tests/issue-1618/test_index_before_data.sql
# Returns: 3 rows (correct)
```

### Using SQLLogicTest (Pooled DB - Fails)
```bash
./scripts/sqllogictest test index/commute/100/slt_good_1.test
# Returns: 0 rows for tab1 queries (incorrect)
```

### Running Unit Tests (Storage Layer - All Pass)
```bash
cargo test --package vibesql-storage test_index_scan_after_database_reset
cargo test --package vibesql-storage test_thread_local_pool_pattern
# Both pass (storage layer works correctly)
```

## Next Steps

1. Add debug logging to executor to trace:
   - Schema/catalog state after reset
   - Index metadata lookups
   - Query plan generation
   - Index scan execution

2. Check for stale caches in:
   - `SelectExecutor`
   - `IndexScanExecutor`
   - `CombinedExpressionEvaluator`
   - `VibeSqlDB` wrapper

3. Compare executor behavior between:
   - First test file (works)
   - Second test file (fails after reset)

## Related Files

- `tests/sqllogictest/db_adapter.rs` - Thread-local pooling implementation
- `crates/vibesql-storage/src/database/core.rs:107-116` - `Database::reset()`
- `crates/vibesql-executor/src/select/scan/index_scan.rs` - Index scan executor
- `third_party/sqllogictest/test/index/**/*.test` - Failing test files

## Timeline

- **Initial Report**: Issue #1618 opened - index scans returning 0 rows
- **Investigation**: Identified thread-local pooling as trigger
- **Root Cause**: Confirmed bug is NOT in `Database::reset()`, but in executor/runner layer
- **Status**: Debugging executor state management

## Links

- Issue: https://github.com/rjwalters/vibesql/issues/1618
- PR #1617: Attempted fix for `reset()` (didn't solve the issue)
- Related commit: `b655bce` - Enhanced reproduction tests
