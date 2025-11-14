# Issue #1664 Investigation Notes

## Problem Summary

Index-based queries using BETWEEN and IN predicates are returning incorrect results. This affects 595/623 test files (95.5% of the SQLLogicTest suite).

## Initial Findings

### Test Case

```sql
CREATE TABLE tab0(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT);
-- Insert 10 rows with pk 0-9
CREATE TABLE tab1(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT);
INSERT INTO tab1 SELECT * FROM tab0;
CREATE INDEX idx_tab1_4 on tab1 (col4);

-- Query: col4 BETWEEN 31.24 AND 86.50
SELECT pk FROM tab0 WHERE (col4 BETWEEN 31.24 AND 86.50);  -- No index
SELECT pk FROM tab1 WHERE (col4 BETWEEN 31.24 AND 86.50);  -- With index
```

### Actual Results from VibeSQL

- Tab0 (no index): Returns pk 6, 7, 9
- Tab1 (with index): Returns pk 1, 3, 4, 6, 7, 9

### Expected Results from SQLLogicTest

**NOTE:** Need to verify this - the test file format may need closer examination.

## Code Locations

### Index Predicate Recognition

File: `crates/vibesql-executor/src/select/scan/index_scan.rs`

- Lines 64-101: `expression_filters_column()` - Recognizes BETWEEN predicates ✅
- Lines 126-247: `extract_range_predicate()` - Extracts BETWEEN bounds ✅
- Lines 229-242: BETWEEN handling looks correct ✅

### Index Scan Execution

File: `crates/vibesql-executor/src/select/scan/index_scan.rs`

- Lines 253-367: `execute_index_scan()`
- Line 286-291: Calls `index_data.range_scan()` with extracted bounds
- Lines 323-364: Re-applies full WHERE clause after index scan

### Next Steps

1. **Verify expected output** - Re-examine SQLLogicTest file format to confirm expected results
2. **Test storage layer** - Check `index_data.range_scan()` implementation in storage crate
3. **Add logging** - Add debug output to see what bounds are being passed to range_scan
4. **Compare implementations** - Look at in-memory vs disk-backed index implementations
5. **Check value types** - Verify FLOAT comparison is working correctly in index scan

## Hypothesis

The issue may be in one of these areas:

1. **Storage layer's range_scan()** - Boundary conditions or comparison logic
2. **Type conversions** - FLOAT values may not be comparing correctly
3. **Inclusive/exclusive bounds** - BETWEEN should be inclusive on both ends
4. **Index data structure** - BTree ordering or lookup logic

## Files to Examine

- `crates/vibesql-storage/src/index/*.rs` - Index data structures
- `crates/vibesql-btree/src/lib.rs` - B+ tree implementation
- Look for `range_scan` method implementations

## Status

- ✅ Minimal repro created
- ✅ Code flow understood
- 🔄 Root cause still being investigated
- ⏸️ Paused due to session complexity - needs fresh investigation

## For Next Session

Start by examining the storage layer's `range_scan()` implementation to understand how it handles BETWEEN boundaries for FLOAT values.
