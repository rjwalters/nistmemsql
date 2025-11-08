# Issue 981 Resolution Verification

## Status: RESOLVED

The test `test_issue_919_in_subquery_hang` is now passing with correct hash values.

## Problem
The test was failing with incorrect hash values for an IN subquery query:
- Expected: `10 values hashing to e20b902b49a98b1a05ed62804c757f94`
- Actual (before fix): `10 values hashing to 8e06beba240f96e23de7d2ba7291282c`

## Solution
Fixed by commit 3d1144e which addressed the root cause:

The SQLLogicTest hash calculation was using the display format (with .000 suffix for integers) 
instead of the canonical format for hashing. This caused hash mismatches.

### Key Changes:
1. Added `format_sql_value_canonical()` function for proper hash calculation
2. Modified `format_result_rows()` to use canonical format when hashing large result sets
3. Separated display format from canonical format for hashing

## Verification
All tests pass successfully:

```bash
cargo test --test sqllogictest_runner --release test_issue_919_in_subquery_hang
```

Result:
```
running 1 test
test test_issue_919_in_subquery_hang ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured
```

## Query Details
The test validates complex WHERE clause with subqueries:
```sql
SELECT pk FROM tab0
WHERE col3 >= 94
   OR (col1 IN (63.39,21.7,52.63,42.27,35.11,72.69))
   OR col3 > 30 AND col0 IN (SELECT col3 FROM tab0 WHERE col1 < 71.54)
   OR (col3 > 35)
```

Returns 10 values with correct hash: `e20b902b49a98b1a05ed62804c757f94` ✓
