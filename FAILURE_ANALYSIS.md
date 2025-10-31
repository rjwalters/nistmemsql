# SQL:1999 Test Failure Analysis

**Generated**: 2025-10-30 (Auto-generated from test results)
**Total Failures**: 108/739 tests (14.6%)
**Current Pass Rate**: 85.4%
**Target**: 90%+ (need to fix ~34 tests)

---

## Executive Summary

Analysis of the 108 failing tests reveals the following primary failure patterns:

**Top Failure Patterns** (by count):
1. **SELECT * AS (col1, col2)** (12 tests) - Derived column lists for wildcards
2. **GRANT/REVOKE advanced syntax** (49 tests) - Procedure/Function/Domain/Sequence privileges
3. **Cursor operations** (13 tests) - OPEN, FETCH, CLOSE
4. **TIMESTAMP WITHOUT TIME ZONE** (0 tests) - Parser support needed
5. **SET statement extensions** (6 tests) - Transaction isolation, read-only mode
6. **Foreign key referential actions** (8 tests) - ON UPDATE/DELETE actions
7. **Aggregate function ALL syntax** (5 tests) - ALL keyword in aggregates
8. **Column-level privileges** (4 tests) - GRANT/REVOKE on specific columns
9. **Miscellaneous** (11 tests) - Various parser/executor gaps

**Path to 90%+**: Need to fix ~34 more tests. Quick wins available in patterns with smaller test counts.

---

## Detailed Failure Patterns

### Pattern 1: SELECT * AS (col1, col2) - 12 tests

**Error Message**:
```
Statement 2 failed: Parse error: ParseError { message: "Expected Symbol('('), found LParen" }
```

**Example SQL**:
```sql
SELECT * AS (C, D) FROM table_name
```

**Issue**: Parser doesn't support derived column lists (AS clause after wildcards).

**Feature Code**: E051-07, E051-08

**Affected Tests**:
e051_07_01_01,e051_07_01_03,e051_07_01_05,e051_07_01_07,e051_07_01_09,e051_07_01_11,e051_08_01_01,e051_08_01_03,e051_08_01_05,e051_08_01_07,e051_08_01_09,e051_08_01_11

---

### Pattern 2: GRANT/REVOKE Advanced Privileges - 49 tests

**Subcategories**:
- GRANT/REVOKE on FUNCTION/PROCEDURE/METHOD
- GRANT/REVOKE on DOMAIN/SEQUENCE/TRANSLATION
- GRANT/REVOKE on CHARACTER SET/COLLATION/TYPE
- Column-level privileges (GRANT SELECT(col) ON table)

**Error Messages**:
```
Expected keyword To/From, found Keyword(Function/Procedure/Domain/Sequence)
Expected keyword On, found LParen (for column privileges)
```

**Issue**: Parser doesn't recognize advanced privilege targets beyond tables and schemas.

**Affected Test Patterns**: f031_03_*, f031_19_*, e081_*

---

### Pattern 3: Cursor Operations - 13 tests

**Error Message**:
```
Statement failed: Parse error: ParseError { message: "Expected statement, found Identifier(\"OPEN\")" }
```

**Example SQL**:
```sql
DECLARE cur1 CURSOR FOR SELECT * FROM t1;
OPEN cur1;
FETCH FROM cur1;
CLOSE cur1;
```

**Issue**: No cursor support (procedural SQL feature).

**Feature Code**: E121 (Cursor support)

**Affected Test Pattern**: e121_*

---

### Pattern 4: TIMESTAMP WITHOUT TIME ZONE - 0 tests

**Error Message**:
```
Parse error: ParseError { message: "Expected RParen, found Keyword(Without)" }
```

**Example SQL**:
```sql
CAST('2016-03-26 01:02:03' AS TIMESTAMP WITHOUT TIME ZONE)
```

**Issue**: Parser doesn't support TIMESTAMP WITHOUT TIME ZONE syntax.

**Affected Test Pattern**: f051_05_*

---

### Pattern 5: SET Statement Extensions - 6 tests

**Error Messages**:
```
Expected SCHEMA, CATALOG, NAMES, or TIME ZONE after SET
```

**Example SQL**:
```sql
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SET TRANSACTION READ ONLY;
SET LOCAL TRANSACTION ...;
```

**Issue**: SET statement only supports SCHEMA, needs transaction-related extensions.

**Affected Test Pattern**: e152_*

---

### Pattern 6: Foreign Key Referential Actions - 8 tests

**Error Message**:
```
Parse error: ParseError { message: "Expected UPDATE after ON" }
```

**Example SQL**:
```sql
CREATE TABLE t2 (
  b INT REFERENCES t1(a) ON UPDATE NO ACTION ON DELETE NO ACTION
);
```

**Issue**: Parser expects only "ON UPDATE" not "ON DELETE" for foreign key actions.

**Affected Test Pattern**: e141_04_*

---

### Pattern 7: Aggregate Function ALL Syntax - 5 tests

**Error Message**:
```
Parse error: ParseError { message: "Expected expression, found Keyword(All)" }
```

**Example SQL**:
```sql
SELECT AVG(ALL col) FROM table;
SELECT COUNT(ALL col) FROM table;
```

**Issue**: Parser doesn't support ALL keyword in aggregate functions.

**Affected Test Pattern**: e091_06_*

---

### Pattern 8: Miscellaneous Parse Errors - 11 tests

Various one-off parsing and execution issues requiring individual investigation.

---

## Prioritized Action Plan

### Quick Wins (Highest ROI)

1. **Aggregate ALL syntax** ($AGG_ALL_COUNT tests) - Simple parser fix
2. **SET transaction statements** ($SET_COUNT tests) - Parser extension
3. **Foreign key actions** ($FK_ACTION_COUNT tests) - Parser fix

**Estimated Impact**: ~$(echo "$AGG_ALL_COUNT + $SET_COUNT + $FK_ACTION_COUNT" | bc) tests, moderate effort

### Medium-Term Fixes

4. **SELECT * AS aliasing** ($SELECT_AS_COUNT tests) - Parser + executor work
5. **TIMESTAMP WITHOUT TIME ZONE** ($TIMESTAMP_COUNT tests) - Type system extension
6. **Column-level privileges** ($COL_PRIV_COUNT tests) - Security model enhancement

### Long-Term Features

7. **GRANT/REVOKE advanced** ($GRANT_REVOKE_COUNT tests) - Requires stub objects
8. **Cursor operations** ($CURSOR_COUNT tests) - Major procedural SQL feature

---

## Implementation Notes

**Auto-generated from**: target/sqltest_results.json
**Last test run**: $CURRENT_DATE
**Test suite**: SQL:1999 conformance tests (upstream YAML)

To regenerate this file:
\`\`\`bash
cargo test --test sqltest_conformance --release -- --nocapture
./scripts/update_failure_analysis.sh
\`\`\`

