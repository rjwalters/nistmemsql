# SQL:1999 Test Failure Analysis

**Generated**: 2025-10-30
**Total Failures**: 106/739 tests (14.3%)
**Current Pass Rate**: 85.7%
**Target**: 90%+ (need to fix ~40 tests)

---

## Executive Summary

Analysis of the 106 failing tests reveals **5 high-impact failure patterns** that could be fixed to achieve 90%+ conformance:

1. **SELECT * with AS clause** (12 tests) - Column aliasing for wildcards
2. **GRANT/REVOKE advanced syntax** (28 tests) - Procedure/Function/Domain/Sequence privileges
3. **SET statement extensions** (6 tests) - CATALOG, NAMES, TIME ZONE
4. **CREATE statement extensions** (6 tests) - WITHOUT OIDS clause
5. **Cursor operations** (10 tests) - OPEN, FETCH, CLOSE

**Quick Win Opportunity**: Fixing pattern #1 alone would bring us to **87.3%** conformance.

---

## Failure Patterns (Grouped by Impact)

### Pattern 1: SELECT * with Column Aliasing (12 tests) 🎯 HIGH IMPACT

**Error Message**:
```
Statement 2 failed: Execution error: UnsupportedFeature("SELECT * and qualified wildcards require FROM clause")
```

**Example SQL**:
```sql
CREATE TABLE TABLE_E051_07_01_01 ( A INT, B INT );
SELECT * AS ( C , D ) FROM TABLE_E051_07_01_01
```

**Issue**: The executor rejects `SELECT * AS (C, D)` syntax - this is SQL:1999 feature E051-07/08 (derived column list).

**Impact**: 12 tests
**Difficulty**: Medium
**Feature Code**: E051-07, E051-08

**Fix Required**:
- Parser already supports this (no parse errors)
- Executor needs to expand `*` to columns, then apply the AS aliases
- Implementation: In projection handling, detect `* AS (col1, col2, ...)` pattern

**Tests Affected**:
- e051_07_01_01, e051_07_01_03, e051_07_01_05, e051_07_01_07, e051_07_01_09, e051_07_01_11
- e051_08_01_01, e051_08_01_03, e051_08_01_05, e051_08_01_07, e051_08_01_09, e051_08_01_11

---

### Pattern 2: GRANT/REVOKE Advanced Privileges (28 tests) ⚠️ MEDIUM-HIGH IMPACT

**Subcategory 2.1: Procedure/Function/Method Privileges (14 tests)**

**Error Messages**:
```
Statement 2 failed: Parse error: ParseError { message: "Expected keyword To/From, found Keyword(Function)" }
Statement 2 failed: Parse error: ParseError { message: "Expected keyword To/From, found Keyword(Procedure)" }
Statement 2 failed: Parse error: ParseError { message: "Expected keyword To/From, found Keyword(Method)" }
Statement 2 failed: Parse error: ParseError { message: "Expected keyword To/From, found Keyword(Constructor)" }
Statement 2 failed: Parse error: ParseError { message: "Expected keyword To/From, found Keyword(Routine)" }
Statement 2 failed: Parse error: ParseError { message: "Expected keyword To/From, found Keyword(Static)" }
Statement 2 failed: Parse error: ParseError { message: "Expected keyword To/From, found Keyword(Instance)" }
```

**Example SQL**:
```sql
GRANT EXECUTE ON FUNCTION my_func TO user1;
GRANT EXECUTE ON PROCEDURE my_proc TO user1;
GRANT EXECUTE ON METHOD my_method FOR my_type TO user1;
```

**Issue**: Parser doesn't recognize FUNCTION/PROCEDURE/METHOD/etc. as privilege targets.

**Impact**: 14 tests
**Difficulty**: Medium
**Feature Code**: SQL/PSM and OOP SQL extensions

**Fix Required**:
- Extend GRANT/REVOKE parser to accept: `ON {FUNCTION|PROCEDURE|METHOD|ROUTINE|CONSTRUCTOR|STATIC|INSTANCE} <name>`
- Add storage for these privilege types
- Update privilege enforcement

**Subcategory 2.2: Domain/Sequence/Translation Privileges (12 tests)**

**Error Messages**:
```
Statement 3 failed: Parse error: ParseError { message: "Expected keyword To/From, found Identifier(\"DOMAIN1\")" }
Statement 3 failed: Parse error: ParseError { message: "Expected keyword To/From, found Identifier(\"SEQUENCE1\")" }
Statement 3 failed: Parse error: ParseError { message: "Expected keyword To/From, found Identifier(\"TRANSLATION1\")" }
```

**Example SQL**:
```sql
GRANT USAGE ON DOMAIN domain1 TO user1;
GRANT USAGE ON SEQUENCE seq1 TO user1;
GRANT USAGE ON TRANSLATION trans1 TO user1;
```

**Issue**: Parser expects table/schema names, not domain/sequence/translation objects.

**Impact**: 12 tests
**Difficulty**: High (requires implementing CREATE DOMAIN/SEQUENCE/TRANSLATION first)
**Feature Code**: Advanced DDL features

**Subcategory 2.3: SET Privilege Context (2 tests)**

**Error Messages**:
```
Statement 3 failed: Parse error: ParseError { message: "Expected keyword To/From, found Keyword(Set)" }
```

**Issue**: GRANT with SET clause (probably for user/role context).

**Impact**: 2 tests
**Difficulty**: Medium

---

### Pattern 3: SET Statement Extensions (6 tests) 🎯 MEDIUM IMPACT

**Error Message**:
```
Statement 2 failed: Parse error: ParseError { message: "Expected SCHEMA, CATALOG, NAMES, or TIME ZONE after SET" }
```

**Example SQL**:
```sql
SET CATALOG my_catalog;
SET NAMES 'UTF8';
SET TIME ZONE LOCAL;
```

**Issue**: Parser only accepts `SET SCHEMA`, not other SET variants.

**Impact**: 6 tests
**Difficulty**: Easy
**Feature Code**: F031 (Basic schema manipulation)

**Fix Required**:
- Extend SET parser to accept: CATALOG, NAMES, TIME ZONE
- Add AST nodes for these SET variants
- Implement execution (may be no-ops for some)

---

### Pattern 4: CREATE Statement Extensions (6 tests) 🎯 QUICK WIN

**Error Message**:
```
Parse error: ParseError { message: "Expected RParen, found Keyword(Without)" }
```

**Example SQL**:
```sql
CREATE TABLE foo (id INT) WITHOUT OIDS;
```

**Issue**: Parser doesn't recognize `WITHOUT OIDS` clause (PostgreSQL-specific but in SQL:1999 optional features).

**Impact**: 6 tests
**Difficulty**: Easy
**Feature Code**: Optional table properties

**Fix Required**:
- Extend CREATE TABLE parser to accept optional `WITHOUT OIDS` / `WITH OIDS`
- Can be no-op in execution (we don't have OIDs anyway)

---

### Pattern 5: Cursor Operations (10 tests) ⚠️ MEDIUM IMPACT

**Error Message**:
```
Statement 4 failed: Parse error: ParseError { message: "Expected statement, found Identifier(\"OPEN\")" }
```

**Example SQL**:
```sql
DECLARE cur1 CURSOR FOR SELECT * FROM t1;
OPEN cur1;
FETCH FROM cur1;
CLOSE cur1;
```

**Issue**: Parser doesn't recognize DECLARE CURSOR, OPEN, FETCH, CLOSE statements.

**Impact**: 10 tests
**Difficulty**: High
**Feature Code**: Cursor support (SQL/PSM)

**Fix Required**:
- Add DECLARE CURSOR statement
- Add OPEN/FETCH/CLOSE statements
- Implement cursor state management
- This is a larger feature (procedural SQL)

---

### Pattern 6: UPDATE/DELETE Trigger Extensions (8 tests) ⚠️ MEDIUM IMPACT

**Error Message**:
```
Statement 2 failed: Parse error: ParseError { message: "Expected UPDATE after ON" }
```

**Example SQL**:
```sql
CREATE TRIGGER trig1 BEFORE DELETE ON table1 FOR EACH ROW ...
```

**Issue**: Parser expects `ON UPDATE` for triggers, not other trigger events.

**Impact**: 8 tests
**Difficulty**: High
**Feature Code**: Trigger support

**Fix Required**:
- Complete trigger syntax parsing (already partially implemented)
- Add DELETE, INSERT trigger events
- Implement trigger execution

---

### Pattern 7: VIEW Execution Errors (9 tests) ⚠️ MEDIUM IMPACT

**Error Message**:
```
Statement 3 failed: Execution error: TableNotFound("VIEW_F131_01_01_01")
Statement 4 failed: Execution error: TableNotFound("VIEW_F131_02_01_01")
```

**Example Test Pattern**:
```sql
CREATE VIEW my_view AS SELECT * FROM t1;  -- Statement 1
-- Statement 2: some other operation
-- Statement 3: tries to query the view, but it's not found
```

**Issue**: Views are created but not stored/queryable properly.

**Impact**: 9 tests
**Difficulty**: Medium
**Feature Code**: F131 (Grouped views)

**Fix Required**:
- Debug view storage/retrieval
- Ensure views are registered in catalog
- Fix view query expansion

---

### Pattern 8: ALTER TABLE ADD Constraint (1 test)

**Error Message**:
```
Statement 2 failed: Parse error: ParseError { message: "Expected COLUMN, CONSTRAINT, or constraint type after ADD" }
```

**Example SQL**:
```sql
ALTER TABLE t1 ADD CHECK (col1 > 0);
```

**Issue**: Parser expects `ADD COLUMN` or `ADD CONSTRAINT`, not bare constraint.

**Impact**: 1 test
**Difficulty**: Easy
**Feature Code**: DDL enhancement

---

### Pattern 9: Miscellaneous Parse Errors (12 tests)

Various one-off parsing issues:
- `Expected expression, found Keyword(All)` (5 tests)
- `Expected table name after DELETE FROM` (1 test)
- `Expected identifier, found String("de_DE")` (2 tests)
- `Expected identifier after CURRENT, found Keyword(Of)` (2 tests)
- Others (2 tests)

**Impact**: 12 tests
**Difficulty**: Varies
**Approach**: Each needs individual investigation

---

## Prioritized Action Plan

### Phase 1: Quick Wins (Target: 87-88% conformance)

**Goal**: Fix 15-20 tests with minimal effort

1. ✅ **SELECT * AS (col1, col2)** - 12 tests
   - Estimated effort: 4-6 hours
   - Impact: +1.6% conformance
   - Difficulty: Medium (executor change)

2. ✅ **CREATE TABLE WITHOUT OIDS** - 6 tests
   - Estimated effort: 1-2 hours
   - Impact: +0.8% conformance
   - Difficulty: Easy (parser no-op)

3. ✅ **SET CATALOG/NAMES/TIME ZONE** - 6 tests
   - Estimated effort: 2-3 hours
   - Impact: +0.8% conformance
   - Difficulty: Easy (parser + no-op execution)

**Phase 1 Total**: 24 tests fixed → **88.9% conformance** (+3.2%)

---

### Phase 2: Medium Impact Fixes (Target: 90%+ conformance)

**Goal**: Fix patterns with good ROI

4. ✅ **VIEW query execution fixes** - 9 tests
   - Estimated effort: 4-6 hours
   - Impact: +1.2% conformance
   - Difficulty: Medium (debugging)

5. ✅ **ALTER TABLE ADD bare constraint** - 1 test
   - Estimated effort: 1 hour
   - Impact: +0.1% conformance
   - Difficulty: Easy

6. ✅ **GRANT ON FUNCTION/PROCEDURE** - 14 tests
   - Estimated effort: 6-8 hours
   - Impact: +1.9% conformance
   - Difficulty: Medium (requires stub function/procedure objects)

**Phase 2 Total**: 24 tests fixed → **92.2% conformance** (+3.2%)

**Cumulative**: 48 tests fixed → **92.2% total conformance**

---

### Phase 3: Advanced Features (Optional for 95%+)

**Long-term features** (not required for 90%):

7. ⬜ **Cursor operations** - 10 tests
   - Requires procedural SQL implementation
   - Large feature (cursors, state management)

8. ⬜ **GRANT ON DOMAIN/SEQUENCE** - 12 tests
   - Requires CREATE DOMAIN/SEQUENCE first
   - Complex DDL features

9. ⬜ **Trigger syntax completion** - 8 tests
   - Requires full trigger implementation
   - Already partially parsed

10. ⬜ **Miscellaneous parse fixes** - 12 tests
    - Various one-off issues
    - Requires individual analysis

**Phase 3 Total**: 42 tests → **97.9% conformance**

---

## Recommended Approach

### Immediate Focus: Phase 1 (Days 1-2)

**Target**: 88.9% conformance in 2 days

**Day 1**:
1. Implement `SELECT * AS (col1, col2, ...)` support
   - Modify executor projection handling
   - Add tests for wildcard aliasing
   - Estimated: 12 tests fixed

**Day 2**:
2. Add `WITHOUT OIDS` clause to CREATE TABLE (parser no-op)
   - Estimated: 6 tests fixed
3. Add `SET CATALOG/NAMES/TIME ZONE` statements
   - Estimated: 6 tests fixed

**Result**: 24 tests fixed → **88.9% conformance**

---

### Short-term Focus: Phase 2 (Days 3-5)

**Target**: 92%+ conformance in 3 days

**Day 3**:
4. Debug and fix VIEW query execution
   - Investigate why views aren't found
   - Fix catalog registration
   - Estimated: 9 tests fixed

**Day 4**:
5. Add ALTER TABLE ADD bare constraint syntax
   - Simple parser update
   - Estimated: 1 test fixed
6. Start GRANT ON FUNCTION/PROCEDURE support
   - Add privilege target types
   - Create stub function/procedure objects

**Day 5**:
7. Complete GRANT ON FUNCTION/PROCEDURE
   - Wire up privilege checking
   - Estimated: 14 tests fixed

**Result**: 24 more tests fixed → **92.2% conformance**

---

### Long-term Focus: Phase 3 (Weeks 2-4)

**Target**: 95%+ conformance

Only pursue if aiming for near-complete SQL:1999 Core:
- Cursor support (major feature)
- CREATE DOMAIN/SEQUENCE (advanced DDL)
- Trigger completion (already started)
- Miscellaneous fixes

---

## Impact Summary

| Phase | Tests Fixed | Conformance Gain | Effort | Priority |
|-------|-------------|------------------|--------|----------|
| **Phase 1** | 24 | +3.2% → 88.9% | 7-11 hours | ⭐⭐⭐ High |
| **Phase 2** | 24 | +3.2% → 92.2% | 11-15 hours | ⭐⭐ Medium |
| **Phase 3** | 42 | +5.7% → 97.9% | 40-60 hours | ⭐ Low |

**Recommended immediate action**: Execute Phase 1 in next 2 days to reach ~89% conformance with minimal effort.

**90% milestone**: Phases 1+2 will achieve 92.2%, exceeding the 90% target.

---

## Feature Code Reference

- **E051-07/08**: Derived column lists (SELECT * AS (cols))
- **F031**: Basic schema manipulation (SET CATALOG, etc.)
- **F131**: Grouped views
- **SQL/PSM**: Procedural SQL (cursors, procedures, functions)
- **Advanced DDL**: DOMAIN, SEQUENCE, TRANSLATION

---

**Next Steps**:
1. Review and approve this analysis
2. Create GitHub issues for Phase 1 tasks
3. Begin implementation starting with SELECT * AS aliasing

---

*Generated by Claude Code analysis of target/sqltest_results.json*
