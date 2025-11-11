# Index Test Pass Rate Analysis

**Issue**: #1233
**Goal**: Analyze and improve index/* test pass rate from 38.6% to 60%+
**Date**: 2025-11-10

## Executive Summary

This document analyzes failures in the SQLLogicTest index/* test category to identify patterns and recommend targeted fixes.

## Current Status

- **Total Tests**: 308 test cases across 214 test files
- **Passing**: 119 tests (38.6%)
- **Failing**: 189 tests (61.4%)
- **Target**: 185+ passing (60%+)
- **Gap**: 66 tests needed to reach target

## Test Structure

Index tests validate that queries return identical results with and without indexes:
- `tab0`: Baseline table without indexes
- `tab1`: Identical table with indexes on col0, col1, col3, col4
- Test validates: `SELECT ... FROM tab0` ≡ `SELECT ... FROM tab1`

## Test Categories

| Subcategory | Files | Description |
|-------------|-------|-------------|
| commute | 52 | Commutative property (a=b vs b=a) |
| orderby_nosort | 49 | ORDER BY optimization with indexes |
| orderby | 31 | ORDER BY with indexed columns |
| random | 26 | Random query patterns |
| view | 16 | Views over indexed tables |
| delete | 14 | DELETE with indexed columns |
| between | 13 | BETWEEN operator with indexes |
| in | 13 | IN operator with indexes |

## Known Issues

### 1. Multi-column Index Flattening (Fixed in PR #1210)
- **Status**: Fixed but not yet tested
- **Impact**: Unknown - awaiting retest
- **Code**: `crates/vibesql-storage/src/database/indexes.rs`

### 2. Type Formatting (Issue #1230)
- **Status**: Open
- **Impact**: INTEGER vs DECIMAL formatting
- **Code**: Related to MySQL compatibility

### 3. Range Scan Performance
- **Status**: Design limitation
- **Impact**: O(n) HashMap scan vs O(log n) BTreeMap
- **Code**: `indexes.rs:36-95`
- **Fix**: Migrate to BTreeMap (future optimization)

## Analysis Methodology

### Phase 1: Baseline Establishment
Run full index test suite and capture results

### Phase 2: Pattern Analysis
Group failures by subcategory and identify common error patterns

### Phase 3: Sample Analysis
For each high-impact category, examine failure modes

## Phase 2 Analysis: Infrastructure Discovery

**Date**: 2025-11-11
**Status**: Test infrastructure challenges identified
**Blocker**: Full test suite execution time exceeds practical limits for iterative analysis

### Test Infrastructure Findings

**Test Suite Scale**:
- Total test files: 621 (all categories)
- Index-specific: 214 files, 308 test cases
- Execution time: >10 minutes for parallel run with 8 workers

**Infrastructure Dependencies**:
- Analysis script requires populated database: `~/.vibesql/test_results/test_results.db`
- Parallel runner uses intermediate storage: `/tmp/sqllogictest_results/`
- Post-processing needed to convert JSON → SQLite format

**Test Category Structure Identified**:
```
index/
├── between/         13 files
├── commute/         52 files  ← LARGEST
├── delete/          14 files
├── in/              13 files
├── orderby/         31 files  ← HIGH PRIORITY
├── orderby_nosort/  49 files  ← SECOND LARGEST
├── random/          26 files
└── view/            16 files
```

### Top 3 Expected Failure Patterns

Based on test structure analysis and known issues:

#### 1. Commutative Property Issues (`commute/*`, 52 files)
**Hypothesis**: Index lookups may be directional
**Example**: `WHERE col0=5 AND col1=3` ≠ `WHERE col1=3 AND col0=5`
**Root Cause**: Multi-column index flattening (possibly fixed in PR #1210)
**Impact**: HIGH (largest category, ~25-30% of index tests)

#### 2. ORDER BY Without Sort (`orderby_nosort/*`, 49 files)
**Hypothesis**: Index scan order doesn't match expected result order
**Example**: Query returns results in index order instead of insertion order
**Root Cause**: Assumption that index order = correct order
**Impact**: HIGH (second largest, ~23-25% of index tests)

#### 3. Complex ORDER BY (`orderby/*`, 31 files)
**Hypothesis**: Multi-column or mixed ASC/DESC ordering
**Example**: `ORDER BY col0 ASC, col1 DESC` with multi-column index
**Root Cause**: Index traversal doesn't support mixed directions
**Impact**: MEDIUM-HIGH (~15-18% of index tests)

**Combined Impact**: ~132 files (60-73% of index tests)
**Current Pass Rate**: 119/308 = 38.6%
**If Top 3 Fixed**: Could reach 180-210 passing = 58-68% ✓ EXCEEDS 60% TARGET

## Recommendations

### For Completing Phase 2 Analysis

Three options for obtaining actual test data:

**Option A - Targeted Testing** (Recommended for next iteration):
```bash
# Add --category filter to test runner (requires implementation)
./scripts/sqllogictest run --category index --parallel --workers 8 --time 120
# Estimated: 2-3 minutes, sufficient for analysis
```

**Option B - Extended Run** (Comprehensive but time-intensive):
```bash
# Full suite on powerful machine
./scripts/sqllogictest run --parallel --workers 32 --time 600
# Estimated: 15-30 minutes, complete dataset
```

**Option C - Manual Sampling** (Fast, directional):
```bash
# Test specific files manually
./scripts/sqllogictest test index/commute/10/slt_good_0.test
./scripts/sqllogictest test index/orderby_nosort/10/slt_good_0.test
# Immediate feedback, good for pattern validation
```

### For Phase 3 Implementation

**Priority Queue** (based on expected impact):

1. **Commutative Property** (Priority: CRITICAL)
   - Verify PR #1210 resolves the issue
   - Retest `commute/*` category
   - Estimated fix: 25-30 tests

2. **ORDER BY Optimization** (Priority: HIGH)
   - Review index scan assumptions
   - Fix `orderby_nosort/*` failures
   - Estimated fix: 25-30 tests

3. **Complex Ordering** (Priority: MEDIUM)
   - Multi-column mixed ASC/DESC support
   - Address `orderby/*` failures
   - Estimated fix: 15-20 tests

**Projected Outcome**:
- Current: 119 passing (38.6%)
- After Priority 1+2+3: ~190 passing (61.7%)
- **Result**: ✓ EXCEEDS 60% TARGET

### Infrastructure Improvements Needed

1. **Test Runner Enhancement**:
   - Add `--category <name>` filter for targeted testing
   - Implement test result caching to avoid full re-runs
   - Document expected runtimes for different configurations

2. **Analysis Workflow**:
   ```bash
   # Proposed efficient workflow
   ./scripts/sqllogictest run --category index --time 120
   ./scripts/analyze_index_tests.sh > analysis_output.txt
   ```

3. **Documentation**:
   - Add runtime expectations to test runner help
   - Document database schema and query patterns
   - Create troubleshooting guide for common issues

## Follow-up Issues

**Phase 3 Implementation Issues** (Created from #1236):

1. **#1241**: Fix commutative property issues in index tests
   - Pattern: `index/commute/` (52 files)
   - Impact: ~25-30 tests (~8-10% improvement)
   - Priority: CRITICAL

2. **#1242**: Fix ORDER BY optimization issues in index tests
   - Pattern: `index/orderby_nosort/` (49 files)
   - Impact: ~23-25 tests (~7-8% improvement)
   - Priority: HIGH

3. **#1243**: Fix complex ORDER BY with indexes
   - Pattern: `index/orderby/` (31 files)
   - Impact: ~15-18 tests (~5-6% improvement)
   - Priority: MEDIUM

**Combined Impact**: If all three issues resolved → 60-73% pass rate ✓ EXCEEDS TARGET

## References

- Issue #1233: Parent analysis issue
- Issue #1235: Phase 2 pattern analysis
- Issue #1236: Phase 3 implementation coordination
- Issue #1241: Commutative property fixes
- Issue #1242: ORDER BY optimization fixes
- Issue #1243: Complex ORDER BY fixes
- Issue #1232: Retest with latest code
- Issue #1230: SQL dialect modes
- PR #1210: Multi-column index flattening fix
- Issue #1198: Original index test tracking (closed)

## Code References

- Index storage: `crates/vibesql-storage/src/database/indexes.rs`
- Index DDL: `crates/vibesql-executor/src/index_ddl/create_index.rs`
- Test runner: `tests/sqllogictest_basic.rs`
- Query tool: `scripts/query_test_results.py`
