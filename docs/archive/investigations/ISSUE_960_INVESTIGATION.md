# Issue 960: Fix General Result Mismatches

## Status: Investigation & Solution Design

This document outlines the investigation and proposed solutions for issue #960 - fixing the 112 test files with general result mismatches that don't fall into hash or formatting categories.

## Problem Summary

112 SQLLogicTest files (22.4% of failures) have result mismatches. These are categorized as "other" mismatches after accounting for:
- Hash mismatches (#959)
- Formatting issues (#956, #957)  
- Column resolution issues (#958)

## Root Cause Analysis

### Example 1: Aggregate Formatting Issue

**Query:**
```sql
SELECT ALL + ( MAX( col1 ) ) AS col1, - 20 FROM tab0 AS cor0
```

**Expected output:**
```
81
-20
```

**Actual output:**
```
81 -20
```

**Root Cause**: Aggregate function with unary operators in SELECT list should produce multi-row output, not concatenated result.

**Fix Location**: `crates/executor/src/select/executor/aggregation/` - result formatting for aggregates with unary operators

### Example 2: Arithmetic Expression Evaluation

**Query:**
```sql
SELECT ALL - 40 * - + 80 + + 32 + - + 20 + 47 * - + COUNT( * ) + ( 2 ) AS col0
```

**Expected**: `3167`
**Actual**: Truncated/incorrect result

**Root Cause**: Complex arithmetic with multiple unary operators and aggregates. Issues with:
1. Operator precedence (multiplication before addition)
2. Unary operator handling (-, +)
3. Type coercion in aggregate context

**Fix Locations**:
- `crates/executor/src/evaluator/operators/arithmetic.rs` - operator implementation
- `crates/executor/src/select/executor/aggregation/evaluation.rs` - aggregate evaluation with complex expressions

### Example 3: Complex WHERE with Subquery

**Query:**
```sql
SELECT pk FROM tab0 WHERE col3 < 942 AND (col0 < 779 AND col0 IS NULL AND (col1 IN (SELECT col4 FROM tab0 WHERE ...)))
```

**Root Cause**: Complex WHERE conditions with multiple AND/OR and subqueries producing wrong results.

**Fix Locations**:
- `crates/executor/src/evaluator/` - boolean expression evaluation
- `crates/executor/src/select/filters/` - WHERE clause filtering

## Investigation Strategy

Based on the roadmap, this issue should be addressed AFTER fixing #956-#959:

1. **Fix #956** (Decimal formatting) - Low effort, +56 tests
2. **Fix #957** (Multi-row formatting) - Low effort, +19 tests  
3. **Fix #959** (Hash mismatches) - Medium effort, +138 tests
4. **Fix #958** (Column resolution) - Medium effort, +39 tests
5. **THEN investigate #960** - Will help identify remaining patterns

## Current Code Analysis

### Aggregation Result Formatting

File: `crates/executor/src/select/grouping.rs`

**Current implementation**:
- `AggregateAccumulator::finalize()` (line 186) - returns single aggregated value
- `add_sql_values()` (line 208) - handles SUM/AVG accumulation
- `divide_sql_value()` (line 234) - handles AVG division

**Issue**: When aggregates appear with other expressions in SELECT, the formatting doesn't properly handle multi-row vs single-row output.

### Arithmetic Operator Implementation

File: `crates/executor/src/evaluator/operators/arithmetic.rs`

**Current operators**:
- `add()` - Addition
- `subtract()` - Subtraction  
- `multiply()` - Multiplication
- `divide()` - Division with zero check
- `integer_divide()` - Integer division
- `modulo()` - Modulo

**Potential issues**:
1. Operator precedence in expression trees - how multiplication is evaluated before addition
2. Unary operator handling - `- 40 * - + 80` needs careful precedence
3. Type coercion - mixing Integer, Numeric, etc. in complex expressions

## Next Steps

1. **After #956-#959 are fixed**:
   - Rerun full test suite
   - Generate updated failure analysis
   - Identify specific patterns in remaining #960 failures

2. **For each identified pattern**:
   - Create minimal test case
   - Trace through evaluator code
   - Identify root cause
   - Implement fix
   - Validate with test case

3. **Focus areas**:
   - Aggregate function formatting with operators
   - Unary operator precedence in arithmetic
   - Type coercion in aggregate context
   - Complex WHERE clause evaluation with subqueries

## Testing

After implementing fixes, test with:

```bash
# Run specific failing test files
cargo test --test sqllogictest_suite -- --exact random/aggregates/slt_good_129.test

# Run full test suite
SQLLOGICTEST_TIME_BUDGET=1200 cargo test --test sqllogictest_suite

# Analyze results
python3 scripts/analyze_test_failures.py
```

## Related Issues

- #956: Decimal formatting (7.000 vs 7)
- #957: Multi-row formatting  
- #958: Column resolution (COL0 vs col0)
- #959: Hash mismatches (complex queries)
- #960: General result mismatches (THIS ISSUE)
- #961: NULLIF/COALESCE functions
- #962: NOT NULL handling
- #963: Parse errors

## Progress

- [ ] Wait for #956-#959 to be fixed
- [ ] Rerun analysis with updated failures
- [ ] Identify specific patterns
- [ ] Create sub-issues for each pattern
- [ ] Implement fixes
- [ ] Validate improvements
- [ ] Reach target pass rate (78%)

---

**Created**: 2025-11-07
**Priority**: P0 (Medium effort, Very high impact)
**Expected Impact**: +112 passing tests (22.4% improvement)
