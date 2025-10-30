# Column Resolution Investigation: Issue #383

**Status**: Investigation Complete
**Date**: 2025-10-29
**Issue**: https://github.com/rjwalters/nistmemsql/issues/383

## Summary

This document details the investigation into column resolution failures affecting 4 web demo examples (join-4, uni-1, uni-5, window-8). The investigation revealed **architectural limitations** in how table aliases and qualifiers are tracked through the query execution pipeline.

## Failing Examples

### 1. join-4: CROSS JOIN with Self-Join
```sql
SELECT c1.category_name, c2.category_name
FROM categories c1
CROSS JOIN categories c2
WHERE c1.category_id < c2.category_id
```

**Error**: `ColumnNotFound("category_name")`
**Root Cause**: SELECT list evaluation occurs before FROM clause completes

### 2. uni-1: Correlated Subquery
```sql
SELECT s.name, s.major,
       (SELECT AVG(...) FROM enrollments e
        WHERE e.student_id = s.student_id) AS calc_gpa
FROM students s
```

**Error**: `ColumnNotFound("student_id")`
**Root Cause**: Outer context schema loses alias information

### 3. uni-5: Correlated Subquery with Comparison
```sql
SELECT s.name, s.major, s.gpa
FROM students s
WHERE s.gpa > (SELECT AVG(s2.gpa) FROM students s2
               WHERE s2.major = s.major)
```

**Error**: `ColumnNotFound("major")`
**Root Cause**: Same as uni-1 - alias lost in outer context

### 4. window-8: Window Function with Alias
```sql
SELECT product_name, unit_price,
       SUM(unit_price) OVER () AS total_price,
       ROUND(unit_price * 100.0 / SUM(unit_price) OVER (), 2) AS pct_of_total
FROM products
ORDER BY pct_of_total DESC
```

**Error**: `ColumnNotFound("pct_of_total")`
**Root Cause**: ORDER BY references computed column alias

## Architectural Analysis

### Current Query Execution Flow

```
Parser → SELECT list → FROM clause → WHERE → GROUP BY → ORDER BY
         ^
         |
    Problem: Evaluates before aliases are established
```

### Schema Architecture Issues

#### 1. TableSchema vs CombinedSchema

**TableSchema** (used for single tables and outer contexts):
- Stores only the base table name (e.g., "students")
- Does NOT track aliases (e.g., "s")
- Used in: ExpressionEvaluator for subqueries

**CombinedSchema** (used for joins):
- Stores table aliases in HashMap keys
- Properly handles qualified references like `c1.category_name`
- Used in: CombinedExpressionEvaluator for join operations

#### 2. Evaluator Mismatch

```rust
// crates/executor/src/evaluator/expressions/eval.rs
ast::Expression::ColumnRef { table: _, column } => {
    self.eval_column_ref(column, row)  // ❌ Table qualifier ignored
}

// crates/executor/src/evaluator/combined/eval.rs
ast::Expression::ColumnRef { table, column } => {
    self.schema.get_column_index(table.as_deref(), column)  // ✅ Qualifier used
}
```

### Debug Evidence

Running the batch generator with debug output revealed:

```
/// Example: join-4 - CROSS JOIN
DEBUG: Looking for c1.category_name in schemas: ["c1"]
DEBUG: Found table c1, schema.name=categories, looking for column
DEBUG: Looking for c2.category_name in schemas: ["c1"]  ← Only c1 exists!
DEBUG: Table c2 not found in schemas
⏭️  SKIP: Execution error - ColumnNotFound("category_name")
```

**Key Finding**: The second table in the self-join (`c2`) is not present in the schema when the WHERE clause is evaluated, suggesting the SELECT list and WHERE are evaluated before the CROSS JOIN fully completes.

## Proposed Solutions

### Option 1: Fix Query Execution Order (Recommended)

**Change execution flow** so FROM clause fully completes before SELECT/WHERE evaluation:

```
Parser → FROM clause → Schema building → WHERE → SELECT → GROUP BY → ORDER BY
              ↓              ↓
         All joins      All aliases
         complete       established
```

**Impact**:
- Fixes all 4 failing examples
- Aligns with SQL standard semantics
- Requires refactoring SelectExecutor

**Estimated Effort**: 8-12 hours

**Files to Modify**:
- `crates/executor/src/select/executor/execute.rs`
- `crates/executor/src/select/executor/nonagg.rs`
- `crates/executor/src/select/executor/aggregation.rs`

### Option 2: Enhance TableSchema to Track Aliases

**Add alias tracking** to TableSchema:

```rust
pub struct TableSchema {
    pub name: String,
    pub alias: Option<String>,  // ← Add this
    pub columns: Vec<ColumnSchema>,
    // ...
}
```

**Impact**:
- Fixes uni-1, uni-5 (correlated subqueries)
- Does NOT fix join-4 (execution order issue)
- Simpler than Option 1

**Estimated Effort**: 4-6 hours

**Files to Modify**:
- `crates/catalog/src/table.rs`
- `crates/executor/src/select/scan.rs`
- `crates/executor/src/evaluator/expressions/eval.rs`

### Option 3: Use CombinedSchema Everywhere

**Replace TableSchema** with CombinedSchema throughout:

```rust
// Instead of passing TableSchema to subquery evaluator
let evaluator = ExpressionEvaluator::new(table_schema, ...);

// Pass CombinedSchema
let combined = CombinedSchema::from_table(alias, table_schema);
let evaluator = CombinedExpressionEvaluator::new(combined, ...);
```

**Impact**:
- Fixes uni-1, uni-5
- Consistent schema handling
- Does NOT fix join-4 (execution order issue)

**Estimated Effort**: 6-8 hours

**Files to Modify**:
- All evaluator instantiation sites
- Subquery execution in `crates/executor/src/evaluator/expressions/subqueries.rs`

### Hybrid Approach (Best Solution)

**Combine Option 1 + Option 2/3**:
1. Fix execution order for join-4
2. Use CombinedSchema or enhance TableSchema for correlated subqueries

**Impact**: Fixes all examples comprehensively

**Estimated Effort**: 10-14 hours

## Testing Strategy

### Verification Tests

After implementing fixes, verify with:

```bash
# Run batch generator to check all examples
cargo run --example batch_results_generator 2>&1 | grep "join-4\|uni-1\|uni-5\|window-8"

# Should show success instead of SKIP
```

### Regression Tests

Add specific test cases:

```rust
#[test]
fn test_self_join_with_aliases() {
    // join-4 scenario
    let sql = r#"
        SELECT c1.name, c2.name
        FROM categories c1
        CROSS JOIN categories c2
        WHERE c1.id < c2.id
    "#;
    // ...
}

#[test]
fn test_correlated_subquery_with_alias() {
    // uni-1 scenario
    let sql = r#"
        SELECT s.name,
               (SELECT COUNT(*) FROM enrollments e
                WHERE e.student_id = s.student_id) AS count
        FROM students s
    "#;
    // ...
}
```

## Related Work

- **Issue #382**: Successfully increased web demo coverage to 70%
- **Recent fixes**:
  - ORDER BY with aggregates
  - LEFT/RIGHT function support
  - University database population

## References

- **SQL:1999 Standard**: Section 7.6 (Table reference) and 7.12 (Query specification)
- **Execution order**: FROM → WHERE → SELECT → ORDER BY (standard semantics)
- **Alias scope**: Table aliases should be visible throughout query

## Conclusion

The column resolution issues are **not simple bugs** but rather **architectural limitations** in:
1. Query execution ordering
2. Schema representation during execution
3. Alias tracking through the pipeline

A complete fix requires refactoring query execution to match SQL standard semantics where FROM clause fully completes before other clauses evaluate.

The hybrid approach (Option 1 + Option 2/3) is recommended for comprehensive resolution.
