# Subquery Implementation - Quick Reference Guide

## Status Overview

| Feature | Status | Implementation | Tests | PR |
|---------|--------|-----------------|-------|-----|
| **Scalar Subqueries** | ✅ COMPLETE | `/crates/executor/src/evaluator.rs` lines 110-145 | 5 tests | #107, #100 |
| **FROM Subqueries** | ✅ COMPLETE | `/crates/executor/src/select/mod.rs` lines 293-338 | ✅ | #114 |
| **IN Operator** | ⚠️ PENDING | Parsed, not executed | ❌ | #96 |
| **Correlated Support** | 🔄 PARTIAL | Infrastructure only | 5 tests | #93 |

## Key Files Map

```
ARCHITECTURE & DESIGN:
  SUBQUERY_ARCHITECTURE.md
  SUBQUERY_ARCHITECTURE_DIAGRAMS.md

IMPLEMENTATION:
  /crates/executor/src/evaluator.rs          ← Main work goes here
  /crates/executor/src/select/mod.rs
  /crates/ast/src/expression.rs
  /crates/ast/src/select.rs

TESTS:
  /crates/executor/src/tests/scalar_subqueries.rs
  /crates/executor/src/tests/expression_eval.rs
  /crates/parser/src/tests/subquery.rs

SCHEMA & STORAGE:
  /crates/executor/src/schema.rs             ← CombinedSchema
  /crates/catalog/src/table.rs
  /crates/storage/src/row.rs
  /crates/storage/src/database.rs
```

## ExpressionEvaluator Structure

```rust
pub struct ExpressionEvaluator<'a> {
    schema: &'a TableSchema,              // Current table schema
    outer_row: Option<&'a Row>,           // For correlated subqueries (MOSTLY UNUSED)
    outer_schema: Option<&'a TableSchema>, // For correlated subqueries (MOSTLY UNUSED)
    database: Option<&'a Database>,       // For subquery execution (WORKING)
}

// Key methods
new(schema)                                // Basic init, no subqueries
with_database(schema, database)            // With subquery support
with_outer_context(...)                    // With correlation support (NOT USED YET)
eval(expr, row) -> Result<SqlValue>        // Evaluate expression
```

## Scalar Subquery Execution Path

```
WHERE clause evaluation:
  eval(BinaryOp { left, op: >, right: ScalarSubquery(...) }, row)
    → eval(left, row)  // e.g., "salary" field
    → eval(right, row) // Executes subquery:
      1. SelectExecutor::new(database)
      2. execute(subquery_stmt) → Vec<Row>
      3. Validate: 1 row, 1 column
      4. Return value or NULL
    → Compare and return Boolean
```

## Column Resolution Chain (Current)

```
When evaluating ColumnRef { table: None, column: "X" }:

1. Try to find X in inner schema
   schema.get_column_index("X") → Some(idx)
   return row.values[idx]

2. If not found, try outer schema (IF context exists)
   outer_schema.get_column_index("X") → Some(idx)
   return outer_row.values[idx]

3. If still not found:
   return Error::ColumnNotFound(X)
```

## Critical Insight: The Missing Link for Correlated Subqueries

**THE PROBLEM**: When a scalar subquery is executed, it's created with fresh evaluator that has NO outer context:

```rust
// Current code (line 116-117 in evaluator.rs)
let select_executor = crate::select::SelectExecutor::new(database);
let rows = select_executor.execute(subquery)?;  // ← NO OUTER CONTEXT PASSED
```

**WHAT'S NEEDED**: Pass outer context through SelectExecutor:

```rust
// Option 1: Extend SelectExecutor to accept context
let select_executor = crate::select::SelectExecutor::new_with_context(
    database,
    self.outer_row,        // Current becomes outer
    self.outer_schema      // Current schema becomes outer schema
);

// Option 2: Create child evaluator in subquery execution
// For each row in subquery evaluation, use evaluator with context set
```

## Test Scenarios to Understand

### 1. Basic Scalar Subquery (WORKS)
```sql
SELECT * FROM employees 
WHERE salary > (SELECT AVG(salary) FROM employees)
```
File: `scalar_subqueries.rs` line 12-110

### 2. Scalar in SELECT List (WORKS)
```sql
SELECT id, (SELECT COUNT(*) FROM orders) as order_count
FROM users
```
File: `scalar_subqueries.rs` line 113-211

### 3. Empty Result (WORKS)
```sql
SELECT (SELECT id FROM employees WHERE id = 999)
-- Returns NULL
```
File: `scalar_subqueries.rs` line 214-270

### 4. Correlated Subquery (NOT YET WORKING)
```sql
SELECT e.id FROM employees e
WHERE EXISTS (SELECT 1 FROM orders WHERE orders.emp_id = e.id)
-- Needs: outer context with "e.id" value
```

### 5. IN Operator (NOT IMPLEMENTED)
```sql
SELECT * FROM employees 
WHERE id IN (SELECT emp_id FROM orders)
-- Needs: subquery execution + membership test
```

## Quick Implementation Checklist for Phase 4

- [ ] **Step 1**: Understand current outer_context infrastructure
  - Read `ExpressionEvaluator::with_outer_context()` constructor
  - Review column resolution logic (lines 62-83)
  - Run existing outer context tests

- [ ] **Step 2**: Create test for correlated subqueries
  - Add test in `scalar_subqueries.rs`
  - Simple query: `WHERE (SELECT COUNT(*) FROM ... WHERE ... = outer.column) > 0`
  - Make it fail first (TDD)

- [ ] **Step 3**: Modify scalar subquery execution to pass context
  - In `evaluator.rs` lines 110-145
  - Need to pass `outer_row` and `outer_schema` to subquery

- [ ] **Step 4**: Refactor SelectExecutor (or create variant)
  - Either extend `execute()` with optional context parameter
  - Or create parallel execution path
  - Thread context through FROM clause execution

- [ ] **Step 5**: Test edge cases
  - Multiple correlation levels
  - Column shadowing
  - Correlation in JOINs
  - Correlation in GROUP BY aggregates

## Common Pitfalls to Avoid

1. **Lifetime Issues**: Rust borrow checker will complain about nested lifetimes
   - Solution: Use references carefully, may need restructuring

2. **Column Name Ambiguity**: Same column in inner and outer
   - Current behavior: Inner shadows outer (correct per SQL)
   - Test with schema: outer.col1, inner.col1

3. **NULL Semantics**: Subquery returning NULL for correlation
   - Must handle properly in comparisons
   - Three-valued logic applies

4. **Performance**: Each outer row re-executes subquery
   - Not an issue for Phase 4 (correctness first)
   - Future optimization: caching, semi-joins

5. **CombinedEvaluator Gap**: Lacks outer context for JOINs
   - May need separate handling for correlated subqueries in JOINs
   - Lower priority for Phase 4

## External References

- **SQL:1999 Standard**: Section 7.9 (Scalar Subquery), Section 7.2 (Table Subquery)
- **Rust Lifetime Rules**: Will be the hardest part
- **Three-Valued Logic**: Understand NULL behavior thoroughly
- **Existing Tests**: `/crates/executor/src/tests/scalar_subqueries.rs` is gold standard

## Files to Read in Order

1. **Start**: `/crates/executor/src/evaluator.rs` - Understand structure
2. **Then**: `/crates/executor/src/tests/scalar_subqueries.rs` - See what works
3. **Next**: `/crates/executor/src/select/mod.rs` - Understand SelectExecutor
4. **Deep Dive**: `/crates/ast/src/expression.rs` - AST structure
5. **Reference**: `/crates/executor/src/schema.rs` - Schema management

## Testing Commands

```bash
# Run all subquery tests
cargo test --lib executor::tests::scalar_subqueries

# Run with verbose output
cargo test --lib executor::tests::scalar_subqueries -- --nocapture

# Run parser tests for subqueries
cargo test --lib parser::tests::subquery

# Run outer context tests
cargo test outer_context

# Run all executor tests
cargo test --lib executor
```

## SQL Compliance Notes

Per SQL:1999:
- Scalar subquery must return exactly 1 row, 1 column
- If 0 rows: return NULL (not error)
- If >1 rows: ERROR - cardinality violation
- Correlation happens via ColumnRef resolution
- IN operator has special NULL semantics (different from =)

## Performance Considerations (Future)

For Phase 4, ignore performance. But know that:
- Current implementation: O(n*m) where n=outer rows, m=subquery executions
- Optimization opportunity: Semi-join, decorrelation, caching
- Batching: Could execute subquery once with multiple outer values
- Not priority unless tests fail due to timeout

