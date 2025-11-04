# Subquery Implementation Analysis - NIST MemSQL Database

## Executive Summary

This document provides a comprehensive analysis of how subqueries are currently implemented in the NIST MemSQL Rust SQL database, with particular focus on understanding the architecture needed to support **correlated subqueries** (Issue #82, Phase 4).

## Current Implementation Status

### Completed Features (Phases 1-3)

#### 1. **Scalar Subqueries** ✅
**Status**: Fully implemented and tested
**PR**: #107, #100
**Files**: 
- AST: `/crates/ast/src/expression.rs` - `Expression::ScalarSubquery(Box<SelectStmt>)`
- Executor: `/crates/executor/src/evaluator.rs` - Both `ExpressionEvaluator` and `CombinedExpressionEvaluator`
- Tests: `/crates/executor/src/tests/scalar_subqueries.rs`

**How it works**:
```rust
// AST Definition
ScalarSubquery(Box<SelectStmt>)  // Wraps a complete SELECT statement

// Execution in ExpressionEvaluator (lines 110-145)
ast::Expression::ScalarSubquery(subquery) => {
    let database = self.database.ok_or(...)?;
    let select_executor = crate::select::SelectExecutor::new(database);
    let rows = select_executor.execute(subquery)?;
    
    // Validation
    if rows.len() > 1 {  // SQL:1999: must return exactly 1 row
        return Err(ExecutorError::SubqueryReturnedMultipleRows { expected: 1, actual: rows.len() });
    }
    if subquery.select_list.len() != 1 {  // Must return exactly 1 column
        return Err(ExecutorError::SubqueryColumnCountMismatch { ... });
    }
    
    // Return single value or NULL if no rows
    if rows.is_empty() {
        Ok(types::SqlValue::Null)
    } else {
        rows[0].get(0).cloned()
    }
}
```

**Limitations**:
- Non-correlated only: Cannot reference outer query columns
- Database reference required: `ExpressionEvaluator::with_database()` must be used
- No outer context: Lacks `outer_row` and `outer_schema` in subquery evaluation

#### 2. **IN Operator with Subqueries** ⚠️
**Status**: Parsed but NOT executed
**PR**: #96
**Files**: 
- AST: `/crates/ast/src/expression.rs` - `Expression::In { expr, subquery, negated }`
- Executor: `/crates/executor/src/evaluator.rs` - Lines 98-107 (both evaluators)

**Current Code**:
```rust
ast::Expression::In { expr, subquery: _, negated: _ } => {
    let _left_val = self.eval(expr, row)?;
    Err(ExecutorError::UnsupportedFeature(
        "IN with subquery requires database access - implementation pending".to_string(),
    ))
}
```

**Why it's pending**:
- Requires database access to execute the subquery
- Needs to compare left expression against all subquery result rows
- More complex than scalar subqueries

#### 3. **FROM Clause Subqueries (Derived Tables)** ✅
**Status**: Fully implemented and tested
**PR**: #114
**Files**: 
- AST: `/crates/ast/src/select.rs` - `FromClause::Subquery { query, alias }`
- Executor: `/crates/executor/src/select/mod.rs` - Lines 293-338

**How it works**:
```rust
// AST Definition
FromClause::Subquery { 
    query: Box<SelectStmt>, 
    alias: String 
}

// Execution
ast::FromClause::Subquery { query, alias } => {
    let rows = self.execute(query)?;  // Execute the subquery
    
    // Derive schema from SELECT list
    let mut column_names = Vec::new();
    let mut column_types = Vec::new();
    
    for (i, item) in query.select_list.iter().enumerate() {
        // Extract column name and infer type from data
    }
    
    // Create schema with table alias
    let schema = CombinedSchema::from_derived_table(alias.clone(), column_names, column_types);
    Ok(FromResult { schema, rows })
}
```

**Features**:
- Subquery results treated as derived table
- Requires mandatory AS alias
- Schema inferred from result rows and column aliases

### Partially Implemented Features

#### 4. **Correlated Subquery Infrastructure** 🔄
**Status**: Partial infrastructure in place
**PR**: #93
**Files**: `/crates/executor/src/evaluator.rs` - Lines 5-50

**What's implemented**:
```rust
pub struct ExpressionEvaluator<'a> {
    schema: &'a catalog::TableSchema,
    outer_row: Option<&'a storage::Row>,        // ✅ Storage for outer row
    outer_schema: Option<&'a catalog::TableSchema>, // ✅ Storage for outer schema
    database: Option<&'a storage::Database>,    // ✅ For executing subqueries
}

// Constructor for correlated subqueries
pub fn with_outer_context(
    schema: &'a catalog::TableSchema,
    outer_row: &'a storage::Row,
    outer_schema: &'a catalog::TableSchema,
) -> Self { ... }
```

**Column resolution with outer context** (Lines 61-83):
```rust
ast::Expression::ColumnRef { table: _, column } => {
    // Try to resolve in inner schema first
    if let Some(col_index) = self.schema.get_column_index(column) {
        return row.get(col_index).cloned();
    }
    
    // If not found in inner schema and outer context exists, try outer schema
    if let (Some(outer_row), Some(outer_schema)) = (self.outer_row, self.outer_schema) {
        if let Some(col_index) = outer_schema.get_column_index(column) {
            return outer_row.get(col_index).cloned();
        }
    }
    
    Err(ExecutorError::ColumnNotFound(column.clone()))
}
```

**Tests for outer context** (Lines 439-541):
- `test_evaluator_with_outer_context_resolves_inner_column` ✅
- `test_evaluator_with_outer_context_resolves_outer_column` ✅
- `test_evaluator_with_outer_context_inner_shadows_outer` ✅
- `test_evaluator_with_outer_context_column_not_found` ✅
- `test_evaluator_without_outer_context` ✅

**What's missing**:
- No mechanism to pass outer context when evaluating subqueries
- Subqueries always executed without outer row context
- Need to modify scalar subquery execution to propagate outer context

## Architecture Analysis

### Expression Evaluator Architecture

```
ExpressionEvaluator<'a>
├── schema: &'a TableSchema                    // Current table's schema
├── outer_row: Option<&'a Row>                 // For correlated subqueries
├── outer_schema: Option<&'a TableSchema>      // For correlated subqueries
└── database: Option<&'a Database>             // For executing subqueries

CombinedExpressionEvaluator<'a>
├── schema: &'a CombinedSchema                 // Combined schema from JOINs
└── database: Option<&'a Database>             // For executing subqueries
```

### Key Design Decisions

**1. Dual Evaluator Pattern**
- `ExpressionEvaluator`: Single table context
- `CombinedExpressionEvaluator`: Multiple table context (for JOINs)
- Both support database access for subquery execution
- Only `ExpressionEvaluator` has outer context support

**2. Schema Management**
```
Single Table:
row values = [col0, col1, col2, ...]
TableSchema.columns = [col_schema0, col_schema1, col_schema2, ...]
column_index maps to direct row index

Combined (for JOINs):
row values = [left_col0, left_col1, right_col0, right_col1, ...]
CombinedSchema tracks:
  - table_schemas: Map<table_name, (start_index, TableSchema)>
  - For each table, know where its columns start
  - column lookup calculates: start_index + local_index
```

**3. Database Access Pattern**
```rust
// With database reference
let evaluator = ExpressionEvaluator::with_database(&schema, database);

// For scalar subqueries
ast::Expression::ScalarSubquery(subquery) => {
    let database = self.database.ok_or(...)?;
    let select_executor = SelectExecutor::new(database);
    let rows = select_executor.execute(subquery)?;
    // ...
}
```

### Column Resolution Order (Current)

```
For ColumnRef { table: None, column: "X" }:

1. Check inner schema (current/inner table)
   - TableSchema.get_column_index("X")
   
2. If not found AND outer context exists:
   - Check outer schema
   - outer_schema.get_column_index("X")
   
3. If not found in either:
   - Error: ColumnNotFound
```

**For JOINs** (uses CombinedExpressionEvaluator):
```
1. Check if qualified (table.column):
   - Look up in table's schema
   - Calculate: start_index + local_index
   
2. If unqualified:
   - Search all tables in order
   - First match wins
   - Calculate: start_index + local_index
```

## Subquery Execution Flow

### Current Non-Correlated Scalar Subquery

```
SelectExecutor::execute()
  │
  ├─→ execute_with_aggregation() or execute_without_aggregation()
  │
  ├─→ (In WHERE clause evaluation)
  │   evaluator.eval(where_expr, row)
  │
  └─→ (If expr contains ScalarSubquery)
      │
      ├─→ Create ExpressionEvaluator::with_database(schema, database)
      │
      ├─→ eval(Expression::ScalarSubquery(subquery), row)
      │   │
      │   ├─→ Create NEW SelectExecutor
      │   │
      │   ├─→ Execute subquery WITHOUT outer context
      │   │   (outer_row and outer_schema are None)
      │   │
      │   └─→ Return single value
      │
      └─→ (No correlation with outer row)
```

### Missing Flow for Correlated Subqueries

**What's needed**:
```
When evaluating scalar subquery:
1. Create evaluator with outer_context:
   ExpressionEvaluator::with_outer_context(
       inner_schema,      // Subquery's FROM clause schema
       outer_row,         // Current row from outer query
       outer_schema       // Outer query's schema
   )
   
2. Pass to SelectExecutor somehow:
   - SelectExecutor needs ability to pass outer context
   - Currently SelectExecutor doesn't know about outer context
   - Need to modify evaluate chain
   
3. During column resolution in subquery:
   - First check subquery's FROM clause schema
   - Then check outer schema
   - This happens automatically via evaluator logic
```

## Storage Layer

### Row Structure

```rust
pub struct Row {
    pub values: Vec<SqlValue>,
}

impl Row {
    pub fn get(&self, index: usize) -> Option<&SqlValue>
    pub fn new(values: Vec<SqlValue>) -> Self
}
```

**Key points**:
- Rows are just ordered lists of values
- No metadata about columns within the row
- Schema provides mapping from column names to indices

### Database Structure

```rust
pub struct Database {
    tables: HashMap<String, Table>,
}

pub struct Table {
    pub schema: catalog::TableSchema,
    rows: Vec<Row>,
}
```

**Key points**:
- Database is purely in-memory
- Tables indexed by name
- No persistence

## Execution Paths for Subqueries

### Path 1: Scalar Subquery in WHERE Clause
```
SELECT * FROM employees 
WHERE salary > (SELECT AVG(salary) FROM employees)

Flow:
1. execute() → execute_without_aggregation()
2. For each row:
   a. evaluator.eval(where_clause, row)
   b. Evaluates: salary > ScalarSubquery(...)
   c. Left side: evaluator.eval(ColumnRef "salary", row)
   d. Right side: evaluator.eval(ScalarSubquery(...), row)
      i. Creates SelectExecutor
      ii. Executes subquery (WITHOUT outer context)
      iii. Returns single value
   e. Compares values
3. Include row if result is true
```

### Path 2: Scalar Subquery in SELECT List
```
SELECT id, (SELECT COUNT(*) FROM orders) as total_orders 
FROM users

Flow:
1. execute() → execute_without_aggregation()
2. For each row:
   a. project_row_combined() called
   b. For each SelectItem:
      i. evaluator.eval(expr, row)
      ii. If expr is ScalarSubquery, executes same as Path 1
3. Project row with all evaluated expressions
```

### Path 3: FROM Clause Subquery
```
SELECT * FROM (SELECT * FROM users WHERE active = TRUE) AS active_users

Flow:
1. execute() → execute_without_aggregation()
2. execute_from(FromClause::Subquery {...})
   a. Executes subquery first: SelectExecutor::execute(query)
   b. Derives schema from SELECT list
   c. Returns FromResult with rows and schema
3. Continue processing with result as if it's a table
```

### Path 4: IN Operator (Pending)
```
SELECT * FROM orders WHERE user_id IN (SELECT id FROM users)

Current status: PARSE ✅, EXECUTE ❌
Pending implementation would:
1. Execute subquery to get all result rows
2. Collect all values from first column
3. Check if left_value is in the set
```

## Type System Integration

### SqlValue Types
```rust
pub enum SqlValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Varchar(String),
    // Plus other types...
}

impl SqlValue {
    pub fn get_type(&self) -> DataType  // Infer type from runtime value
}
```

**Used for**:
- Subquery result type inference (when creating derived tables)
- Three-valued logic (NULL comparisons)
- Type checking in WHERE clauses

### DataType for Schema
```rust
pub enum DataType {
    Integer,
    Varchar { max_length: usize },
    Boolean,
    Null,
    // Plus other types...
}
```

**Used for**:
- Table schema definition
- Column type validation
- Query result schema

## Error Handling

### Executor Errors Related to Subqueries
```rust
pub enum ExecutorError {
    SubqueryReturnedMultipleRows { expected: usize, actual: usize },
    SubqueryColumnCountMismatch { expected: usize, actual: usize },
    UnsupportedFeature(String),
    // Plus other errors...
}
```

**Usage**:
- Scalar subqueries: Validated to return exactly 1 row, 1 column
- IN operator: Pending - will validate result structure
- Table subqueries: Flexible, inferred schema

## Current Limitations & What's Missing

### For Correlated Subqueries

**Problem 1: No Context Propagation**
- Scalar subqueries always execute with `outer_row: None`
- SelectExecutor has no mechanism to pass outer context
- Need to thread outer context through entire evaluation chain

**Problem 2: CombinedExpressionEvaluator Lacks Outer Context**
- Used for JOINs
- Doesn't support `outer_row` and `outer_schema`
- Need correlated subqueries in JOIN contexts

**Problem 3: SelectExecutor Architecture**
- Takes only `&SelectStmt` and `&Database`
- No outer context parameter
- Would need significant refactoring to support correlation

**Problem 4: Aggregate Functions**
- No correlated subqueries in aggregate functions yet
- `evaluate_with_aggregates()` doesn't support outer context

### For IN Operator Subqueries

**Missing**:
- Subquery execution (have parsing only)
- Result collection and membership testing
- NULL handling in IN lists (special SQL semantics)

## Key Files & Code Locations

| Component | File | Lines | Purpose |
|-----------|------|-------|---------|
| ExpressionEvaluator | `/crates/executor/src/evaluator.rs` | 1-280 | Core expression evaluation |
| Scalar subquery eval | `/crates/executor/src/evaluator.rs` | 110-145 | Scalar subquery execution |
| Outer context | `/crates/executor/src/evaluator.rs` | 5-50 | Correlated subquery infrastructure |
| CombinedEvaluator | `/crates/executor/src/evaluator.rs` | 282-430 | JOIN context evaluation |
| SelectExecutor | `/crates/executor/src/select/mod.rs` | 1-459 | Query execution engine |
| FROM subqueries | `/crates/executor/src/select/mod.rs` | 293-338 | Derived table execution |
| CombinedSchema | `/crates/executor/src/schema.rs` | 1-80 | Schema for JOINs |
| Expression AST | `/crates/ast/src/expression.rs` | 1-62 | Expression definitions |
| SelectStmt AST | `/crates/ast/src/select.rs` | 1-80 | SELECT statement structure |
| Scalar subquery tests | `/crates/executor/src/tests/scalar_subqueries.rs` | 1-414 | Comprehensive test suite |
| Column resolution | `/crates/catalog/src/table.rs` | 16-23 | Schema lookup |

## Testing Infrastructure

### Test Files
1. `/crates/executor/src/tests/scalar_subqueries.rs` - 5 tests ✅
2. `/crates/executor/src/tests/select_joins.rs` - JOIN tests
3. `/crates/executor/src/tests/expression_eval.rs` - Basic expression tests
4. `/crates/parser/src/tests/subquery.rs` - Parser tests for all subquery types

### Test Coverage
- **Scalar subqueries**: Complete ✅
  - In WHERE clause
  - In SELECT list
  - Empty result (NULL)
  - Error cases (multiple rows, multiple columns)
- **FROM subqueries**: Complete ✅
  - Basic derived table
  - Alias handling
  - Schema inference
- **IN operator**: Parsed only ⚠️
  - No execution tests
- **Correlated**: Infrastructure tests only 🔄
  - Outer context column resolution
  - Shadowing behavior

## SQL:1999 Compliance Notes

From SQL:1999 Section 7.9 (Scalar Subquery):
> "A scalar subquery shall contain a select list consisting of a single value expression."
> "If the result of the query contains zero rows, the result of the scalar subquery is the null value."
> "If the result of the query contains one row, the result of the scalar subquery is the value of the selected value expression."
> "If the result of the query contains more than one row, an exception condition is raised: cardinality violation."

**Current Implementation**: ✅ Fully compliant

## Implementation Notes for Phase 4 (Correlated Subqueries)

### Recommended Approach

1. **Modify SelectExecutor** to accept optional outer context
2. **Update scalar subquery evaluation** to pass outer context
3. **Extend CombinedEvaluator** to support outer context
4. **Add correlated subquery tests** for all scenarios
5. **Implement IN operator subqueries** as secondary feature

### Key Challenges

1. **Lifetime Management**: Rust's borrow checker with nested contexts
2. **Performance**: Each outer row processes entire subquery (consider optimization later)
3. **Schema Conflicts**: Column resolution with multiple levels of nesting
4. **Test Complexity**: Many edge cases and corner cases to verify

