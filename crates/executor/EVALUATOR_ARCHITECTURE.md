# Expression Evaluator Architecture

## Overview

The expression evaluation system has been refactored to use a modular architecture with clear separation of concerns. The evaluator is split into focused modules, each handling specific expression types.

## Structure

### Single-Table Evaluator (`ExpressionEvaluator`)

Located in `crates/executor/src/evaluator/expressions/`

- **`eval.rs`** - Main dispatch logic and column references
- **`predicates.rs`** - BETWEEN, LIKE, IN, POSITION, CAST
- **`subqueries.rs`** - Scalar subqueries, IN with subquery, EXISTS, quantified comparisons
- **`special.rs`** - CASE expressions and function calls
- **`operators.rs`** - Unary operators (+, -)

### Combined-Schema Evaluator (`CombinedExpressionEvaluator`)

Located in `crates/executor/src/evaluator/combined/`

Used for JOIN operations where rows are combined from multiple tables.

- **`eval.rs`** - Main dispatch logic and column references with table qualifiers
- **`predicates.rs`** - BETWEEN, LIKE, IN list, IS NULL
- **`subqueries.rs`** - Scalar subqueries, EXISTS, quantified comparisons
- **`special.rs`** - CASE expressions, CAST, function calls

### Shared Logic

Located in `crates/executor/src/evaluator/core.rs`

- **Binary operations** - Arithmetic, comparison, logical operators
- **Type coercion** - Cross-type numeric operations
- **Equality semantics** - NULL-safe equality for CASE expressions

## Benefits

1. **Modularity**: Each expression type is in its own module, making the code easier to navigate and understand

2. **Single Responsibility**: Each module has a clear, focused purpose

3. **Maintainability**: Changes to one expression type don't affect others

4. **Testability**: Individual evaluators can be tested independently

5. **Extensibility**: Adding new expression types is straightforward - create a new method in the appropriate module

6. **Reduced Complexity**: The original 437-line combined.rs file is now split into 4 focused modules of ~100-200 lines each

## Design Principles

### Expression Dispatch

Both evaluators use a main `eval()` method that dispatches to specialized methods based on expression type. This pattern:

- Keeps the main dispatch logic clean and readable
- Allows each expression type to have its own implementation
- Makes it easy to see what expressions are supported

### Shared Binary Operations

Binary operations are implemented once in `core.rs` as static methods. This ensures:

- Consistent behavior across all evaluators
- Single point of maintenance for type coercion logic
- No code duplication

### Module Organization

Each module contains related expression types:

- **Predicates**: Boolean-valued expressions (BETWEEN, LIKE, IN, IS NULL)
- **Subqueries**: Expressions involving nested SELECT statements
- **Special**: Unique constructs (CASE, CAST, functions)
- **Eval**: Main entry point and basic expression types (literals, column refs, binary ops)

## Future Enhancements

### Trait-Based Dispatch

The current implementation could be extended with a trait-based architecture:

```rust
trait ExpressionEval {
    fn eval(&self, expr: &ast::Expression, row: &storage::Row)
        -> Result<types::SqlValue, ExecutorError>;
}
```

This would enable:
- Dynamic dispatch for different evaluation contexts
- Plugin-style evaluator extensions
- More flexible testing and mocking

However, the current modular structure provides most benefits without the additional complexity of trait objects.

## Migration Guide

The refactoring maintains full API compatibility. All public methods remain unchanged:

- `CombinedExpressionEvaluator::eval()` - still the main entry point
- `ExpressionEvaluator::eval()` - unchanged interface
- Static helper methods - still accessible from core module

Existing code using these evaluators requires no changes.

## Performance

The refactoring has no performance impact:

- No additional indirection (no trait objects)
- Same evaluation logic, just organized differently
- All methods are still statically dispatched
- No heap allocations added

All existing tests pass without modification, confirming behavioral equivalence.
