# UPDATE Subquery Tests Organization

This directory contains tests for UPDATE statements with subqueries, organized into focused modules for better maintainability.

## File Structure

```
update_subquery_tests/
├── README.md                          # This file
├── mod.rs                             # Module organization and documentation
├── simple_scalar_subqueries.rs        # Tests for scalar subqueries in SET clauses (646 lines)
├── error_cases.rs                     # Tests for error handling (147 lines)
├── where_in_subqueries.rs             # Tests for IN/NOT IN predicates (406 lines)
└── where_comparison_subqueries.rs     # Tests for comparison operators (392 lines)
```

## Test Modules

### `simple_scalar_subqueries.rs` (646 lines, 10 tests)
Tests for scalar subqueries in SET clauses that return single values:

- **Basic subqueries**: Single value returns
- **Aggregate functions**: MAX, MIN, AVG
- **NULL handling**: Subqueries returning NULL
- **Empty results**: Subqueries returning no rows
- **Multiple subqueries**: Multiple SET assignments with subqueries
- **WHERE filtering**: Combining SET subqueries with WHERE clauses

**Example tests**:
- `test_update_with_scalar_subquery_single_value`
- `test_update_with_scalar_subquery_max_aggregate`
- `test_update_with_multiple_subqueries`

### `error_cases.rs` (147 lines, 2 tests)
Tests for error conditions when using subqueries:

- **Multiple rows error**: Scalar subquery returning multiple rows
- **Multiple columns error**: Scalar subquery returning multiple columns

**Example tests**:
- `test_update_with_subquery_multiple_rows_error`
- `test_update_with_subquery_multiple_columns_error`

### `where_in_subqueries.rs` (406 lines, 6 tests)
Tests for IN and NOT IN subqueries in WHERE clauses:

- **IN operator**: Filtering rows with IN subqueries
- **NOT IN operator**: Filtering rows with NOT IN subqueries
- **Empty results**: Handling empty subquery results
- **Complex conditions**: Subqueries with WHERE clauses
- **Multiple rows**: Subqueries returning multiple rows (valid for IN)

**Example tests**:
- `test_update_where_in_subquery`
- `test_update_where_not_in_subquery`
- `test_update_where_complex_subquery_condition`

### `where_comparison_subqueries.rs` (392 lines, 5 tests)
Tests for comparison operators with scalar subqueries in WHERE clauses:

- **Equality operator**: WHERE column = (SELECT ...)
- **Comparison operators**: <, >, <=, >=
- **NULL handling**: Subqueries returning NULL in comparisons
- **Aggregate functions**: Using MAX, MIN, AVG in WHERE subqueries
- **Combined SET and WHERE**: Using subqueries in both SET and WHERE

**Example tests**:
- `test_update_where_scalar_subquery_equal`
- `test_update_where_scalar_subquery_less_than`
- `test_update_where_and_set_both_use_subqueries`

## Running Tests

```bash
# Run all UPDATE subquery tests
cargo test --package executor --test update_subquery_tests

# Run tests from a specific module
cargo test --package executor --test update_subquery_tests simple_scalar

# Run a specific test
cargo test --package executor --test update_subquery_tests test_update_with_scalar_subquery_single_value
```

## Test Coverage Summary

| Category | Tests | Lines | Coverage |
|----------|-------|-------|----------|
| Simple Scalar Subqueries | 10 | 646 | SET clause subqueries with aggregates, NULL, empty results |
| Error Cases | 2 | 147 | Multiple rows/columns error handling |
| WHERE IN Subqueries | 6 | 406 | IN/NOT IN operators with subqueries |
| WHERE Comparison Subqueries | 5 | 392 | Comparison operators with scalar subqueries |
| **Total** | **21** | **1,591** | **Comprehensive subquery support** |

## Related Issues

- **Issue #353**: UPDATE WHERE with subquery support
- **Issue #619**: Refactor update_subquery_tests.rs into focused modules

## History

This test organization was created to split the original 1,576-line `update_subquery_tests.rs` file into smaller, more maintainable modules. Each module focuses on a specific aspect of UPDATE statement subquery functionality, making it easier to:

- Understand test coverage
- Add new tests
- Debug failing tests
- Maintain code quality

## Adding New Tests

When adding new tests, consider which module best fits the test:

1. **simple_scalar_subqueries.rs**: Tests for SET clause subqueries
2. **error_cases.rs**: Tests for error conditions
3. **where_in_subqueries.rs**: Tests for IN/NOT IN in WHERE clause
4. **where_comparison_subqueries.rs**: Tests for comparison operators in WHERE clause

If your test doesn't fit into any existing module, consider creating a new module and updating `mod.rs` to include it.
