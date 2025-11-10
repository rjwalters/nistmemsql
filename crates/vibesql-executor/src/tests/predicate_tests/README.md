# Predicate Tests - Test Coverage Documentation

This directory contains comprehensive tests for SQL predicates and expressions, organized by category for improved maintainability.

## Organization

Tests are split into focused modules, each under 250 lines:

| Module | Lines | Test Count | Description |
|--------|-------|------------|-------------|
| `in_predicate.rs` | 177 | 4 | IN/NOT IN list membership |
| `like_predicate.rs` | 237 | 5 | LIKE/NOT LIKE pattern matching |
| `between_predicate.rs` | 129 | 3 | BETWEEN/NOT BETWEEN range predicates |
| `position_function.rs` | 153 | 4 | POSITION string function |
| `trim_function.rs` | 220 | 6 | TRIM string function variations |
| `cast_expression.rs` | 107 | 3 | CAST type conversion |

**Total:** 25 tests across 6 modules (originally 1,003 lines in single file)

## Test Coverage Matrix

### IN Predicate Tests (`in_predicate.rs`)

| Test | Feature | SQL Standard | NULL Handling |
|------|---------|--------------|---------------|
| `test_in_list_basic` | IN list matching | SQL:1999 | No |
| `test_not_in_list` | NOT IN exclusion | SQL:1999 | No |
| `test_in_list_with_null_value` | NULL in expression | Three-valued logic | Yes |
| `test_in_list_with_null_in_list` | NULL in list | Three-valued logic | Yes |

**SQL Standard Compliance:**
- Three-valued logic (TRUE, FALSE, UNKNOWN)
- NULL comparison always returns UNKNOWN
- IN with NULL in list: non-matching value returns UNKNOWN (filtered out)

### LIKE Predicate Tests (`like_predicate.rs`)

| Test | Feature | Wildcard | NULL Handling |
|------|---------|----------|---------------|
| `test_like_wildcard_percent` | % (any sequence) | Yes | No |
| `test_like_wildcard_underscore` | _ (single char) | Yes | No |
| `test_not_like` | NOT LIKE negation | Yes | No |
| `test_like_null_pattern` | NULL pattern | N/A | Yes |
| `test_like_null_value` | NULL value | Yes | Yes |

**SQL Standard Compliance:**
- `%` matches any sequence of characters (including empty)
- `_` matches exactly one character
- NULL in pattern or value yields UNKNOWN (no rows match)

### BETWEEN Predicate Tests (`between_predicate.rs`)

| Test | Feature | Boundaries | NULL Handling |
|------|---------|------------|---------------|
| `test_between_with_null_expr` | NULL expression | Inclusive | Yes |
| `test_not_between` | NOT BETWEEN exclusion | Inclusive | No |
| `test_between_boundary_values` | Boundary inclusion | Inclusive | No |

**SQL Standard Compliance:**
- `BETWEEN low AND high` is inclusive of both boundaries
- Equivalent to `expr >= low AND expr <= high`
- NULL expression yields UNKNOWN (filtered out)

### POSITION Function Tests (`position_function.rs`)

| Test | Feature | Result | NULL Handling |
|------|---------|--------|---------------|
| `test_position_found` | Substring found | 1-indexed position | No |
| `test_position_not_found` | Substring not found | Returns 0 | No |
| `test_position_null_substring` | NULL substring | Returns NULL | Yes |
| `test_position_null_string` | NULL string | Returns NULL | Yes |

**SQL Standard Compliance:**
- `POSITION(substring IN string)` returns 1-based index
- Returns 0 if substring not found
- Returns NULL if either argument is NULL

### TRIM Function Tests (`trim_function.rs`)

| Test | Feature | Position | NULL Handling |
|------|---------|----------|---------------|
| `test_trim_both_default` | Default (BOTH, space) | Both | No |
| `test_trim_leading` | LEADING trim | Left | No |
| `test_trim_trailing` | TRAILING trim | Right | No |
| `test_trim_custom_char` | Custom removal char | Both | No |
| `test_trim_null_string` | NULL string | N/A | Yes |
| `test_trim_null_removal_char` | NULL removal char | N/A | Yes |

**SQL Standard Compliance:**
- `TRIM([LEADING|TRAILING|BOTH] [char FROM] string)`
- Default position: BOTH
- Default removal character: space
- NULL in any argument yields NULL

### CAST Expression Tests (`cast_expression.rs`)

| Test | Feature | Conversion | NULL Handling |
|------|---------|------------|---------------|
| `test_cast_integer_to_varchar` | Integer → VARCHAR | Numeric string | No |
| `test_cast_varchar_to_integer` | VARCHAR → Integer | Parse string | No |
| `test_cast_null` | NULL preservation | N/A | Yes |

**SQL Standard Compliance:**
- `CAST(expr AS type)` converts expression to target type
- NULL casts to NULL of target type (type-preserving NULL)
- Type conversion follows SQL:1999 rules

## SQL:1999 Standard Compliance

All tests verify compliance with SQL:1999 standards:

### Three-Valued Logic
- **TRUE**: Predicate evaluates to true
- **FALSE**: Predicate evaluates to false
- **UNKNOWN**: Predicate involves NULL (treated as FALSE in WHERE clauses)

### NULL Propagation Rules
- Any comparison with NULL yields UNKNOWN
- Arithmetic operations with NULL yield NULL
- String functions with NULL yield NULL
- CAST(NULL AS type) yields typed NULL

### Predicate Semantics
- IN predicate: `expr IN (v1, v2, ...)` ≡ `expr = v1 OR expr = v2 OR ...`
- BETWEEN predicate: `expr BETWEEN low AND high` ≡ `expr >= low AND expr <= high`
- LIKE predicate: Pattern matching with `%` (any) and `_` (single char)

## Edge Cases Covered

### NULL Handling
- ✅ NULL in expressions (IN, LIKE, BETWEEN, functions)
- ✅ NULL in lists (IN predicate)
- ✅ NULL in patterns (LIKE predicate)
- ✅ NULL preservation in CAST

### Boundary Conditions
- ✅ BETWEEN boundary inclusion (both ends)
- ✅ POSITION returns 0 when not found, 1-indexed when found
- ✅ Empty string handling in LIKE and TRIM
- ✅ Wildcard matching edge cases

### Negation
- ✅ NOT IN list exclusion
- ✅ NOT LIKE pattern negation
- ✅ NOT BETWEEN range exclusion

## Running Tests

```bash
# Run all predicate tests
cargo test --package executor --lib predicate_tests

# Run specific module tests
cargo test --package executor --lib in_predicate
cargo test --package executor --lib like_predicate
cargo test --package executor --lib between_predicate
cargo test --package executor --lib position_function
cargo test --package executor --lib trim_function
cargo test --package executor --lib cast_expression

# Run specific test
cargo test --package executor --lib test_in_list_with_null_value
```

## Maintenance Guidelines

### Adding New Tests
1. Identify the appropriate module (IN, LIKE, BETWEEN, etc.)
2. Add test to the relevant `.rs` file
3. Update this README if adding a new test category
4. Ensure module stays under 250 lines (split if needed)

### Module Size Limits
- Target: < 200 lines per module
- Hard limit: 250 lines per module
- If exceeding, consider splitting by subcategory (e.g., basic vs edge cases)

### Documentation Requirements
- Each test should have clear comments explaining:
  - What SQL feature is being tested
  - Expected behavior
  - Edge case being validated
- Module header should list all features tested
- Update test coverage matrix when adding tests

## References

- **SQL:1999 Standard**: ISO/IEC 9075:1999
- **Three-Valued Logic**: SQL-92 and SQL:1999 NULL handling
- **Predicate Specifications**: SQL:1999 Part 2 (Foundation)
- **String Functions**: SQL:1999 Part 2, Section 6.27 (TRIM), 6.28 (POSITION)
