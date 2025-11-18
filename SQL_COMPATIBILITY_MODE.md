# SQL Compatibility Guide: MySQL vs SQLite

This document outlines the behavioral differences between MySQL and SQLite that affect VibeSQL's implementation.

## Table of Contents
- [Arithmetic Operators](#arithmetic-operators)
- [Type System](#type-system)
- [String Operations](#string-operations)
- [Comparison Operations](#comparison-operations)
- [NULL Handling](#null-handling)
- [Boolean Values](#boolean-values)
- [Functions](#functions)
- [Implementation Status](#implementation-status)

---

## Arithmetic Operators

### Division Operator (`/`)

**Most Important Difference**

| Database | Expression | Result Type | Value | Behavior |
|----------|------------|-------------|-------|----------|
| **MySQL** | `5/2` | `DECIMAL` | `2.5000` | Always decimal division |
| **SQLite** | `5/2` | `INTEGER` | `2` | Integer division if both operands are integers |
| **SQLite** | `5.0/2` | `REAL` | `2.5` | Real division if any operand is real |

**VibeSQL Implementation:**
- MySQL mode: Returns `Numeric` for exact decimal arithmetic (matches MySQL `DECIMAL` type)
- SQLite mode: Returns `Integer` for int/int, `Float` for mixed types
- Location: `crates/vibesql-executor/src/evaluator/operators/arithmetic/division.rs`

### Integer Division Operator (`DIV`)

| Database | Operator | Expression | Result | Notes |
|----------|----------|------------|--------|-------|
| **MySQL** | `DIV` | `5 DIV 2` | `2` | MySQL-specific operator |
| **SQLite** | N/A | Not supported | Error | Use CAST to integer instead |

**VibeSQL Implementation:**
- MySQL mode: Supported via `integer_divide()` function
- SQLite mode: Not applicable (would return error)

### Modulo Operator (`%`)

| Database | Expression | Result | Notes |
|----------|------------|--------|-------|
| **MySQL** | `10 % 3` | `1` | Also supports `MOD(10, 3)` function |
| **SQLite** | `10 % 3` | `1` | Operator only, no `MOD()` function |

Both databases support the `%` operator with identical behavior.

---

## Type System

### Numeric Types

| MySQL Type | SQLite Type | VibeSQL Type | Notes |
|------------|-------------|--------------|-------|
| `INTEGER` | `INTEGER` | `SqlValue::Integer` | 64-bit signed integer |
| `DECIMAL/NUMERIC` | N/A | `SqlValue::Numeric` | Exact decimal (f64) |
| `FLOAT/DOUBLE` | `REAL` | `SqlValue::Float` | Approximate (f32) |

**Key Differences:**
- MySQL has distinct DECIMAL (exact) and FLOAT (approximate) types
- SQLite has only INTEGER and REAL (no exact decimal type)
- SQLite uses dynamic typing (type affinity)

### Boolean Values

| Database | TRUE | FALSE | Notes |
|----------|------|-------|-------|
| **MySQL** | `1` | `0` | Native boolean literals, stored as TINYINT(1) |
| **SQLite** | `1` | `0` | No native boolean, uses integers |

Both effectively treat booleans as integers, but MySQL has syntactic sugar.

### Type Coercion

**MySQL:**
- Strict mode can prevent automatic coercion
- String-to-number conversion is permissive: `'123abc' + 0 = 123`
- Mixed type operations follow specific precedence rules

**SQLite:**
- Dynamic typing with type affinity
- More permissive automatic type conversion
- String-to-number: `'123' + 0 = 123` (via type affinity)

---

## String Operations

### String Concatenation

| Database | Operator | Example | Result | Notes |
|----------|----------|---------|--------|-------|
| **MySQL** | `CONCAT()` | `CONCAT('A', 'B')` | `'AB'` | Function-based |
| **MySQL** | `\|\|` | `'A' \|\| 'B'` | `'AB'` | Only in PIPES_AS_CONCAT mode |
| **SQLite** | `\|\|` | `'A' \|\| 'B'` | `'AB'` | Standard operator |

**Important:** MySQL uses `CONCAT()` function by default. The `||` operator is OR unless `PIPES_AS_CONCAT` SQL mode is enabled.

### String Comparison

| Database | Expression | Default Result | Notes |
|----------|------------|----------------|-------|
| **MySQL** | `'abc' = 'ABC'` | `1` (TRUE) | Case-insensitive by default (depends on collation) |
| **SQLite** | `'abc' = 'ABC'` | `0` (FALSE) | Case-sensitive by default |

**Collations:**
- MySQL: Supports collations like `utf8mb4_general_ci` (case-insensitive)
- SQLite: Supports BINARY, NOCASE, RTRIM collations

---

## Comparison Operations

### Three-Valued Logic (NULL comparisons)

Both MySQL and SQLite follow SQL standard three-valued logic:

| Expression | Result | Explanation |
|------------|--------|-------------|
| `NULL = NULL` | `NULL` | Unknown (not TRUE) |
| `NULL <> NULL` | `NULL` | Unknown (not TRUE) |
| `NULL IS NULL` | `TRUE` | Identity test |
| `NULL AND TRUE` | `NULL` | Unknown |
| `NULL OR TRUE` | `TRUE` | Short-circuit |

**Both databases:**
- `NULL` in arithmetic returns `NULL`
- `NULL` in comparisons returns `NULL` (unknown)
- Use `IS NULL` / `IS NOT NULL` for identity tests

---

## NULL Handling

### NULL in Aggregates

| Function | MySQL Behavior | SQLite Behavior | Notes |
|----------|----------------|-----------------|-------|
| `COUNT(col)` | Ignores NULLs | Ignores NULLs | Both identical |
| `SUM(col)` | Ignores NULLs | Ignores NULLs | Returns NULL if all NULL |
| `AVG(col)` | Ignores NULLs | Ignores NULLs | Returns NULL if all NULL |
| `MIN(col)` | Ignores NULLs | Ignores NULLs | Returns NULL if all NULL |
| `MAX(col)` | Ignores NULLs | Ignores NULLs | Returns NULL if all NULL |

Both databases handle NULL in aggregates identically (standard SQL behavior).

### NULL in String Operations

| Database | Expression | Result | Notes |
|----------|------------|--------|-------|
| **MySQL** | `CONCAT('A', NULL)` | `NULL` | NULL propagates |
| **SQLite** | `'A' \|\| NULL` | `NULL` | NULL propagates |

Both propagate NULL in string concatenation.

### Division by Zero

| Database | Expression | Result | Notes |
|----------|------------|--------|-------|
| **MySQL** | `5/0` | `NULL` | Standard behavior |
| **SQLite** | `5/0` | `NULL` | Standard behavior |
| **MySQL** | `5 DIV 0` | `NULL` | Integer division by zero |

**VibeSQL Implementation:** Returns `NULL` for division by zero (matches both databases).

---

## Functions

### Common Functions (Both Databases)

| Function | MySQL | SQLite | Notes |
|----------|-------|--------|-------|
| `COALESCE()` | ✅ | ✅ | Returns first non-NULL value |
| `IFNULL()` / `ISNULL()` | ✅ | ❌ | MySQL-specific, use COALESCE in SQLite |
| `NULLIF()` | ✅ | ✅ | Returns NULL if arguments equal |
| `ABS()` | ✅ | ✅ | Absolute value |
| `ROUND()` | ✅ | ✅ | Rounding (slightly different behavior) |
| `LENGTH()` | ✅ | ✅ | String length (bytes in MySQL, chars in SQLite) |

### MySQL-Specific Functions

- `CONCAT()` - String concatenation
- `IF()` - Conditional expression
- `IFNULL()` - NULL coalescing (2 args only)
- `GREATEST()` / `LEAST()` - Min/max of multiple values
- Date functions: `NOW()`, `CURDATE()`, `DATE_ADD()`, etc.

### SQLite-Specific Functions

- `SUBSTR()` - Substring (MySQL uses `SUBSTRING()`)
- `TRIM()` - Different syntax than MySQL
- `GLOB()` - Pattern matching (like LIKE but case-sensitive)
- `RANDOM()` - Random number (MySQL uses `RAND()`)

---

## Operators

### Logical Operators

| Operator | MySQL | SQLite | Notes |
|----------|-------|--------|-------|
| `AND` | ✅ | ✅ | Logical AND |
| `OR` | ✅ | ✅ | Logical OR |
| `NOT` | ✅ | ✅ | Logical NOT |
| `XOR` | ✅ | ❌ | MySQL-specific (use `(A AND NOT B) OR (NOT A AND B)`) |

### Bitwise Operators

Both databases support: `&` (AND), `|` (OR), `^` (XOR), `<<` (left shift), `>>` (right shift).

### BETWEEN Operator

Both support `BETWEEN` with identical semantics:
- Inclusive range: `x BETWEEN a AND b` ≡ `x >= a AND x <= b`
- Three-valued logic with NULLs

**VibeSQL Implementation:** Correctly implements three-valued logic for `BETWEEN` with NULLs.

---

## Syntax Differences

### CAST Operations

| Database | Syntax | Example | Notes |
|----------|--------|---------|-------|
| **MySQL** | `CAST(x AS type)` | `CAST(5 AS SIGNED)` | SIGNED, UNSIGNED, DECIMAL, etc. |
| **SQLite** | `CAST(x AS type)` | `CAST(5 AS INTEGER)` | INTEGER, REAL, TEXT, BLOB |

Type names differ between databases.

### LIMIT / OFFSET

Both support:
```sql
SELECT ... LIMIT n OFFSET m
```

MySQL also supports legacy syntax: `LIMIT m, n` (offset m, limit n).

### Comments

Both support:
- `--` single-line comments
- `/* ... */` multi-line comments

MySQL also supports `#` for single-line comments.

---

## Implementation Status

### Currently Implemented in VibeSQL

✅ **Working:**
- Division operator mode-specific behavior (int/int)
- DIV operator (MySQL mode)
- Three-valued logic for NULL comparisons
- BETWEEN with NULL handling
- Basic arithmetic operators
- Type coercion for numeric operations

⚠️ **Partial:**
- Division returns Float instead of Numeric in MySQL mode
- String concatenation (CONCAT vs ||)
- Function library (limited set implemented)

❌ **Not Yet Implemented:**
- Full collation support
- All MySQL/SQLite-specific functions
- String comparison case sensitivity modes
- Full type affinity system (SQLite)

### Testing

**Test Coverage:**
- SQLLogicTest suite (from dolthub, generated from MySQL 8)
- Custom unit tests for division behavior
- NULL handling tests

**Known Issues:**
- MySQL mode should use `Numeric` instead of `Float` for exact decimal arithmetic
- Need comprehensive collation testing
- Need mode-specific function tests

---

## Recommendations for VibeSQL Development

### High Priority

1. **~~Fix Division Type in MySQL Mode~~** ✅ **COMPLETED**
   - ~~Change `Float` → `Numeric` for exact decimal arithmetic~~ ✅ Implemented
   - ~~Update tests to verify precision~~ ✅ Tests updated and passing

2. **Document Mode Selection**
   - When to use MySQL mode (default for SQLLogicTest compatibility)
   - When to use SQLite mode (native SQLite app compatibility)

3. **Add Mode-Specific Function Mappings**
   - MySQL `CONCAT()` ↔ SQLite `||`
   - MySQL `IFNULL()` ↔ SQLite `COALESCE()`
   - Document function compatibility matrix

### Medium Priority

4. **String Comparison Modes**
   - Add collation support or case-sensitivity flag
   - Test case-sensitive vs case-insensitive behavior

5. **Type Coercion Rules**
   - Document and test string-to-number coercion
   - Verify mixed-type arithmetic behavior

6. **Comprehensive Test Suite**
   - Add mode-specific tests for all operators
   - Test edge cases (overflow, underflow, precision)
   - Test NULL propagation in all operations

### Low Priority

7. **Full Function Library**
   - Implement all MySQL 8.0 functions (for MySQL mode)
   - Implement all SQLite 3 functions (for SQLite mode)
   - Add function aliasing where compatible

8. **SQL Mode Flags**
   - Support MySQL `PIPES_AS_CONCAT` mode
   - Support other MySQL SQL modes
   - Make modes configurable per session

---

## References

- **MySQL Documentation:** https://dev.mysql.com/doc/refman/8.0/en/
- **SQLite Documentation:** https://www.sqlite.org/lang.html
- **SQL Standard:** ISO/IEC 9075 (SQL:2016)
- **VibeSQL Source:**
  - `crates/vibesql-types/src/sql_mode/` - SQL mode architecture
    - `mod.rs` - Main SqlMode enum and trait implementations
    - `operators.rs` - OperatorBehavior trait (division, XOR, DIV, concat)
    - `types.rs` - TypeBehavior trait (type inference, coercion)
    - `strings.rs` - StringBehavior trait (collations, case sensitivity)
    - `config.rs` - MySqlModeFlags configuration
  - `crates/vibesql-executor/src/evaluator/operators/arithmetic/division.rs` - Division operator implementation
  - `tests/sqllogictest_runner.rs` - Test runner

---

**Document Status:** Living document - update as new differences are discovered or implementations change.

**Last Updated:** 2025-11-17
