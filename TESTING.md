# Testing Standards

This document establishes the standard for organizing tests across the vibesql codebase.

## Overview

We follow [Rust's official test organization guidelines](https://doc.rust-lang.org/book/ch11-03-test-organization.html) to maintain consistency and clarity across the project.

## Test Organization Standard

### 1. **Small Unit Tests** → Inline `#[cfg(test)] mod tests`

**When to use**: Simple tests of module internals that don't require significant additional code.

**Characteristics**:
- Less than 100 lines total
- Tests private functions or module-level behavior
- Contained in the same file as implementation
- Uses `#[cfg(test)]` module blocks

**Example**:
```rust
// src/storage/cache.rs
pub struct Cache {
    data: HashMap<String, Vec<u8>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_insert_and_get() {
        let mut cache = Cache::new();
        cache.insert("key", vec![1, 2, 3]);
        assert_eq!(cache.get("key"), Some(&vec![1, 2, 3]));
    }
}
```

### 2. **Large Unit Test Suites** → `src/tests/` Directory

**When to use**: Comprehensive internal testing that would clutter source files.

**Characteristics**:
- More than 100 lines of tests
- Tests private interfaces or internal behavior
- Each test module is a separate file in `src/tests/`
- Uses `#[cfg(test)]` in the module definition
- Can test private functions via `use crate::*`

**File structure**:
```
crates/my-crate/
├── src/
│   ├── lib.rs
│   ├── core/
│   │   └── mod.rs
│   └── tests/
│       ├── mod.rs
│       ├── core_basic_tests.rs
│       ├── core_advanced_tests.rs
│       └── edge_cases_tests.rs
├── Cargo.toml
└── tests/          # Integration tests
```

**Example** (`src/tests/core_basic_tests.rs`):
```rust
#[cfg(test)]
mod tests {
    use crate::core::*;

    #[test]
    fn test_initialization() {
        // Test internal implementation
    }

    #[test]
    fn test_private_helper_function() {
        // Can access private functions
    }
}
```

### 3. **Integration Tests** → `tests/` Directory

**When to use**: Testing public API and cross-module interactions.

**Characteristics**:
- Tests public API only (private internals not accessible)
- Each file compiled as a separate test binary
- No `#[cfg(test)]` wrapper needed
- File path becomes module name in test compilation
- Can import only public items from the crate

**File structure**:
```
crates/my-crate/
├── src/
│   └── lib.rs (public API)
├── tests/
│   ├── public_api_basic_tests.rs
│   ├── public_api_advanced_tests.rs
│   └── cross_module_integration_tests.rs
└── Cargo.toml
```

**Example** (`tests/public_api_basic_tests.rs`):
```rust
// No #[cfg(test)] wrapper - entire file is a test crate
use my_crate::PublicType;

#[test]
fn test_public_api_usage() {
    let obj = PublicType::new();
    assert!(obj.is_valid());
}
```

## Decision Flowchart

Use this flowchart to decide where to place tests for new code:

```
┌─ Does the test need access to private internals?
│
├─ YES
│  │
│  └─ Is the test < 100 lines?
│     ├─ YES → Inline #[cfg(test)] mod in same file
│     └─ NO  → Create file in src/tests/ directory
│
└─ NO
   │
   └─ Create file in tests/ directory (integration test)
```

## Examples by Crate

### vibesql-executor

Large crate with comprehensive testing. Follows the split pattern:

- **`src/tests/`**: Internal executor implementation tests
  - `aggregate_*.rs` - Aggregate function internals
  - `executor_state_tests.rs` - Internal state management
  - `optimizer_*.rs` - Optimizer internals
  
- **`tests/`**: Public executor API integration tests
  - `basic_query_tests.rs` - Public SELECT/INSERT/UPDATE/DELETE
  - `transaction_tests.rs` - Public transaction API
  - `performance_tests.rs` - Public API performance benchmarks

### vibesql-types

Mixed organization (being standardized):

- **`tests/`**: Public type system API tests (preferred location)
  - `type_ordering_tests.rs` - Public ordering guarantees
  - `type_display_tests.rs` - Public display format
  - `comparison_tests.rs` - Public Eq/Ord behavior for BTreeMap
  
- **No `src/tests/`**: Types crate should have minimal internal complexity

### vibesql-parser

Comprehensive parser testing in `src/tests/`:

- **`src/tests/`**: Parser internals
  - `lexer_*.rs` - Lexer implementation details
  - `parser_statements_tests.rs` - Parser implementation
  - `error_recovery_tests.rs` - Error handling internals

## Migration Checklist

When migrating existing tests, follow this checklist:

- [ ] Identify test file location and purpose
- [ ] Determine if test needs private API access
- [ ] Count lines of test code
- [ ] Check if test is already correctly placed
- [ ] If moving:
  - [ ] Copy test content to correct location
  - [ ] Update any `use` statements/imports
  - [ ] Run `cargo test` to verify
  - [ ] Remove old file
  - [ ] Commit with message: `refactor: move tests for X to proper location`

## Guidelines for Test Files

### Naming Convention

- Use descriptive names that indicate what's being tested
- Suffix with `_tests.rs` or `_integration_tests.rs`
- Examples:
  - `aggregate_count_tests.rs` - Tests for COUNT aggregate
  - `transaction_isolation_tests.rs` - Transaction isolation tests
  - `btree_consistency_tests.rs` - BTreeMap consistency checks

### Module Structure in `src/tests/`

Each test file should have a single `#[cfg(test)] mod tests` block:

```rust
#[cfg(test)]
mod tests {
    use crate::*;
    
    // All test functions here
    
    #[test]
    fn test_something() { }
}
```

### Module Structure in `tests/` (Integration Tests)

No module wrapper needed; use the file directly:

```rust
use crate::*;

#[test]
fn test_public_api() { }

#[test]
fn test_another_api() { }
```

### Test Helpers and Utilities

If multiple test files need shared utilities:

1. **For unit tests** (`src/tests/`): Create `src/tests/helpers.rs`
   ```rust
   #[cfg(test)]
   pub mod helpers {
       pub fn create_test_fixture() -> TestStruct { }
   }
   ```
   Then import: `use super::super::tests::helpers::*;`

2. **For integration tests** (`tests/`): Create `tests/common/mod.rs`
   ```rust
   pub mod setup {
       pub fn create_test_database() -> Db { }
   }
   ```
   Then import: `mod common; use common::setup::*;`

## Running Tests

### Run all tests
```bash
cargo test --all
```

### Run tests for specific crate
```bash
cargo test --package vibesql-executor
```

### Run specific test
```bash
cargo test test_name
```

### Run with output
```bash
cargo test -- --nocapture
```

### Run unit tests only (no integration tests)
```bash
cargo test --lib
```

### Run integration tests only
```bash
cargo test --test '*'
```

## Best Practices

1. **Keep tests close to code**: Inline tests are preferred for simple cases
2. **Test behavior, not implementation**: Focus on public contracts
3. **Descriptive test names**: `test_query_with_null_values` is better than `test_1`
4. **Organize large test suites**: Use separate files by feature/component
5. **Use test helpers**: Create reusable test setup functions
6. **Run tests before PR**: Ensure all tests pass locally
7. **Document test intent**: Add comments for complex test scenarios

## Troubleshooting

### "cannot find X in this scope"
- Tests in `tests/` can only access public items
- If you need private items, move to `src/tests/` directory

### "test module declared multiple times"
- Ensure only one `#[cfg(test)] mod tests` block per file
- If migrating from `src/tests/` to inline, remove the old file

### Tests not running
- Ensure file is in correct location
- Run `cargo test --all` with `--verbose` flag
- Check Cargo.toml for proper test configuration

## See Also

- [Rust Book: Test Organization](https://doc.rust-lang.org/book/ch11-03-test-organization.html)
- [Rust By Example: Testing](https://doc.rust-lang.org/rust-by-example/testing.html)
- [Testing in Rust (O'Reilly)](https://doc.rust-lang.org/book/ch11-00-testing.html)
