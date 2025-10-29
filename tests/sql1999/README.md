# SQL:1999 Conformance Tests

This directory contains SQL:1999 conformance tests from the [sqltest suite](https://github.com/elliotchance/sqltest) by Elliot Chance.

## Test Source

Tests are loaded **directly from YAML files** in `third_party/sqltest/standards/2016/`:
- **E-series** (Core features): 93 test files
- **F-series** (Foundation features): 39 test files

No intermediate conversion or manifest file is needed - the test runner parses YAML files on demand.

## Running Tests

```bash
# Run all SQL:1999 conformance tests
cargo test --test sqltest_conformance

# Run with detailed output
cargo test --test sqltest_conformance -- --nocapture
```

## Test Results

Test results are saved to `target/sqltest_results.json` after each run, showing:
- Total tests run
- Passed/failed/error counts
- Pass rate percentage

## Current Status

- **Total tests**: 739 (automatically loaded from all YAML files)
- **Pass rate**: ~76.9% (568/739 passing)
- **Test categories**:
  - E0xx: Core SQL features
  - F0xx: Foundation features

## Common Gaps Identified

Tests help identify specific areas for improvement:
- **Parser gaps**: DEFAULT keyword in INSERT/UPDATE, VARCHAR casting, schema syntax variations
- **Executor gaps**: Date/time comparison operators, CURRENT_DATE/TIME/TIMESTAMP functions
- **Type system**: Some temporal type operations not yet supported

These failures provide a data-driven roadmap for SQL:1999 compliance improvements.

## Adding More Tests

To include additional test series, edit the glob patterns in `tests/sqltest_conformance.rs`:

```rust
let patterns = vec![
    "third_party/sqltest/standards/2016/E/*.tests.yml",
    "third_party/sqltest/standards/2016/F/*.tests.yml",
    "third_party/sqltest/standards/2016/T/*.tests.yml",  // Add more as needed
];
```

No extraction scripts or conversion needed - just add the pattern and the tests will be loaded automatically.
