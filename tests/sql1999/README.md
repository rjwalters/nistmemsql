# SQL:1999 Conformance Tests

This directory contains SQL:1999 conformance tests extracted from the [sqltest suite](https://github.com/elliotchance/sqltest) by Elliot Chance.

## Contents

- `manifest.json` - Test manifest containing 100 SQL:1999-compatible tests from SQL:2016 Core and Foundation features

## Test Organization

Tests are organized by SQL standard feature codes:

- **E0xx** - Core features (numeric types, character types, basic predicates, etc.)
- **F0xx** - Foundation features (basic schema manipulation, joins, etc.)

## Running Tests

```bash
# Run all SQL:1999 conformance tests
cargo test --test sqltest_conformance

# Run with output
cargo test --test sqltest_conformance -- --nocapture
```

## Test Results

Test results are saved to `target/sqltest_results.json` after each run, showing:
- Total tests run
- Passed/failed/error counts
- Pass rate percentage

## Adding More Tests

To extract more tests from the sqltest repository:

```bash
# Edit limit in scripts/extract_sql1999_tests.py
# Then run:
python3 scripts/extract_sql1999_tests.py
```

## Current Status

- **Initial baseline**: 100 tests from Core features (E011-E021)
- **Pass rate**: ~42% (as of initial integration)
- **Common failures**: Unary +/- operators, DECIMAL type, numeric type coercion

These failures identify specific areas for parser and executor improvements.
