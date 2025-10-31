# SQL:1999 Conformance Report

**Generated**: Fri Oct 31 12:28:51 PDT 2025
**Commit**: 2175578

## Summary

| Metric | Value |
|--------|-------|
| Total Tests | 0 |
| Passed | 0 ✅ |
| Failed | 0 ❌ |
| Errors | 0 ⚠️ |
| Pass Rate | 0.0% |

## Test Coverage

Test suite from [sqltest](https://github.com/elliotchance/sqltest) - upstream-recommended SQL:1999 conformance tests.

Coverage includes:

- **E011**: Numeric data types
- **E021**: Character string types
- **E031**: Identifiers
- **E051**: Basic query specification
- **E061**: Basic predicates and search conditions
- **E071**: Basic query expressions
- **E081**: Basic privileges
- **E091**: Set functions
- **E101**: Basic data manipulation
- **E111**: Single row SELECT statement
- **E121**: Basic cursor support
- **E131**: Null value support
- **E141**: Basic integrity constraints
- **E151**: Transaction support
- **E161**: SQL comments
- **F031**: Basic schema manipulation
- Plus additional features from the F-series

## Running Tests Locally

```bash
# Run all conformance tests
cargo test --test sqltest_conformance -- --nocapture

# Generate coverage report
cargo coverage
# Open coverage report
open target/llvm-cov/html/index.html
```

