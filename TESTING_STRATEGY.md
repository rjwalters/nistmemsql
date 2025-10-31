# Testing Strategy

## Overview

This document describes the SQL:1999 conformance testing strategy, including current implementation status, test suite organization, and future enhancement plans.

## Current Implementation Status

### Test Suite Architecture

**Primary Test Suite**: SQL:1999 tests extracted from [sqltest by Elliot Chance](https://github.com/elliotchance/sqltest)

- **Location**: `tests/sql1999/manifest.json`
- **Test Count**: 739 tests covering Core SQL:1999 features
- **Source**: Tests extracted from sqltest's SQL:2016 Core and Foundation features
- **Organization**: Feature-based by SQL standard codes (E011, E021, F031, etc.)
- **Format**: JSON manifest with SQL statements and expected outcomes

**Test Runner**: `tests/sqltest_conformance.rs`
- Loads test manifest from JSON
- Parses SQL statements using our parser
- Executes against database engine
- Compares results and generates metrics
- Outputs results to `target/sqltest_results.json`

**Test Coverage** (as of current baseline):
- **E011-01**: Numeric data types (INTEGER, SMALLINT, BIGINT, FLOAT, DOUBLE)
- **E011-04**: Arithmetic operators (+, -, *, /)
- **E011-05**: Comparison predicates (<, <=, =, <>, >=, >)
- **E011-06**: Implicit casting between numeric types
- **E021**: Character string types (CHAR, VARCHAR)
- **F051**: Date/Time types and operations
- **E141**: Default values and constraints
- **F031**: Basic schema operations
- **Pass Rate**: **85.7%** (633/739 tests passing, strong foundation)

See `tests/sql1999/README.md` for details.

### Execution Strategy

**Current**: Direct Rust API testing
- Tests instantiate `Database` struct directly
- Execute SQL via parser → executor pipeline
- No protocol overhead (ODBC/JDBC)
- Fast iteration during development
- Identifies parser and executor gaps

**Future**: ODBC/JDBC protocol testing (see "Planned Enhancements" below)

### Automated Testing & Reporting

**GitHub Actions Integration**: `.github/workflows/deploy-demo.yml`

The CI pipeline automatically runs conformance tests and generates compliance artifacts:

1. **Test Execution** (lines 71-73):
   ```yaml
   - name: Run sqltest conformance suite
     run: cargo test --test sqltest_conformance --release -- --nocapture
     continue-on-error: true
   ```

2. **Badge Generation** (lines 76-111):
   ```yaml
   - name: Generate badge JSON
     run: |
       mkdir -p badges
       PASS_RATE=$(jq -r '.pass_rate // "0"' target/sqltest_results.json)

       # Determine color: 80%+ green, 60%+ yellow, 40%+ orange, <40% red
       # Creates shields.io endpoint format badge
       cat > badges/sql1999-conformance.json <<JSON
       {
         "schemaVersion": 1,
         "label": "SQL:1999",
         "message": "${PASS_RATE}%",
         "color": "$COLOR"
       }
       JSON
   ```

3. **Deployment**: Badge and results deployed to GitHub Pages
   - **Badge Endpoint**: https://rjwalters.github.io/nistmemsql/badges/sql1999-conformance.json
   - **README Badge**: Displays live conformance percentage (README.md:7)

**Compliance Report Generation**: `scripts/generate_compliance_report.sh`

Automated script that parses test results and generates `docs/SQL1999_CONFORMANCE.md`:

```bash
# Run after tests complete
./scripts/generate_compliance_report.sh

# Generates report with:
# - Summary metrics (total, passed, failed, errors, pass rate)
# - Test coverage details
# - Known gaps (parser/executor)
# - Improvement roadmap
```

The report provides:
- Summary metrics table
- Feature coverage breakdown
- Known implementation gaps (parser and executor)
- Phased improvement roadmap
- Local testing instructions

## Planned Enhancements

This section describes future testing strategies that are not yet implemented but are being considered for comprehensive SQL:1999 validation.

### Phase 1: ODBC/JDBC Protocol Testing (Planned)

**Status**: Not yet implemented (awaiting ODBC/JDBC driver development)

**Rationale**: Per upstream requirements, conformance tests should execute correctly through both ODBC and JDBC protocols to validate protocol compliance.

**Implementation Plan**:

Once ODBC and JDBC drivers are implemented, adapt the test suite for dual-protocol execution:

```
Test Definition Layer (protocol-agnostic)
         ↓
Protocol Adapter Layer
    ↙         ↘
ODBC Driver    JDBC Driver
    ↓            ↓
Database Engine
```

**Requirements**:
- All 100+ tests must pass via ODBC connection
- All 100+ tests must pass via JDBC connection
- Both protocols produce identical correct results

**GitHub Actions Matrix** (planned):
```yaml
test:
  strategy:
    matrix:
      protocol: [odbc, jdbc]
  steps:
    - name: Run tests via ${{ matrix.protocol }}
      run: ./run_tests.sh --protocol=${{ matrix.protocol }}
```

### Phase 2: SQLLogicTest Integration (Planned)

**Status**: Not yet implemented

**Rationale**: Industry-standard test framework used by SQLite, DuckDB, and other databases provides massive baseline coverage.

**Details**:
- **Scale**: 7+ million tests
- **Coverage**: Core SQL operations (portable subset)
- **Source**: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
- **Implementation**: sqllogictest-rs (Rust): https://github.com/risinglightdb/sqllogictest-rs

**Approach**:
1. Integrate sqllogictest-rs framework
2. Run comprehensive test suite for baseline validation
3. Complement existing SQL:1999 tests with broader coverage

**Benefits**:
- Massive test coverage (7M+ tests)
- Actively maintained
- Used by production databases (SQLite, DuckDB, etc.)
- Available in Rust
- Validates core SQL functionality beyond standard-specific features

### Phase 3: Expanded SQL:1999 Feature Coverage (Planned)

**Status**: Not yet implemented

**Rationale**: Extend beyond current 100-test baseline to cover all Core and Optional SQL:1999 features.

**Approach**:
1. Extract more tests from sqltest repository (currently 100, can expand to 200-500+)
2. Build custom tests based on ISO/IEC 9075:1999 specification
3. Cover all ~169 Core SQL:1999 features
4. Add tests for Optional features (triggers, stored procedures, recursive queries, etc.)

**Proposed Test Organization**:
```
tests/sql1999/
├── manifest.json (current: 100 tests)
├── core/ (planned: expanded coverage)
│   ├── e011_numeric_types/
│   ├── e021_character_types/
│   ├── f031_basic_schema/
│   ├── f041_basic_joins/
│   └── ...
└── optional/ (planned: new coverage)
    ├── t031_boolean_type/
    ├── t131_recursive_queries/
    ├── triggers/
    └── stored_procedures/
```

**Expansion Strategy**:
- Use `scripts/extract_sql1999_tests.py` to pull more tests from sqltest
- Supplement with hand-crafted tests for gaps
- Maintain JSON manifest format for consistency

### Phase 4: Additional Test Sources (Optional)

**NIST SQL-92 Tests**: If obtainable, provides historical baseline
**PostgreSQL Regression Tests**: Real-world SQL validation
**Apache Derby Tests**: JDBC-centric testing examples

## Test Result Tracking

### Current Tracking

**Automated via CI/CD**:
- Test results published to `target/sqltest_results.json`
- Compliance report generated in `docs/SQL1999_CONFORMANCE.md`
- Pass rate badge updated on GitHub Pages
- Metrics: total tests, passed, failed, errors, pass rate percentage

**Current Metrics** (739 tests):
- **85.7% pass rate** (633 passing, 106 errors)
- Strong conformance across Core SQL:1999 features
- Remaining gaps: Need to analyze 106 test failures for patterns and prioritization

### Future Compliance Matrix (Planned)

When ODBC/JDBC testing is implemented, track feature-by-feature compliance:

```markdown
| Feature ID | Feature Name | Core/Optional | Direct | ODBC | JDBC | Notes |
|------------|--------------|---------------|--------|------|------|-------|
| E011 | Numeric types | Core | ✅ | ✅ | ✅ | - |
| F031 | Basic schema | Core | ✅ | ✅ | ✅ | - |
| T031 | BOOLEAN type | Optional | ❌ | ❌ | ❌ | Issue #123 |
```

**Future Tracking Goals**:
- Separate tracking for Core vs Optional features
- Per-protocol breakdown (Direct, ODBC, JDBC)
- Trend over time (improving/regressing)
- Feature-by-feature status updates

## Coverage Reporting

1. Install [`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov) if it is not already available:

   ```bash
   cargo install cargo-llvm-cov
   ```

2. Generate an HTML report for the entire workspace:

   ```bash
   cargo coverage
   ```

   This writes the report to `target/coverage/html/html/index.html`.

3. Create an `lcov.info` artifact that can be uploaded to services such as Codecov:

   ```bash
   cargo coverage-lcov
   ```

   The file is emitted at `target/coverage/lcov.info`.

4. Clean cached instrumentation before re-running coverage:

   ```bash
   cargo coverage-clean
   ```

Coverage commands are defined as Cargo aliases in `.cargo/config.toml`, so the invocations above work consistently for every contributor and in automation.

## Test Development Priorities

### ✅ Completed: Foundation (Phase 1)
- [x] Basic test harness created (`tests/sqltest_conformance.rs`)
- [x] 739 SQL:1999 tests extracted from sqltest
- [x] JSON manifest format established
- [x] GitHub Actions integration with badge generation
- [x] Compliance report automation

### ✅ Completed: Strong Foundation Achievement (Phase 2)
- [x] Achieved **85.7% pass rate** (633/739 tests)
- [x] Fixed parser gaps (constraints, TRIM, CAST, comparison operators)
- [x] Fixed executor gaps (type coercion, constraint enforcement, transactions)
- [x] Strong Core SQL:1999 compliance demonstrated

### 🚧 Current: Analyzing Failures for 90%+ (Phase 3)
1. Analyze 106 remaining test failures
2. Identify common failure patterns
3. Prioritize high-impact fixes
4. Target: 90%+ pass rate on 739-test suite

### Planned: Test Coverage Expansion (Phase 3)
1. Extract 200-500 more tests from sqltest
2. Build tests for all ~169 Core SQL:1999 features
3. Organize expanded test coverage by feature codes
4. Add Optional feature tests (triggers, procedures, recursive queries)

### Future: Protocol & Advanced Testing (Phase 4)
1. Implement ODBC driver and test execution
2. Implement JDBC driver and test execution
3. Integrate SQLLogicTest (7M+ tests)
4. Edge cases, performance, concurrency tests

## Alternative Test Resources

### PostgreSQL Regression Tests
- **Source**: https://github.com/postgres/postgres/tree/master/src/test/regress
- **Coverage**: Extensive SQL tests including SQL:1999 features
- **License**: PostgreSQL License (permissive)
- **Value**: Real-world SQL from production database

### Apache Derby Tests
- **Source**: https://github.com/apache/derby
- **Coverage**: Java-based RDBMS with good SQL:1999 support
- **License**: Apache 2.0
- **Value**: JDBC-centric testing approach

### SQL Feature Comparison Sites
- **Modern-SQL.com**: Documents SQL standard features with examples
- **Use-The-Index-Luke.com**: SQL best practices and testing

## Deliverables

### Test Suite Components
1. **test/** directory with all tests
2. **scripts/** for test execution
3. **docs/COMPLIANCE.md** tracking feature status
4. **GitHub Actions** workflows for CI/CD
5. **Test results dashboard** (HTML report)

### Documentation
1. Test organization and structure
2. How to run tests locally
3. How to add new tests
4. Compliance report interpretation
5. Feature coverage matrix

## Success Criteria

### Current Phase Goals (Direct API Testing)

1. ✅ Basic test infrastructure established
2. ✅ Tests run automatically in GitHub Actions on every commit
3. ✅ Automated badge generation and compliance reporting
4. ✅ 80%+ pass rate achieved - now at **85.7%** (633/739 tests)
5. ✅ 739 tests covering Core SQL:1999 features
6. ✅ Clear gap identification for parser and executor improvements
7. 🚧 Target 90%+ pass rate (analyzing 106 remaining failures)

### Future Phase Goals (Full Compliance)

7. ⬜ ODBC and JDBC drivers implemented
8. ⬜ All tests executable via both ODBC and JDBC protocols
9. ⬜ Both protocols produce identical results for all tests
10. ⬜ Comprehensive test coverage of all SQL:1999 Core features (~169)
11. ⬜ Complete test coverage of SQL:1999 optional features (FULL compliance)
12. ⬜ 100% of tests passing (FULL SQL:1999 compliance achieved)

## Risks and Mitigations

### Risk 1: No Official NIST SQL:1999 Test Suite
- **Mitigation**: Build custom suite based on specification
- **Mitigation**: Use sqllogictest for baseline coverage
- **Mitigation**: Validate against multiple reference implementations

### Risk 2: Ambiguity in Standard Interpretation
- **Mitigation**: Test against PostgreSQL, Oracle, SQL Server for comparison
- **Mitigation**: Use Mimer SQL validator for syntax checking
- **Mitigation**: Document interpretation decisions

### Risk 3: Incomplete Feature Coverage
- **Mitigation**: Systematic approach using feature taxonomy
- **Mitigation**: Track coverage percentage continuously
- **Mitigation**: Regular audit against specification

### Risk 4: ODBC/JDBC Protocol Complexity
- **Mitigation**: Start simple, iterate toward full protocol support
- **Mitigation**: Use existing ODBC/JDBC drivers as reference
- **Mitigation**: Test incrementally as protocols are implemented

## Next Steps

### Immediate Priorities

1. ✅ **Fixed parser gaps** - Achieved 85.7% pass rate
   - ✅ Added constraint naming support
   - ✅ Enhanced TRIM syntax (BOTH/LEADING/TRAILING 'x' FROM s)
   - ✅ CAST to VARCHAR without explicit length
   - ✅ Comparison operators for all types

2. ✅ **Fixed executor gaps**
   - ✅ Type coercion working
   - ✅ All constraints enforced (NOT NULL, PRIMARY KEY, UNIQUE, CHECK, FOREIGN KEY)
   - ✅ Transaction support complete (BEGIN, COMMIT, ROLLBACK, SAVEPOINT)

3. 🚧 **Analyze and fix failures for 90%+ conformance** (current: 85.7%)
   - Analyze 106 remaining test failures
   - Group by failure patterns
   - Prioritize high-impact fixes
   - Systematically address gaps

### Medium-Term Goals

4. ✅ **Achieved 80%+ pass rate** - now at 85.7%
5. 🚧 **Target 90%+ pass rate** (analyzing 106 failures)
6. ⬜ **Build ODBC driver** (deferred - not required for Core compliance)
7. ⬜ **Build JDBC driver** (deferred - not required for Core compliance)

### Long-Term Vision

7. **Integrate SQLLogicTest** for baseline validation
8. **Cover all Core and Optional features** for FULL SQL:1999 compliance
9. **Achieve 100% test pass rate** across all protocols

## References

### Current Implementation Files

1. **Test Suite**: `tests/sql1999/manifest.json` - 100 SQL:1999 tests
2. **Test Runner**: `tests/sqltest_conformance.rs` - Main conformance test harness
3. **Test Documentation**: `tests/sql1999/README.md` - Test organization and usage
4. **Badge Generation**: `.github/workflows/deploy-demo.yml` (lines 76-111)
5. **Compliance Reporting**: `scripts/generate_compliance_report.sh`
6. **Compliance Report**: `docs/SQL1999_CONFORMANCE.md` (auto-generated)
7. **Badge Endpoint**: https://rjwalters.github.io/nistmemsql/badges/sql1999-conformance.json
8. **Test Results**: `target/sqltest_results.json` (generated after test runs)

### External Resources

9. **sqltest by Elliot Chance**: https://github.com/elliotchance/sqltest
10. **SQLLogicTest**: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
11. **sqllogictest-rs**: https://github.com/risinglightdb/sqllogictest-rs
12. **PostgreSQL Regression Tests**: https://github.com/postgres/postgres/tree/master/src/test/regress
13. **SQL:1999 Standard**: ISO/IEC 9075:1999 (purchase required)
14. **NIST SQL Test Suite**: https://www.itl.nist.gov/div897/ctg/sql_form.htm (historical, obsolete)
