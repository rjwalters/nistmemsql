# Testing Strategy

## Overview

This document outlines the testing approach for achieving NIST SQL:1999 compliance, including test suite selection, execution strategy, and GitHub Actions integration.

## OFFICIAL TEST SUITE IDENTIFIED! ✅

**UPDATE**: Upstream maintainer has provided the official test suite to use!

**Test Suite**: [sqltest by Elliot Chance](https://github.com/elliotchance/sqltest)
- **Coverage**: SQL:1992, SQL:1999, SQL:2003, SQL:2011, SQL:2016
- **Organization**: Feature-based tests (E011-02, etc.)
- **Generation**: BNF-driven test generation from SQL standard syntax
- **Status**: Active, open source
- **Test Count**: Hundreds of tests auto-generated from BNF rules
- **Results**: Published at https://elliotchance.github.io/sqltest/

### How sqltest Works

1. **BNF Extraction**: Extracts Backus-Naur Form syntax rules from SQL standard documents
2. **Test Generation**: Uses `bnf.py` tool to reverse-engineer valid SQL statements from BNF
3. **Expansion**: Small template tests expand into hundreds of actual test cases
4. **Automation**: Scripts (`run.sh`, `generate_tests.py`) handle execution
5. **Reporting**: Generates HTML compliance reports

**Example**: 3 base templates for feature E011-02 → 70 individual test cases

## Primary Testing Strategy: sqltest Suite

**DECISION**: Use [sqltest](https://github.com/elliotchance/sqltest) as our primary conformance test suite, as directed by upstream maintainer.

### Why sqltest?

1. **Official Recommendation**: Upstream maintainer explicitly provided this suite
2. **Comprehensive Coverage**: Includes SQL:1999 tests (our target) plus SQL:92, 2003, 2011, 2016
3. **BNF-Driven**: Automatically generates tests from SQL standard BNF grammar
4. **Active Project**: Maintained and used by the SQL community
5. **Well-Organized**: Feature-based structure (E011, F031, etc.) matches standard taxonomy
6. **Proven**: Used for testing multiple database implementations

### Integration Plan

1. **Clone sqltest repository** as submodule or vendored dependency
2. **Focus on SQL:1999 tests** (standards/1999/ directory if exists, or filter from 2016)
3. **Adapt test runner** to work with ODBC and JDBC drivers
4. **Generate compliance reports** showing feature-by-feature status
5. **Automate in GitHub Actions** for CI/CD

### Test Execution Flow

```
sqltest Repository
      ↓
Extract SQL:1999 Tests
      ↓
   Test Runner (our code)
   ↙         ↘
ODBC Driver   JDBC Driver
   ↓            ↓
Our Database Engine
   ↓            ↓
Compare Results
   ↓
Compliance Report
```

## Supplementary Testing Approaches

### Approach 1: Use SQL-92 NIST Tests as Baseline (Optional)
**Rationale**: SQL:1999 is a superset of SQL-92; compliance with SQL-92 is necessary but not sufficient.

**Approach**:
1. Attempt to obtain NIST SQL-92 Test Suite Version 6.0
   - Check NIST archives
   - Contact NIST directly
   - Look for mirrors/distributions in academic repositories
2. Pass all NIST SQL-92 tests as baseline compliance
3. Supplement with SQL:1999-specific tests

**Pros**:
- Established, well-vetted test suite
- Tests fundamental SQL compliance
- Provides credibility baseline

**Cons**:
- Doesn't cover SQL:1999 features
- May no longer be available for download
- Designed for SQL-92, not modern standards

### Strategy 2: SQLLogicTest
**Rationale**: Industry-standard test framework used by SQLite, DuckDB, and other databases.

**Details**:
- **Scale**: 7+ million tests
- **Coverage**: Core SQL operations (portable subset)
- **Source**: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
- **Implementations**:
  - Original C implementation for SQLite
  - sqllogictest-rs: Rust implementation (https://github.com/risinglightdb/sqllogictest-rs)
  - Multiple forks for different databases

**Approach**:
1. Integrate sqllogictest framework
2. Run comprehensive test suite
3. Add custom SQL:1999-specific tests to fill gaps

**Pros**:
- Massive test coverage (7M+ tests)
- Actively maintained
- Used by production databases
- Database-neutral design
- Available in Rust (good for our likely implementation language)

**Cons**:
- Tests "core SQL" but not specifically SQL:1999 features
- Not "NIST" branded
- May need extensive customization for SQL:1999 features

### Strategy 3: Custom SQL:1999 Conformance Test Suite
**Rationale**: Build comprehensive tests directly from ISO/IEC 9075:1999 specification.

**Approach**:
1. Purchase official SQL:1999 standard documents
2. Extract conformance requirements from each section
3. Build test cases for each feature in the taxonomy
4. Cover all Core SQL:1999 features (~169)
5. Add tests for all optional SQL:1999 features (FULL compliance)

**Test Organization**:
```
tests/
├── core/
│   ├── e011_numeric_types/
│   ├── f031_basic_schema/
│   ├── f041_basic_joins/
│   └── ...
├── optional/
│   ├── t031_boolean_type/
│   ├── t131_recursive_queries/
│   ├── triggers/
│   ├── stored_procedures/
│   └── ...
├── odbc/
│   └── (test execution via ODBC)
└── jdbc/
    └── (test execution via JDBC)
```

**Pros**:
- Directly addresses SQL:1999 FULL compliance
- Can be comprehensive and authoritative
- Tailored to our specific requirements
- Can document conformance gaps clearly

**Cons**:
- Massive development effort
- Requires deep standard expertise
- Time-consuming to build
- May miss edge cases that established suites cover

### Strategy 4: Hybrid Approach (RECOMMENDED)

Combine multiple test sources for comprehensive coverage:

#### Tier 1: SQLLogicTest (Foundation)
- Baseline: Run full sqllogictest suite
- Validates core SQL operations work correctly
- Provides millions of test cases automatically
- Use sqllogictest-rs for Rust integration

#### Tier 2: SQL:1999 Feature Tests (Compliance)
- Build custom test suite organized by SQL:1999 feature codes
- One test directory per feature (E011, F031, T131, etc.)
- Cover all 169+ Core features
- Cover all optional features for FULL compliance
- Reference ISO/IEC 9075:1999 specification sections

#### Tier 3: NIST SQL-92 Tests (If Available)
- If obtainable, run NIST SQL-92 suite for historical compliance
- Validates compatibility with legacy systems
- Demonstrates traditional conformance

#### Tier 4: Real-World SQL (Practical Validation)
- Collect SQL from open-source applications
- Test queries from PostgreSQL, MySQL regression tests
- Example schemas and queries from database textbooks
- Ensures practical usability beyond spec compliance

## ODBC/JDBC Testing Requirement

Per upstream Issue #4 clarification: "The nist compat tests should function correctly when run through either ODBC or JDBC"

### Dual-Protocol Test Execution

**Requirement**: ALL tests must pass when executed via:
1. ODBC connection
2. JDBC connection

**Implementation Strategy**:

#### Test Framework Architecture
```
Test Definition Layer (protocol-agnostic)
         ↓
Protocol Adapter Layer
    ↙         ↘
ODBC Driver    JDBC Driver
    ↓            ↓
Database Engine
```

#### Test Execution Flow
1. **Define tests once** in protocol-agnostic format
2. **Execute via ODBC**: Connect through ODBC driver, run test suite
3. **Execute via JDBC**: Connect through JDBC driver, run same test suite
4. **Compare results**: Both must produce identical correct results

#### GitHub Actions Matrix
```yaml
test:
  strategy:
    matrix:
      protocol: [odbc, jdbc]
  steps:
    - name: Run tests via ${{ matrix.protocol }}
      run: ./run_tests.sh --protocol=${{ matrix.protocol }}
```

## GitHub Actions Integration

### CI/CD Pipeline Requirements

Per original problem statement: "Implement full NIST Compat tests as github action"

### Proposed GitHub Actions Workflow

```yaml
name: SQL:1999 Compliance Tests

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  sqllogictest:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Build database
        run: cargo build --release
      - name: Run SQLLogicTest suite
        run: ./scripts/run_sqllogictest.sh
      - name: Upload test results
        uses: actions/upload-artifact@v3
        with:
          name: sqllogictest-results
          path: test-results/sqllogictest/

  sql1999-core-tests:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        protocol: [odbc, jdbc]
    steps:
      - uses: actions/checkout@v3
      - name: Build database and ${{ matrix.protocol }} driver
        run: ./scripts/build_with_${{ matrix.protocol }}.sh
      - name: Run Core SQL:1999 tests via ${{ matrix.protocol }}
        run: ./scripts/run_core_tests.sh --protocol=${{ matrix.protocol }}
      - name: Upload results
        uses: actions/upload-artifact@v3
        with:
          name: core-tests-${{ matrix.protocol }}
          path: test-results/core/${{ matrix.protocol }}/

  sql1999-optional-tests:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        protocol: [odbc, jdbc]
        feature-group: [triggers, procedures, recursive, udt, arrays]
    steps:
      - uses: actions/checkout@v3
      - name: Build database and ${{ matrix.protocol }} driver
        run: ./scripts/build_with_${{ matrix.protocol }}.sh
      - name: Run ${{ matrix.feature-group }} tests via ${{ matrix.protocol }}
        run: ./scripts/run_optional_tests.sh --group=${{ matrix.feature-group }} --protocol=${{ matrix.protocol }}
      - name: Upload results
        uses: actions/upload-artifact@v3
        with:
          name: optional-tests-${{ matrix.feature-group }}-${{ matrix.protocol }}
          path: test-results/optional/${{ matrix.feature-group }}/${{ matrix.protocol }}/

  compliance-report:
    runs-on: ubuntu-latest
    needs: [sqllogictest, sql1999-core-tests, sql1999-optional-tests]
    steps:
      - uses: actions/checkout@v3
      - name: Download all test results
        uses: actions/download-artifact@v3
      - name: Generate compliance report
        run: ./scripts/generate_compliance_report.sh
      - name: Upload compliance report
        uses: actions/upload-artifact@v3
        with:
          name: compliance-report
          path: compliance-report.html
      - name: Comment PR with results
        if: github.event_name == 'pull_request'
        uses: actions/github-script@v6
        with:
          script: |
            const fs = require('fs');
            const report = fs.readFileSync('compliance-summary.md', 'utf8');
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: report
            });
```

## Test Result Tracking

### Compliance Matrix

Track feature-by-feature compliance in a structured format:

```markdown
| Feature ID | Feature Name | Core/Optional | ODBC Status | JDBC Status | Notes |
|------------|--------------|---------------|-------------|-------------|-------|
| E011 | Numeric types | Core | ✅ Pass | ✅ Pass | - |
| F031 | Basic schema | Core | ✅ Pass | ✅ Pass | - |
| T031 | BOOLEAN type | Optional | ❌ Fail | ❌ Fail | Issue #123 |
| T131 | Recursive queries | Optional | 🚧 WIP | 🚧 WIP | PR #45 |
```

### Continuous Tracking

- Track compliance percentage: X% of Y features passing
- Separate tracking for Core vs Optional features
- Per-protocol breakdown (ODBC vs JDBC)
- Trend over time (improving/regressing)

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

### Phase 1: Foundation (Weeks 1-4)
1. Set up sqllogictest-rs integration
2. Create basic test harness
3. Implement simple ODBC/JDBC test execution
4. Get first tests running end-to-end

### Phase 2: Core SQL:1999 (Months 2-6)
1. Build tests for all ~169 Core features
2. Organize by feature code (E011, F031, etc.)
3. Ensure both ODBC and JDBC execution
4. Track compliance gaps

### Phase 3: Optional Features (Months 7-12)
1. Triggers and stored procedures
2. Recursive queries
3. User-defined types
4. Arrays and advanced types
5. All remaining optional features

### Phase 4: Comprehensive Coverage (Months 12+)
1. Edge cases and error conditions
2. Performance tests
3. Concurrency and transaction tests
4. Integration with real-world SQL

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

The testing strategy succeeds when:

1. ✅ Comprehensive test coverage of all SQL:1999 Core features
2. ✅ Complete test coverage of all SQL:1999 optional features (FULL compliance)
3. ✅ All tests executable via both ODBC and JDBC
4. ✅ Tests run automatically in GitHub Actions on every commit
5. ✅ Clear compliance reporting showing feature-by-feature status
6. ✅ Both ODBC and JDBC produce identical results for all tests
7. ✅ 100% of tests passing (FULL SQL:1999 compliance achieved)

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

1. **Immediate**: Set up basic test infrastructure
2. **Week 1**: Integrate sqllogictest-rs
3. **Week 2**: Create first SQL:1999 feature tests
4. **Week 3**: Implement basic ODBC test execution
5. **Week 4**: Implement basic JDBC test execution
6. **Month 2**: Begin systematic Core feature test development
7. **Ongoing**: Continuous integration and compliance tracking

## References

1. NIST SQL Test Suite: https://www.itl.nist.gov/div897/ctg/sql_form.htm (obsolete)
2. SQLLogicTest: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
3. sqllogictest-rs: https://github.com/risinglightdb/sqllogictest-rs
4. PostgreSQL Tests: https://github.com/postgres/postgres/tree/master/src/test/regress
5. SQL:1999 Standard: ISO/IEC 9075:1999 (purchase required)
