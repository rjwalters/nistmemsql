# SQL:1999 Conformance Report

**Generated**: Fri Oct 31 13:46:45 PDT 2025
**Commit**: 44cc897

## Summary

| Metric | Value |
|--------|-------|
| Total Tests | 739 |
| Passed | 705 ✅ |
| Failed | 0 ❌ |
| Errors | 34 ⚠️ |
| Pass Rate | 95.4% |

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

## Test Reporting Policy

### MySQL-Specific Tests

The SQLLogicTest suite contains tests with MySQL-specific syntax extensions that are not part of the SQL:1999 standard. These tests are **included in test runs** but tracked separately for dual reporting metrics.

**MySQL patterns tracked**:
- **System variables**: `@@sql_mode`, `@@session.variable_name`
- **Session management**: `SET SESSION` commands
- **MySQL sql_mode flags**: `ONLY_FULL_GROUP_BY`

**Implementation**: The `cluster_mysql_specific_errors()` function in `scripts/analyze_test_failures.py` identifies MySQL-specific failures for separate metrics reporting.

**Impact**: Approximately 14 test files in `random/groupby/slt_good_*.test` contain MySQL-specific syntax. These are reported separately to provide:
- **SQL:1999 pass rate** - Standard compliance (excludes MySQL-specific tests)
- **MySQL compatibility rate** - MySQL dialect support (just these tests)
- **Overall pass rate** - All tests combined

**Rationale**: We want to track progress on both SQL:1999 standard compliance AND MySQL compatibility. Separate metrics help prioritize work and understand which failures are standard-related vs dialect-specific.

## Sample Failing Tests

The following tests are currently failing (showing first 10):

- **e081_09_01_01**: CREATE SCHEMA TABLE_E081_09_01_011; CREATE ROLE ROLE_E081_09_01_01; GRANT USAGE ON TABLE TABLE_E081_09_01_011 TO ROLE_E081_09_01_01
  - Error: Statement 3 failed: Execution error: TableNotFound("TABLE_E081_09_01_011")
- **e081_10_01_01**: CREATE SCHEMA TABLE_E081_10_01_011; CREATE ROLE ROLE_E081_10_01_01; GRANT EXECUTE ON TABLE TABLE_E081_10_01_011 TO ROLE_E081_10_01_01
  - Error: Statement 3 failed: Execution error: TableNotFound("TABLE_E081_10_01_011")
- **e121_04_01_01**: CREATE TABLE TABLE_E121_04_01_01 ( A INT ); DECLARE CUR_E121_04_01_01 CURSOR FOR SELECT A FROM TABLE_E121_04_01_01; OPEN CUR_E121_04_01_01
  - Error: Statement 3 failed: Parse error: ParseError { message: "Expected statement, found Identifier(\"OPEN\")" }
- **e121_07_01_01**: CREATE TABLE TABLE_E121_07_01_01 ( A INT ); DECLARE CUR_E121_07_01_01 CURSOR FOR SELECT A FROM TABLE_E121_07_01_01; DELETE FROM ONLY ( TABLE_E121_07_01_01 ) WHERE CURRENT OF CUR_E121_07_01_01
  - Error: Statement 3 failed: Parse error: ParseError { message: "Expected table name after DELETE FROM" }
- **e121_08_01_01**: CREATE TABLE TABLE_E121_08_01_01 ( A INT ); DECLARE CUR_E121_08_01_01 CURSOR FOR SELECT A FROM TABLE_E121_08_01_01; OPEN CUR_E121_08_01_01; CLOSE CUR_E121_08_01_01
  - Error: Statement 3 failed: Parse error: ParseError { message: "Expected statement, found Identifier(\"OPEN\")" }
- **e121_10_01_01**: CREATE TABLE TABLE_E121_10_01_011 ( A INT ); CREATE TABLE TABLE_E121_10_01_012 ( A INT ); DECLARE CUR_E121_10_01_01 CURSOR FOR SELECT A FROM TABLE_E121_10_01_011; OPEN CUR_E121_10_01_01; FETCH ABSOLUTE 2 FROM CUR_E121_10_01_01 INTO TABLE_E121_10_01_012
  - Error: Statement 4 failed: Parse error: ParseError { message: "Expected statement, found Identifier(\"OPEN\")" }
- **e121_10_01_02**: CREATE TABLE TABLE_E121_10_01_021 ( A INT ); CREATE TABLE TABLE_E121_10_01_022 ( A INT ); DECLARE CUR_E121_10_01_02 CURSOR FOR SELECT A FROM TABLE_E121_10_01_021; OPEN CUR_E121_10_01_02; FETCH CUR_E121_10_01_02 INTO TABLE_E121_10_01_022
  - Error: Statement 4 failed: Parse error: ParseError { message: "Expected statement, found Identifier(\"OPEN\")" }
- **e121_10_01_03**: CREATE TABLE TABLE_E121_10_01_031 ( A INT ); CREATE TABLE TABLE_E121_10_01_032 ( A INT ); DECLARE CUR_E121_10_01_03 CURSOR FOR SELECT A FROM TABLE_E121_10_01_031; OPEN CUR_E121_10_01_03; FETCH FIRST FROM CUR_E121_10_01_03 INTO TABLE_E121_10_01_032
  - Error: Statement 4 failed: Parse error: ParseError { message: "Expected statement, found Identifier(\"OPEN\")" }
- **e121_10_01_04**: CREATE TABLE TABLE_E121_10_01_041 ( A INT ); CREATE TABLE TABLE_E121_10_01_042 ( A INT ); DECLARE CUR_E121_10_01_04 CURSOR FOR SELECT A FROM TABLE_E121_10_01_041; OPEN CUR_E121_10_01_04; FETCH FROM CUR_E121_10_01_04 INTO TABLE_E121_10_01_042
  - Error: Statement 4 failed: Parse error: ParseError { message: "Expected statement, found Identifier(\"OPEN\")" }
- **e121_10_01_05**: CREATE TABLE TABLE_E121_10_01_051 ( A INT ); CREATE TABLE TABLE_E121_10_01_052 ( A INT ); DECLARE CUR_E121_10_01_05 CURSOR FOR SELECT A FROM TABLE_E121_10_01_051; OPEN CUR_E121_10_01_05; FETCH LAST FROM CUR_E121_10_01_05 INTO TABLE_E121_10_01_052
  - Error: Statement 4 failed: Parse error: ParseError { message: "Expected statement, found Identifier(\"OPEN\")" }

<details>
<summary>View all failing tests (click to expand)</summary>

```
e081_09_01_01: CREATE SCHEMA TABLE_E081_09_01_011; CREATE ROLE ROLE_E081_09_01_01; GRANT USAGE ON TABLE TABLE_E081_09_01_011 TO ROLE_E081_09_01_01
  Error: Statement 3 failed: Execution error: TableNotFound("TABLE_E081_09_01_011")

e081_10_01_01: CREATE SCHEMA TABLE_E081_10_01_011; CREATE ROLE ROLE_E081_10_01_01; GRANT EXECUTE ON TABLE TABLE_E081_10_01_011 TO ROLE_E081_10_01_01
  Error: Statement 3 failed: Execution error: TableNotFound("TABLE_E081_10_01_011")

e121_04_01_01: CREATE TABLE TABLE_E121_04_01_01 ( A INT ); DECLARE CUR_E121_04_01_01 CURSOR FOR SELECT A FROM TABLE_E121_04_01_01; OPEN CUR_E121_04_01_01
  Error: Statement 3 failed: Parse error: ParseError { message: "Expected statement, found Identifier(\"OPEN\")" }

e121_07_01_01: CREATE TABLE TABLE_E121_07_01_01 ( A INT ); DECLARE CUR_E121_07_01_01 CURSOR FOR SELECT A FROM TABLE_E121_07_01_01; DELETE FROM ONLY ( TABLE_E121_07_01_01 ) WHERE CURRENT OF CUR_E121_07_01_01
  Error: Statement 3 failed: Parse error: ParseError { message: "Expected table name after DELETE FROM" }

e121_08_01_01: CREATE TABLE TABLE_E121_08_01_01 ( A INT ); DECLARE CUR_E121_08_01_01 CURSOR FOR SELECT A FROM TABLE_E121_08_01_01; OPEN CUR_E121_08_01_01; CLOSE CUR_E121_08_01_01
  Error: Statement 3 failed: Parse error: ParseError { message: "Expected statement, found Identifier(\"OPEN\")" }

e121_10_01_01: CREATE TABLE TABLE_E121_10_01_011 ( A INT ); CREATE TABLE TABLE_E121_10_01_012 ( A INT ); DECLARE CUR_E121_10_01_01 CURSOR FOR SELECT A FROM TABLE_E121_10_01_011; OPEN CUR_E121_10_01_01; FETCH ABSOLUTE 2 FROM CUR_E121_10_01_01 INTO TABLE_E121_10_01_012
  Error: Statement 4 failed: Parse error: ParseError { message: "Expected statement, found Identifier(\"OPEN\")" }

e121_10_01_02: CREATE TABLE TABLE_E121_10_01_021 ( A INT ); CREATE TABLE TABLE_E121_10_01_022 ( A INT ); DECLARE CUR_E121_10_01_02 CURSOR FOR SELECT A FROM TABLE_E121_10_01_021; OPEN CUR_E121_10_01_02; FETCH CUR_E121_10_01_02 INTO TABLE_E121_10_01_022
  Error: Statement 4 failed: Parse error: ParseError { message: "Expected statement, found Identifier(\"OPEN\")" }

e121_10_01_03: CREATE TABLE TABLE_E121_10_01_031 ( A INT ); CREATE TABLE TABLE_E121_10_01_032 ( A INT ); DECLARE CUR_E121_10_01_03 CURSOR FOR SELECT A FROM TABLE_E121_10_01_031; OPEN CUR_E121_10_01_03; FETCH FIRST FROM CUR_E121_10_01_03 INTO TABLE_E121_10_01_032
  Error: Statement 4 failed: Parse error: ParseError { message: "Expected statement, found Identifier(\"OPEN\")" }

e121_10_01_04: CREATE TABLE TABLE_E121_10_01_041 ( A INT ); CREATE TABLE TABLE_E121_10_01_042 ( A INT ); DECLARE CUR_E121_10_01_04 CURSOR FOR SELECT A FROM TABLE_E121_10_01_041; OPEN CUR_E121_10_01_04; FETCH FROM CUR_E121_10_01_04 INTO TABLE_E121_10_01_042
  Error: Statement 4 failed: Parse error: ParseError { message: "Expected statement, found Identifier(\"OPEN\")" }

e121_10_01_05: CREATE TABLE TABLE_E121_10_01_051 ( A INT ); CREATE TABLE TABLE_E121_10_01_052 ( A INT ); DECLARE CUR_E121_10_01_05 CURSOR FOR SELECT A FROM TABLE_E121_10_01_051; OPEN CUR_E121_10_01_05; FETCH LAST FROM CUR_E121_10_01_05 INTO TABLE_E121_10_01_052
  Error: Statement 4 failed: Parse error: ParseError { message: "Expected statement, found Identifier(\"OPEN\")" }

e121_10_01_06: CREATE TABLE TABLE_E121_10_01_061 ( A INT ); CREATE TABLE TABLE_E121_10_01_062 ( A INT ); DECLARE CUR_E121_10_01_06 CURSOR FOR SELECT A FROM TABLE_E121_10_01_061; OPEN CUR_E121_10_01_06; FETCH NEXT FROM CUR_E121_10_01_06 INTO TABLE_E121_10_01_062
  Error: Statement 4 failed: Parse error: ParseError { message: "Expected statement, found Identifier(\"OPEN\")" }

e121_10_01_07: CREATE TABLE TABLE_E121_10_01_071 ( A INT ); CREATE TABLE TABLE_E121_10_01_072 ( A INT ); DECLARE CUR_E121_10_01_07 CURSOR FOR SELECT A FROM TABLE_E121_10_01_071; OPEN CUR_E121_10_01_07; FETCH PRIOR FROM CUR_E121_10_01_07 INTO TABLE_E121_10_01_072
  Error: Statement 4 failed: Parse error: ParseError { message: "Expected statement, found Identifier(\"OPEN\")" }

e121_10_01_08: CREATE TABLE TABLE_E121_10_01_081 ( A INT ); CREATE TABLE TABLE_E121_10_01_082 ( A INT ); DECLARE CUR_E121_10_01_08 CURSOR FOR SELECT A FROM TABLE_E121_10_01_081; OPEN CUR_E121_10_01_08; FETCH RELATIVE 2 FROM CUR_E121_10_01_08 INTO TABLE_E121_10_01_082
  Error: Statement 4 failed: Parse error: ParseError { message: "Expected statement, found Identifier(\"OPEN\")" }

f031_03_07_01: CREATE COLLATION COLLATION1 FROM 'de_DE'; CREATE ROLE ROLE_F031_03_07_01; GRANT ALL PRIVILEGES ON COLLATION COLLATION1 TO ROLE_F031_03_07_01
  Error: Statement 1 failed: Parse error: ParseError { message: "Expected identifier, found String(\"de_DE\")" }

f031_03_10_01: CREATE TYPE TYPE1; CREATE ROLE ROLE_F031_03_10_01; GRANT ALL PRIVILEGES ON TYPE TYPE1 TO ROLE_F031_03_10_01
  Error: Statement 1 failed: Parse error: ParseError { message: "Expected keyword As, found Eof" }

f031_03_12_13: CREATE ROLE ROLE_F031_03_12_13; GRANT ALL PRIVILEGES ON SPECIFIC CONSTRUCTOR METHOD FOO TO ROLE_F031_03_12_13
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected FUNCTION, PROCEDURE, or ROUTINE after SPECIFIC, found Keyword(Constructor)" }

f031_03_12_15: CREATE ROLE ROLE_F031_03_12_15; GRANT ALL PRIVILEGES ON SPECIFIC INSTANCE METHOD FOO TO ROLE_F031_03_12_15
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected FUNCTION, PROCEDURE, or ROUTINE after SPECIFIC, found Keyword(Instance)" }

f031_03_12_16: CREATE ROLE ROLE_F031_03_12_16; GRANT ALL PRIVILEGES ON SPECIFIC METHOD FOO TO ROLE_F031_03_12_16
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected FUNCTION, PROCEDURE, or ROUTINE after SPECIFIC, found Keyword(Method)" }

f031_03_12_19: CREATE ROLE ROLE_F031_03_12_19; GRANT ALL PRIVILEGES ON SPECIFIC STATIC METHOD FOO TO ROLE_F031_03_12_19
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected FUNCTION, PROCEDURE, or ROUTINE after SPECIFIC, found Keyword(Static)" }

f031_19_05_01: CREATE COLLATION COLLATION1 FROM 'de_DE'; CREATE ROLE ROLE_F031_19_05_01; REVOKE ALL PRIVILEGES ON COLLATION COLLATION1 FROM ROLE_F031_19_05_01
  Error: Statement 1 failed: Parse error: ParseError { message: "Expected identifier, found String(\"de_DE\")" }

f031_19_08_01: CREATE TYPE TYPE1; CREATE ROLE ROLE_F031_19_08_01; REVOKE ALL PRIVILEGES ON TYPE TYPE1 FROM ROLE_F031_19_08_01
  Error: Statement 1 failed: Parse error: ParseError { message: "Expected keyword As, found Eof" }

f031_19_10_03: CREATE ROLE ROLE_F031_19_10_03; REVOKE ALL PRIVILEGES ON FUNCTION BAR FOR BAZ FROM ROLE_F031_19_10_03
  Error: Statement 2 failed: Execution error: Other("Function 'BAR' not found")

f031_19_10_04: CREATE ROLE ROLE_F031_19_10_04; REVOKE ALL PRIVILEGES ON FUNCTION BAR FROM ROLE_F031_19_10_04
  Error: Statement 2 failed: Execution error: Other("Function 'BAR' not found")

f031_19_10_09: CREATE ROLE ROLE_F031_19_10_09; REVOKE ALL PRIVILEGES ON PROCEDURE BAR FOR BAZ FROM ROLE_F031_19_10_09
  Error: Statement 2 failed: Execution error: Other("Procedure 'BAR' not found")

f031_19_10_10: CREATE ROLE ROLE_F031_19_10_10; REVOKE ALL PRIVILEGES ON PROCEDURE BAR FROM ROLE_F031_19_10_10
  Error: Statement 2 failed: Execution error: Other("Procedure 'BAR' not found")

f031_19_10_11: CREATE ROLE ROLE_F031_19_10_11; REVOKE ALL PRIVILEGES ON ROUTINE BAR FOR BAZ FROM ROLE_F031_19_10_11
  Error: Statement 2 failed: Execution error: Other("Routine 'BAR' not found")

f031_19_10_12: CREATE ROLE ROLE_F031_19_10_12; REVOKE ALL PRIVILEGES ON ROUTINE BAR FROM ROLE_F031_19_10_12
  Error: Statement 2 failed: Execution error: Other("Routine 'BAR' not found")

f031_19_10_13: CREATE ROLE ROLE_F031_19_10_13; REVOKE ALL PRIVILEGES ON SPECIFIC CONSTRUCTOR METHOD FOO FROM ROLE_F031_19_10_13
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected FUNCTION, PROCEDURE, or ROUTINE after SPECIFIC, found Keyword(Constructor)" }

f031_19_10_14: CREATE ROLE ROLE_F031_19_10_14; REVOKE ALL PRIVILEGES ON SPECIFIC FUNCTION FOO FROM ROLE_F031_19_10_14
  Error: Statement 2 failed: Execution error: Other("Function 'FOO' not found")

f031_19_10_15: CREATE ROLE ROLE_F031_19_10_15; REVOKE ALL PRIVILEGES ON SPECIFIC INSTANCE METHOD FOO FROM ROLE_F031_19_10_15
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected FUNCTION, PROCEDURE, or ROUTINE after SPECIFIC, found Keyword(Instance)" }

f031_19_10_16: CREATE ROLE ROLE_F031_19_10_16; REVOKE ALL PRIVILEGES ON SPECIFIC METHOD FOO FROM ROLE_F031_19_10_16
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected FUNCTION, PROCEDURE, or ROUTINE after SPECIFIC, found Keyword(Method)" }

f031_19_10_17: CREATE ROLE ROLE_F031_19_10_17; REVOKE ALL PRIVILEGES ON SPECIFIC PROCEDURE FOO FROM ROLE_F031_19_10_17
  Error: Statement 2 failed: Execution error: Other("Procedure 'FOO' not found")

f031_19_10_18: CREATE ROLE ROLE_F031_19_10_18; REVOKE ALL PRIVILEGES ON SPECIFIC ROUTINE FOO FROM ROLE_F031_19_10_18
  Error: Statement 2 failed: Execution error: Other("Routine 'FOO' not found")

f031_19_10_19: CREATE ROLE ROLE_F031_19_10_19; REVOKE ALL PRIVILEGES ON SPECIFIC STATIC METHOD FOO FROM ROLE_F031_19_10_19
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected FUNCTION, PROCEDURE, or ROUTINE after SPECIFIC, found Keyword(Static)" }

```

</details>

## Running Tests Locally

```bash
# Run all conformance tests
cargo test --test sqltest_conformance -- --nocapture

# Generate coverage report
cargo coverage
# Open coverage report
open target/llvm-cov/html/index.html
```

