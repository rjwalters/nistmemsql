# SQL:1999 Conformance Report

**Generated**: Fri Oct 31 11:21:57 PDT 2025
**Commit**: d979715

## Summary

| Metric | Value |
|--------|-------|
| Total Tests | 739 |
| Passed | 661 ✅ |
| Failed | 0 ❌ |
| Errors | 78 ⚠️ |
| Pass Rate | 89.4% |

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

## Sample Failing Tests

The following tests are currently failing (showing first 10):

- **e051_07_01_01**: CREATE TABLE TABLE_E051_07_01_01 ( A INT, B INT ); SELECT * AS ( C , D ) FROM TABLE_E051_07_01_01
  - Error: Statement 2 failed: Parse error: ParseError { message: "Expected Symbol('('), found LParen" }
- **e051_07_01_03**: CREATE TABLE TABLE_E051_07_01_03 ( A INT, B INT ); SELECT ALL * AS ( C , D ) FROM TABLE_E051_07_01_03
  - Error: Statement 2 failed: Parse error: ParseError { message: "Expected Symbol('('), found LParen" }
- **e051_07_01_05**: CREATE TABLE TABLE_E051_07_01_05 ( A INT, B INT ); SELECT ALL TABLE_E051_07_01_05 . * AS ( C , D ) FROM TABLE_E051_07_01_05
  - Error: Statement 2 failed: Parse error: ParseError { message: "Expected Symbol('('), found LParen" }
- **e051_07_01_07**: CREATE TABLE TABLE_E051_07_01_07 ( A INT, B INT ); SELECT DISTINCT * AS ( C , D ) FROM TABLE_E051_07_01_07
  - Error: Statement 2 failed: Parse error: ParseError { message: "Expected Symbol('('), found LParen" }
- **e051_07_01_09**: CREATE TABLE TABLE_E051_07_01_09 ( A INT, B INT ); SELECT DISTINCT TABLE_E051_07_01_09 . * AS ( C , D ) FROM TABLE_E051_07_01_09
  - Error: Statement 2 failed: Parse error: ParseError { message: "Expected Symbol('('), found LParen" }
- **e051_07_01_11**: CREATE TABLE TABLE_E051_07_01_11 ( A INT, B INT ); SELECT TABLE_E051_07_01_11 . * AS ( C , D ) FROM TABLE_E051_07_01_11
  - Error: Statement 2 failed: Parse error: ParseError { message: "Expected Symbol('('), found LParen" }
- **e051_08_01_01**: CREATE TABLE TABLE_E051_08_01_01 ( A INT, B INT ); SELECT * AS ( C , D ) FROM TABLE_E051_08_01_01 AS MYTEMP
  - Error: Statement 2 failed: Parse error: ParseError { message: "Expected Symbol('('), found LParen" }
- **e051_08_01_03**: CREATE TABLE TABLE_E051_08_01_03 ( A INT, B INT ); SELECT ALL * AS ( C , D ) FROM TABLE_E051_08_01_03 AS MYTEMP
  - Error: Statement 2 failed: Parse error: ParseError { message: "Expected Symbol('('), found LParen" }
- **e051_08_01_05**: CREATE TABLE TABLE_E051_08_01_05 ( A INT, B INT ); SELECT ALL MYTEMP . * AS ( C , D ) FROM TABLE_E051_08_01_05 AS MYTEMP
  - Error: Statement 2 failed: Parse error: ParseError { message: "Expected Symbol('('), found LParen" }
- **e051_08_01_07**: CREATE TABLE TABLE_E051_08_01_07 ( A INT, B INT ); SELECT DISTINCT * AS ( C , D ) FROM TABLE_E051_08_01_07 AS MYTEMP
  - Error: Statement 2 failed: Parse error: ParseError { message: "Expected Symbol('('), found LParen" }

<details>
<summary>View all failing tests (click to expand)</summary>

```
e051_07_01_01: CREATE TABLE TABLE_E051_07_01_01 ( A INT, B INT ); SELECT * AS ( C , D ) FROM TABLE_E051_07_01_01
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected Symbol('('), found LParen" }

e051_07_01_03: CREATE TABLE TABLE_E051_07_01_03 ( A INT, B INT ); SELECT ALL * AS ( C , D ) FROM TABLE_E051_07_01_03
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected Symbol('('), found LParen" }

e051_07_01_05: CREATE TABLE TABLE_E051_07_01_05 ( A INT, B INT ); SELECT ALL TABLE_E051_07_01_05 . * AS ( C , D ) FROM TABLE_E051_07_01_05
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected Symbol('('), found LParen" }

e051_07_01_07: CREATE TABLE TABLE_E051_07_01_07 ( A INT, B INT ); SELECT DISTINCT * AS ( C , D ) FROM TABLE_E051_07_01_07
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected Symbol('('), found LParen" }

e051_07_01_09: CREATE TABLE TABLE_E051_07_01_09 ( A INT, B INT ); SELECT DISTINCT TABLE_E051_07_01_09 . * AS ( C , D ) FROM TABLE_E051_07_01_09
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected Symbol('('), found LParen" }

e051_07_01_11: CREATE TABLE TABLE_E051_07_01_11 ( A INT, B INT ); SELECT TABLE_E051_07_01_11 . * AS ( C , D ) FROM TABLE_E051_07_01_11
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected Symbol('('), found LParen" }

e051_08_01_01: CREATE TABLE TABLE_E051_08_01_01 ( A INT, B INT ); SELECT * AS ( C , D ) FROM TABLE_E051_08_01_01 AS MYTEMP
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected Symbol('('), found LParen" }

e051_08_01_03: CREATE TABLE TABLE_E051_08_01_03 ( A INT, B INT ); SELECT ALL * AS ( C , D ) FROM TABLE_E051_08_01_03 AS MYTEMP
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected Symbol('('), found LParen" }

e051_08_01_05: CREATE TABLE TABLE_E051_08_01_05 ( A INT, B INT ); SELECT ALL MYTEMP . * AS ( C , D ) FROM TABLE_E051_08_01_05 AS MYTEMP
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected Symbol('('), found LParen" }

e051_08_01_07: CREATE TABLE TABLE_E051_08_01_07 ( A INT, B INT ); SELECT DISTINCT * AS ( C , D ) FROM TABLE_E051_08_01_07 AS MYTEMP
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected Symbol('('), found LParen" }

e051_08_01_09: CREATE TABLE TABLE_E051_08_01_09 ( A INT, B INT ); SELECT DISTINCT MYTEMP . * AS ( C , D ) FROM TABLE_E051_08_01_09 AS MYTEMP
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected Symbol('('), found LParen" }

e051_08_01_11: CREATE TABLE TABLE_E051_08_01_11 ( A INT, B INT ); SELECT MYTEMP . * AS ( C , D ) FROM TABLE_E051_08_01_11 AS MYTEMP
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected Symbol('('), found LParen" }

e081_09_01_01: CREATE SCHEMA TABLE_E081_09_01_011; CREATE ROLE ROLE_E081_09_01_01; GRANT USAGE ON TABLE TABLE_E081_09_01_011 TO ROLE_E081_09_01_01
  Error: Statement 3 failed: Execution error: TableNotFound("TABLE_E081_09_01_011")

e081_10_01_01: CREATE SCHEMA TABLE_E081_10_01_011; CREATE ROLE ROLE_E081_10_01_01; GRANT EXECUTE ON TABLE TABLE_E081_10_01_011 TO ROLE_E081_10_01_01
  Error: Statement 3 failed: Execution error: TableNotFound("TABLE_E081_10_01_011")

e121_04_01_01: CREATE TABLE TABLE_E121_04_01_01 ( A INT ); DECLARE CUR_E121_04_01_01 CURSOR FOR SELECT A FROM TABLE_E121_04_01_01; OPEN CUR_E121_04_01_01
  Error: Statement 3 failed: Parse error: ParseError { message: "Expected statement, found Identifier(\"OPEN\")" }

e121_06_01_01: CREATE TABLE TABLE_E121_06_01_01 ( A INT ); DECLARE CUR_E121_06_01_01 CURSOR FOR SELECT A FROM TABLE_E121_06_01_01; UPDATE TABLE_E121_06_01_01 SET A = 1 WHERE CURRENT OF CUR_E121_06_01_01
  Error: Statement 3 failed: Parse error: ParseError { message: "Expected identifier after CURRENT, found Keyword(Of)" }

e121_07_01_01: CREATE TABLE TABLE_E121_07_01_01 ( A INT ); DECLARE CUR_E121_07_01_01 CURSOR FOR SELECT A FROM TABLE_E121_07_01_01; DELETE FROM ONLY ( TABLE_E121_07_01_01 ) WHERE CURRENT OF CUR_E121_07_01_01
  Error: Statement 3 failed: Parse error: ParseError { message: "Expected table name after DELETE FROM" }

e121_07_01_02: CREATE TABLE TABLE_E121_07_01_02 ( A INT ); DECLARE CUR_E121_07_01_02 CURSOR FOR SELECT A FROM TABLE_E121_07_01_02; DELETE FROM TABLE_E121_07_01_02 WHERE CURRENT OF CUR_E121_07_01_02
  Error: Statement 3 failed: Parse error: ParseError { message: "Expected identifier after CURRENT, found Keyword(Of)" }

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

e152_01_01_01: START TRANSACTION; SET LOCAL TRANSACTION ISOLATION LEVEL SERIALIZABLE
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected SCHEMA, CATALOG, NAMES, TIME ZONE, or TRANSACTION after SET" }

e152_02_01_01: START TRANSACTION; SET LOCAL TRANSACTION READ ONLY
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected SCHEMA, CATALOG, NAMES, TIME ZONE, or TRANSACTION after SET" }

e152_02_01_02: START TRANSACTION; SET LOCAL TRANSACTION READ WRITE
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected SCHEMA, CATALOG, NAMES, TIME ZONE, or TRANSACTION after SET" }

e152_02_01_04: START TRANSACTION; SET TRANSACTION READ WRITE
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected ONLY or WRITE after READ" }

f031_03_03_02: CREATE TABLE TABLE_F031_03_03_02 ( A INTEGER ); CREATE ROLE ROLE_F031_03_03_02; GRANT INSERT ( A ) ON TABLE_F031_03_03_02 TO ROLE_F031_03_03_02
  Error: Statement 3 failed: Parse error: ParseError { message: "Expected keyword On, found LParen" }

f031_03_03_06: CREATE TABLE TABLE_F031_03_03_06 ( A INTEGER ); CREATE ROLE ROLE_F031_03_03_06; GRANT SELECT ( A ) ON TABLE_F031_03_03_06 TO ROLE_F031_03_03_06
  Error: Statement 3 failed: Parse error: ParseError { message: "Expected keyword On, found LParen" }

f031_03_06_01: CREATE DOMAIN DOMAIN1 AS INT; CREATE ROLE ROLE_F031_03_06_01; GRANT ALL PRIVILEGES ON DOMAIN DOMAIN1 TO ROLE_F031_03_06_01
  Error: Statement 3 failed: Parse error: ParseError { message: "Expected keyword To, found Identifier(\"DOMAIN1\")" }

f031_03_07_01: CREATE COLLATION COLLATION1 FROM 'de_DE'; CREATE ROLE ROLE_F031_03_07_01; GRANT ALL PRIVILEGES ON COLLATION COLLATION1 TO ROLE_F031_03_07_01
  Error: Statement 1 failed: Parse error: ParseError { message: "Expected identifier, found String(\"de_DE\")" }

f031_03_08_01: CREATE CHARACTER SET CHARACTERSET1; CREATE ROLE ROLE_F031_03_08_01; GRANT ALL PRIVILEGES ON CHARACTER SET CHARACTERSET1 TO ROLE_F031_03_08_01
  Error: Statement 3 failed: Parse error: ParseError { message: "Expected keyword To, found Keyword(Set)" }

f031_03_09_01: CREATE TRANSLATION TRANSLATION1; CREATE ROLE ROLE_F031_03_09_01; GRANT ALL PRIVILEGES ON TRANSLATION TRANSLATION1 TO ROLE_F031_03_09_01
  Error: Statement 3 failed: Parse error: ParseError { message: "Expected keyword To, found Identifier(\"TRANSLATION1\")" }

f031_03_10_01: CREATE TYPE TYPE1; CREATE ROLE ROLE_F031_03_10_01; GRANT ALL PRIVILEGES ON TYPE TYPE1 TO ROLE_F031_03_10_01
  Error: Statement 1 failed: Parse error: ParseError { message: "Expected keyword As, found Eof" }

f031_03_11_01: CREATE SEQUENCE SEQUENCE1; CREATE ROLE ROLE_F031_03_11_01; GRANT ALL PRIVILEGES ON SEQUENCE SEQUENCE1 TO ROLE_F031_03_11_01
  Error: Statement 3 failed: Parse error: ParseError { message: "Expected keyword To, found Identifier(\"SEQUENCE1\")" }

f031_03_12_01: CREATE ROLE ROLE_F031_03_12_01; GRANT ALL PRIVILEGES ON CONSTRUCTOR METHOD BAR FOR BAZ TO ROLE_F031_03_12_01
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword To, found Keyword(For)" }

f031_03_12_03: CREATE ROLE ROLE_F031_03_12_03; GRANT ALL PRIVILEGES ON FUNCTION BAR FOR BAZ TO ROLE_F031_03_12_03
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword To, found Keyword(For)" }

f031_03_12_05: CREATE ROLE ROLE_F031_03_12_05; GRANT ALL PRIVILEGES ON INSTANCE METHOD BAR FOR BAZ TO ROLE_F031_03_12_05
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword To, found Keyword(For)" }

f031_03_12_07: CREATE ROLE ROLE_F031_03_12_07; GRANT ALL PRIVILEGES ON METHOD BAR FOR BAZ TO ROLE_F031_03_12_07
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword To, found Keyword(For)" }

f031_03_12_09: CREATE ROLE ROLE_F031_03_12_09; GRANT ALL PRIVILEGES ON PROCEDURE BAR FOR BAZ TO ROLE_F031_03_12_09
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword To, found Keyword(For)" }

f031_03_12_11: CREATE ROLE ROLE_F031_03_12_11; GRANT ALL PRIVILEGES ON ROUTINE BAR FOR BAZ TO ROLE_F031_03_12_11
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword To, found Keyword(For)" }

f031_03_12_13: CREATE ROLE ROLE_F031_03_12_13; GRANT ALL PRIVILEGES ON SPECIFIC CONSTRUCTOR METHOD FOO TO ROLE_F031_03_12_13
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword To, found Keyword(Constructor)" }

f031_03_12_14: CREATE ROLE ROLE_F031_03_12_14; GRANT ALL PRIVILEGES ON SPECIFIC FUNCTION FOO TO ROLE_F031_03_12_14
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword To, found Keyword(Function)" }

f031_03_12_15: CREATE ROLE ROLE_F031_03_12_15; GRANT ALL PRIVILEGES ON SPECIFIC INSTANCE METHOD FOO TO ROLE_F031_03_12_15
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword To, found Keyword(Instance)" }

f031_03_12_16: CREATE ROLE ROLE_F031_03_12_16; GRANT ALL PRIVILEGES ON SPECIFIC METHOD FOO TO ROLE_F031_03_12_16
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword To, found Keyword(Method)" }

f031_03_12_17: CREATE ROLE ROLE_F031_03_12_17; GRANT ALL PRIVILEGES ON SPECIFIC PROCEDURE FOO TO ROLE_F031_03_12_17
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword To, found Keyword(Procedure)" }

f031_03_12_18: CREATE ROLE ROLE_F031_03_12_18; GRANT ALL PRIVILEGES ON SPECIFIC ROUTINE FOO TO ROLE_F031_03_12_18
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword To, found Keyword(Routine)" }

f031_03_12_19: CREATE ROLE ROLE_F031_03_12_19; GRANT ALL PRIVILEGES ON SPECIFIC STATIC METHOD FOO TO ROLE_F031_03_12_19
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword To, found Keyword(Static)" }

f031_03_12_20: CREATE ROLE ROLE_F031_03_12_20; GRANT ALL PRIVILEGES ON STATIC METHOD BAR FOR BAZ TO ROLE_F031_03_12_20
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword To, found Keyword(For)" }

f031_19_02_03: CREATE TABLE TABLE_F031_19_02_03 ( A INTEGER ); CREATE ROLE ROLE_F031_19_02_03; REVOKE INSERT ( A ) ON TABLE_F031_19_02_03 FROM ROLE_F031_19_02_03
  Error: Statement 3 failed: Parse error: ParseError { message: "Expected keyword On, found LParen" }

f031_19_02_07: CREATE TABLE TABLE_F031_19_02_07 ( A INTEGER ); CREATE ROLE ROLE_F031_19_02_07; REVOKE SELECT ( A ) ON TABLE_F031_19_02_07 FROM ROLE_F031_19_02_07
  Error: Statement 3 failed: Parse error: ParseError { message: "Expected keyword On, found LParen" }

f031_19_04_01: CREATE DOMAIN DOMAIN1 AS INT; CREATE ROLE ROLE_F031_19_04_01; REVOKE ALL PRIVILEGES ON DOMAIN DOMAIN1 FROM ROLE_F031_19_04_01
  Error: Statement 3 failed: Parse error: ParseError { message: "Expected keyword From, found Identifier(\"DOMAIN1\")" }

f031_19_05_01: CREATE COLLATION COLLATION1 FROM 'de_DE'; CREATE ROLE ROLE_F031_19_05_01; REVOKE ALL PRIVILEGES ON COLLATION COLLATION1 FROM ROLE_F031_19_05_01
  Error: Statement 1 failed: Parse error: ParseError { message: "Expected identifier, found String(\"de_DE\")" }

f031_19_06_01: CREATE CHARACTER SET CHARACTERSET1; CREATE ROLE ROLE_F031_19_06_01; REVOKE ALL PRIVILEGES ON CHARACTER SET CHARACTERSET1 FROM ROLE_F031_19_06_01
  Error: Statement 3 failed: Parse error: ParseError { message: "Expected keyword From, found Keyword(Set)" }

f031_19_07_01: CREATE TRANSLATION TRANSLATION1; CREATE ROLE ROLE_F031_19_07_01; REVOKE ALL PRIVILEGES ON TRANSLATION TRANSLATION1 FROM ROLE_F031_19_07_01
  Error: Statement 3 failed: Parse error: ParseError { message: "Expected keyword From, found Identifier(\"TRANSLATION1\")" }

f031_19_08_01: CREATE TYPE TYPE1; CREATE ROLE ROLE_F031_19_08_01; REVOKE ALL PRIVILEGES ON TYPE TYPE1 FROM ROLE_F031_19_08_01
  Error: Statement 1 failed: Parse error: ParseError { message: "Expected keyword As, found Eof" }

f031_19_09_01: CREATE SEQUENCE SEQUENCE1; CREATE ROLE ROLE_F031_19_09_01; REVOKE ALL PRIVILEGES ON SEQUENCE SEQUENCE1 FROM ROLE_F031_19_09_01
  Error: Statement 3 failed: Parse error: ParseError { message: "Expected keyword From, found Identifier(\"SEQUENCE1\")" }

f031_19_10_01: CREATE ROLE ROLE_F031_19_10_01; REVOKE ALL PRIVILEGES ON CONSTRUCTOR METHOD BAR FOR BAZ FROM ROLE_F031_19_10_01
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword From, found Keyword(For)" }

f031_19_10_03: CREATE ROLE ROLE_F031_19_10_03; REVOKE ALL PRIVILEGES ON FUNCTION BAR FOR BAZ FROM ROLE_F031_19_10_03
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword From, found Keyword(For)" }

f031_19_10_04: CREATE ROLE ROLE_F031_19_10_04; REVOKE ALL PRIVILEGES ON FUNCTION BAR FROM ROLE_F031_19_10_04
  Error: Statement 2 failed: Execution error: Other("Function 'BAR' not found")

f031_19_10_05: CREATE ROLE ROLE_F031_19_10_05; REVOKE ALL PRIVILEGES ON INSTANCE METHOD BAR FOR BAZ FROM ROLE_F031_19_10_05
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword From, found Keyword(For)" }

f031_19_10_07: CREATE ROLE ROLE_F031_19_10_07; REVOKE ALL PRIVILEGES ON METHOD BAR FOR BAZ FROM ROLE_F031_19_10_07
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword From, found Keyword(For)" }

f031_19_10_09: CREATE ROLE ROLE_F031_19_10_09; REVOKE ALL PRIVILEGES ON PROCEDURE BAR FOR BAZ FROM ROLE_F031_19_10_09
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword From, found Keyword(For)" }

f031_19_10_10: CREATE ROLE ROLE_F031_19_10_10; REVOKE ALL PRIVILEGES ON PROCEDURE BAR FROM ROLE_F031_19_10_10
  Error: Statement 2 failed: Execution error: Other("Procedure 'BAR' not found")

f031_19_10_11: CREATE ROLE ROLE_F031_19_10_11; REVOKE ALL PRIVILEGES ON ROUTINE BAR FOR BAZ FROM ROLE_F031_19_10_11
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword From, found Keyword(For)" }

f031_19_10_12: CREATE ROLE ROLE_F031_19_10_12; REVOKE ALL PRIVILEGES ON ROUTINE BAR FROM ROLE_F031_19_10_12
  Error: Statement 2 failed: Execution error: Other("Routine 'BAR' not found")

f031_19_10_13: CREATE ROLE ROLE_F031_19_10_13; REVOKE ALL PRIVILEGES ON SPECIFIC CONSTRUCTOR METHOD FOO FROM ROLE_F031_19_10_13
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword From, found Keyword(Constructor)" }

f031_19_10_14: CREATE ROLE ROLE_F031_19_10_14; REVOKE ALL PRIVILEGES ON SPECIFIC FUNCTION FOO FROM ROLE_F031_19_10_14
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword From, found Keyword(Function)" }

f031_19_10_15: CREATE ROLE ROLE_F031_19_10_15; REVOKE ALL PRIVILEGES ON SPECIFIC INSTANCE METHOD FOO FROM ROLE_F031_19_10_15
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword From, found Keyword(Instance)" }

f031_19_10_16: CREATE ROLE ROLE_F031_19_10_16; REVOKE ALL PRIVILEGES ON SPECIFIC METHOD FOO FROM ROLE_F031_19_10_16
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword From, found Keyword(Method)" }

f031_19_10_17: CREATE ROLE ROLE_F031_19_10_17; REVOKE ALL PRIVILEGES ON SPECIFIC PROCEDURE FOO FROM ROLE_F031_19_10_17
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword From, found Keyword(Procedure)" }

f031_19_10_18: CREATE ROLE ROLE_F031_19_10_18; REVOKE ALL PRIVILEGES ON SPECIFIC ROUTINE FOO FROM ROLE_F031_19_10_18
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword From, found Keyword(Routine)" }

f031_19_10_19: CREATE ROLE ROLE_F031_19_10_19; REVOKE ALL PRIVILEGES ON SPECIFIC STATIC METHOD FOO FROM ROLE_F031_19_10_19
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword From, found Keyword(Static)" }

f031_19_10_20: CREATE ROLE ROLE_F031_19_10_20; REVOKE ALL PRIVILEGES ON STATIC METHOD BAR FOR BAZ FROM ROLE_F031_19_10_20
  Error: Statement 2 failed: Parse error: ParseError { message: "Expected keyword From, found Keyword(For)" }

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

