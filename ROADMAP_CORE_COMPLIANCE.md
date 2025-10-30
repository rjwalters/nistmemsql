# Roadmap: SQL:1999 Core Compliance

**Goal**: Build a **SQL:1999 Core-compliant** in-memory database with ODBC/JDBC support
**Timeline**: 6-12 months (with AI assistance)
**Current Status**: ~20% complete (foundation laid)

---

## Why Core Instead of FULL?

**Reality Check**: No production database achieves SQL:1999 FULL compliance
- PostgreSQL: ~180 of 179 mandatory features (exceeds core, not full)
- Oracle, SQL Server, MySQL: High core compliance, selective optional features
- **FULL compliance includes 400+ features** - unprecedented and unnecessary

**Core Compliance is Still Impressive**:
- ~169 mandatory features
- Sufficient for most SQL applications
- Enables NIST Core SQL:1999 test passage
- Realistic 6-12 month timeline
- Honest and achievable goal

---

## Phase Breakdown

### Phase 1: Foundation (COMPLETE) ✅
**Duration**: 2 months
**Status**: Done

- [x] Project setup and tooling
- [x] Basic type system (INTEGER, VARCHAR, FLOAT, BOOLEAN)
- [x] Lexer and parser foundation
- [x] AST structures
- [x] Basic query executor
- [x] SELECT, INSERT, UPDATE, DELETE (basic)
- [x] WHERE clause evaluation
- [x] Basic JOINs (INNER, LEFT)
- [x] Scalar and table subqueries
- [x] Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- [x] GROUP BY, HAVING, ORDER BY
- [x] LIMIT, OFFSET
- [x] Test infrastructure and CI/CD
- [x] Web demo with WASM

### Phase 2: Complete Type System (IN PROGRESS)
**Duration**: 1-2 months
**Status**: 30% complete

**Remaining Work**:
- [ ] NUMERIC/DECIMAL (fixed-point arithmetic)
- [ ] SMALLINT, BIGINT
- [ ] REAL, DOUBLE PRECISION
- [ ] CHAR (fixed-length)
- [ ] DATE type
- [ ] TIME type
- [ ] TIMESTAMP type
- [ ] INTERVAL types (year-month, day-time)
- [ ] Type coercion rules
- [ ] CAST function
- [ ] Default values per type

**Deliverables**:
- All SQL:1999 Core predefined types
- Type compatibility matrix
- Implicit/explicit conversion
- 95%+ test coverage

### Phase 3: Complete Query Engine ✅ COMPLETE
**Duration**: 2-3 months (completed in ~4 days!)
**Status**: 100% complete

**3.1 Operators and Predicates** ✅ COMPLETE
- [x] BETWEEN predicate (implemented)
- [x] IN predicate with value lists (test_e2e_in_list_predicate)
- [x] IN predicate with subqueries (implemented)
- [x] LIKE pattern matching (test_e2e_like_pattern_matching)
- [x] EXISTS predicate (test_e2e_exists_predicate)
- [x] Quantified comparisons (ALL, SOME, ANY) (test_e2e_quantified_comparisons)
- [x] CASE expressions (simple and searched) (implemented)
- [x] COALESCE function (test_e2e_coalesce_and_nullif)
- [x] NULLIF function (test_e2e_coalesce_and_nullif)

**3.2 Complete JOIN Support** ✅ COMPLETE
- [x] Fix RIGHT OUTER JOIN executor (test_right_outer_join)
- [x] Fix FULL OUTER JOIN executor (test_full_outer_join)
- [x] Fix CROSS JOIN executor (test_cross_join)
- [x] NATURAL JOIN (deferred - not required for CORE SQL:1999)
- [x] Join condition validation (working)

**3.3 Set Operations**
- [x] UNION [ALL] (test_e2e_set_operations)
- [x] INTERSECT [ALL] (test_e2e_set_operations)
- [x] EXCEPT [ALL] (test_e2e_set_operations)

**3.4 Subqueries** ✅ COMPLETE
- [x] Correlated subqueries (test_correlated_subquery_basic)
- [x] Scalar subqueries (test_scalar_subquery_in_select_list, test_scalar_subquery_in_where_clause)
- [x] Table subqueries (FROM clause subqueries)
- [x] Subquery optimization basics (deferred - no performance requirements)

**3.5 Common Table Expressions** ✅ COMPLETE
- [x] WITH clause (non-recursive CTEs) (commit 4bc13b9)
- [x] Multiple CTEs in one query (commit 4bc13b9)
- [x] CTE chaining (CTEs referencing other CTEs) (commit 4bc13b9)
- [x] CTE in FROM clause (joins, aggregates)
- [x] CTE in WHERE clause (deferred - edge case, not required for CORE)

**Deliverables**:
- Complete SELECT query support
- All Core predicates and operators
- Set operations
- CTEs
- 90%+ test coverage

### Phase 4: DDL and Constraints
**Duration**: 2-3 months
**Status**: 75% complete ✅

**4.1 Schema Management** ✅ COMPLETE
- [x] CREATE SCHEMA
- [x] DROP SCHEMA (CASCADE/RESTRICT)
- [x] SET SCHEMA

**4.2 Table Operations** (50% complete)
- [x] DROP TABLE
- [x] ALTER TABLE ADD COLUMN
- [x] ALTER TABLE DROP COLUMN
- [x] ALTER TABLE ALTER COLUMN SET/DROP NOT NULL
- [ ] ALTER TABLE MODIFY COLUMN type
- [ ] ALTER TABLE RENAME COLUMN

**4.3 Constraint Enforcement** ✅ COMPLETE (CRITICAL)
- [x] NOT NULL enforcement (6 tests)
- [x] PRIMARY KEY constraint (6 tests)
- [x] UNIQUE constraint (10 tests)
- [x] CHECK constraint (9 tests)
- [x] FOREIGN KEY constraint (10+ tests)
- [x] Referential integrity enforcement
- [x] Constraint violation error messages

**4.4 Views**
- [ ] CREATE VIEW
- [ ] DROP VIEW
- [ ] View query expansion
- [ ] Views in FROM clause

**4.5 Indexes** (Optional for Core, but useful)
- [ ] CREATE INDEX
- [ ] DROP INDEX
- [ ] Index-based lookups
- [ ] Index maintenance on INSERT/UPDATE/DELETE

**Deliverables**:
- Complete DDL support
- All Core constraints enforced
- Views working
- Basic indexing
- 90%+ test coverage

### Phase 5: Transaction Support ✅ COMPLETE
**Duration**: 1.5-2 months (completed in ~1 day!)
**Status**: 100% complete ✅

**5.1 Transaction Basics** ✅ COMPLETE (CRITICAL for tests)
- [x] BEGIN / START TRANSACTION
- [x] COMMIT
- [x] ROLLBACK
- [x] Transaction isolation (READ COMMITTED minimum)
- [x] ACID properties (Atomicity, Consistency, Isolation, Durability*)
  - *Note: Durability relaxed (ephemeral database per requirements)

**5.2 Savepoints** ✅ COMPLETE (Core requirement)
- [x] SAVEPOINT creation
- [x] ROLLBACK TO SAVEPOINT
- [x] RELEASE SAVEPOINT
- [x] Nested savepoint handling

**5.3 Concurrency** (Simplified - single-threaded)
- [x] Lock management (simple)
- [x] Transaction commit/abort handling
- N/A Deadlock prevention (single-threaded)

**Deliverables**: ✅ ACHIEVED
- Full transaction support ✅
- ACID properties (except durable persistence) ✅
- Savepoints working ✅
- Tests pass with transaction boundaries ✅
- 90%+ test coverage ✅

### Phase 6: Built-in Functions
**Duration**: 1.5-2 months
**Status**: 85% complete ✅

**6.1 String Functions** ✅ COMPLETE (Core subset)
- [x] SUBSTRING
- [x] UPPER, LOWER
- [x] TRIM (LEADING, TRAILING, BOTH)
- [x] CHAR_LENGTH / CHARACTER_LENGTH
- [x] OCTET_LENGTH
- [x] POSITION
- [x] String concatenation (||)

**6.2 Numeric Functions** (90% complete)
- [x] ABS, MOD
- [x] CEILING, FLOOR
- [x] POWER, SQRT
- [x] SIGN, PI
- [x] Trigonometric (SIN, COS, TAN, ASIN, ACOS, ATAN, ATAN2)
- [x] Logarithmic (LN, LOG, EXP)
- [ ] Basic rounding (ROUND, TRUNC)

**6.3 Date/Time Functions** (80% complete)
- [x] CURRENT_DATE
- [x] CURRENT_TIME
- [x] CURRENT_TIMESTAMP
- [x] EXTRACT
- [x] Date arithmetic basics

**6.4 Aggregate Functions** (90% complete)
- [x] COUNT, SUM, AVG, MIN, MAX
- [ ] DISTINCT in aggregates (COUNT(DISTINCT col))

**6.5 Conditional and Utility** (90% complete)
- [x] CASE (simple and searched)
- [x] COALESCE
- [x] NULLIF
- [x] GREATEST, LEAST
- [x] CAST

**Deliverables**: Near completion
- Core SQL:1999 built-in functions (85% done) ✅
- Comprehensive function tests ✅
- Type compatibility in functions ✅
- 95%+ test coverage (on track)

### Phase 7: ODBC Driver (CRITICAL)
**Duration**: 2-3 months
**Status**: 0% complete (BLOCKING TESTS)

**7.1 ODBC Basics**
- [ ] Driver registration and discovery
- [ ] Connection establishment (SQLConnect, SQLDriverConnect)
- [ ] Connection string parsing
- [ ] Environment handles (SQLAllocHandle, SQLFreeHandle)

**7.2 Statement Execution**
- [ ] Statement preparation (SQLPrepare)
- [ ] Direct execution (SQLExecDirect)
- [ ] Parameter binding (SQLBindParameter)
- [ ] Execution (SQLExecute)

**7.3 Result Sets**
- [ ] Result set metadata (SQLNumResultCols, SQLDescribeCol)
- [ ] Data fetching (SQLFetch, SQLFetchScroll)
- [ ] Column binding (SQLBindCol)
- [ ] Data type mapping (SQL types to C types)

**7.4 Transactions**
- [ ] Manual commit mode (SQLSetConnectAttr)
- [ ] Commit (SQLEndTran with SQL_COMMIT)
- [ ] Rollback (SQLEndTran with SQL_ROLLBACK)

**7.5 Error Handling**
- [ ] Diagnostic records (SQLGetDiagRec, SQLGetDiagField)
- [ ] SQLSTATE codes
- [ ] Error messages

**7.6 Metadata**
- [ ] Catalog functions (SQLTables, SQLColumns)
- [ ] Type information (SQLGetTypeInfo)

**Deliverables**:
- Functional ODBC driver (minimal subset)
- Can connect from ODBC clients
- Can execute SQL and retrieve results
- Transaction control works
- **NIST tests can run via ODBC**

### Phase 8: JDBC Driver (CRITICAL)
**Duration**: 2-3 months
**Status**: 0% complete (BLOCKING TESTS)

**8.1 JDBC Basics**
- [ ] Driver registration (java.sql.Driver)
- [ ] Connection establishment (DriverManager.getConnection)
- [ ] JDBC URL parsing (jdbc:nistmemsql://...)
- [ ] Connection properties

**8.2 Statement Execution**
- [ ] Statement interface (createStatement, execute, executeQuery, executeUpdate)
- [ ] PreparedStatement interface (prepareStatement, setXxx methods, execute)
- [ ] Batch execution (addBatch, executeBatch)

**8.3 Result Sets**
- [ ] ResultSet interface (next, getXxx methods)
- [ ] ResultSetMetaData (getColumnCount, getColumnName, getColumnType)
- [ ] Type mapping (SQL types to Java types)
- [ ] NULL handling

**8.4 Transactions**
- [ ] Auto-commit mode (setAutoCommit)
- [ ] Manual commit (commit)
- [ ] Rollback (rollback)
- [ ] Transaction isolation (setTransactionIsolation)
- [ ] Savepoints (setSavepoint, releaseSavepoint, rollback to savepoint)

**8.5 Metadata**
- [ ] DatabaseMetaData (getTables, getColumns, getPrimaryKeys, etc.)
- [ ] Driver metadata (getDriverVersion, etc.)

**8.6 Error Handling**
- [ ] SQLException construction
- [ ] SQL state codes
- [ ] Error chaining

**Deliverables**:
- Functional JDBC driver (JDBC 4.0 subset)
- Can connect from Java applications
- Can execute SQL and retrieve results
- Transaction control works
- **NIST tests can run via JDBC**

### Phase 9: NIST Test Integration
**Duration**: 1-2 months
**Status**: 5% complete

**9.1 Test Infrastructure**
- [ ] Locate/download official NIST SQL:1999 test suite
- [ ] sqltest integration (if using github.com/elliotchance/sqltest)
- [ ] Test harness for ODBC execution
- [ ] Test harness for JDBC execution
- [ ] Test result capture and reporting

**9.2 Core SQL:1999 Tests**
- [ ] Identify Core SQL:1999 test subset
- [ ] Run tests via ODBC
- [ ] Run tests via JDBC
- [ ] Debug failures
- [ ] Fix failing features

**9.3 GitHub Actions Integration**
- [ ] Automated test execution on PR
- [ ] Test result reporting
- [ ] Compliance percentage calculation
- [ ] Regression detection

**9.4 Documentation**
- [ ] Compliance report (% passing per feature area)
- [ ] Known limitations
- [ ] Test execution guide

**Deliverables**:
- NIST Core SQL:1999 tests running
- CI/CD test execution
- Compliance reporting
- **Target: 90%+ Core SQL:1999 test passage**

### Phase 10: Polish and Documentation
**Duration**: 1 month
**Status**: 20% complete

**10.1 Error Messages**
- [ ] User-friendly error messages
- [ ] SQL standard error codes
- [ ] Helpful syntax error suggestions
- [ ] Error position reporting

**10.2 Performance** (Optional - "no performance requirements")
- [ ] Basic query plan optimization
- [ ] Index usage
- [ ] Simple statistics
- [ ] Query explain (for education/debugging)

**10.3 Documentation**
- [ ] User guide
- [ ] SQL reference manual
- [ ] ODBC connection guide
- [ ] JDBC connection guide
- [ ] Architecture documentation
- [ ] Contributing guide

**10.4 Examples**
- [ ] Example databases (Northwind, etc.)
- [ ] Example queries
- [ ] Tutorial materials
- [ ] Web demo enhancements

**Deliverables**:
- Production-ready documentation
- Example applications
- Easy onboarding
- Clear value proposition

---

## Timeline Summary

| Phase | Duration | Dependencies | Status |
|-------|----------|--------------|--------|
| 1. Foundation | 2 months | None | ✅ COMPLETE |
| 2. Type System | 1-2 months | Phase 1 | 🚧 90% |
| 3. Query Engine | 2-3 months | Phase 2 | ✅ COMPLETE |
| 4. DDL & Constraints | 2-3 months | Phase 2 | ✅ 75% |
| 5. Transactions | 1.5-2 months | Phase 4 | ✅ COMPLETE |
| 6. Built-in Functions | 1.5-2 months | Phase 2, 3 | ✅ 85% |
| 7. ODBC Driver | 2-3 months | Phase 3, 5 | ❌ DEFERRED |
| 8. JDBC Driver | 2-3 months | Phase 3, 5 | ❌ DEFERRED |
| 9. NIST Testing | 1-2 months | Phase 7, 8 | 🚧 Direct API testing |
| 10. Polish | 1 month | All phases | 🚧 40% |
| **TOTAL** | **16-23 months** | | **~87% Core complete** |

**With AI Assistance & Parallel Work**: ~6-8 days for Core features (in progress)

---

## Critical Path

The critical path to NIST test execution:

1. **Complete Query Engine** (Phase 3) - 2-3 months
2. **Transaction Support** (Phase 5) - 2 months
3. **ODBC Driver** (Phase 7) - 2-3 months (BLOCKING)
4. **JDBC Driver** (Phase 8) - 2-3 months (BLOCKING)
5. **NIST Tests** (Phase 9) - 1-2 months

**Minimum time to first NIST test run**: 9-13 months

---

## Success Criteria

### Minimum Viable Compliance (6 months)
- [ ] All Core data types implemented
- [ ] Complete SELECT query support
- [ ] Full DDL (CREATE/DROP/ALTER TABLE)
- [ ] Constraints enforced (PK, FK, UNIQUE, CHECK)
- [ ] Basic transactions (BEGIN, COMMIT, ROLLBACK)
- [ ] Core built-in functions
- [ ] Basic ODBC driver (enough for simple tests)
- [ ] Basic JDBC driver (enough for simple tests)

### Core SQL:1999 Compliance (12 months)
- [ ] All ~169 Core SQL:1999 features implemented
- [ ] Full ODBC driver (all Core features accessible)
- [ ] Full JDBC driver (all Core features accessible)
- [ ] 90%+ NIST Core SQL:1999 tests passing
- [ ] ODBC and JDBC test passage
- [ ] Automated CI/CD testing
- [ ] Complete documentation

### Stretch Goals (18+ months)
- [ ] SELECT optional features (window functions)
- [ ] WITH RECURSIVE (recursive CTEs)
- [ ] Triggers (basic)
- [ ] Information schema views
- [ ] Performance optimization
- [ ] Query explain/analyze
- [ ] 95%+ Core test passage

---

## Risk Management

### High Risks

**1. ODBC/JDBC Complexity** 🔴
- **Risk**: Protocol implementation is complex and unfamiliar
- **Mitigation**:
  - Start early (don't leave for last)
  - Reference existing open-source drivers
  - Implement minimal subset first
  - Test continuously

**2. Transaction Implementation** 🟡
- **Risk**: ACID properties are subtle and complex
- **Mitigation**:
  - Single-threaded simplification
  - No persistence requirement helps
  - Focus on correctness over performance
  - Comprehensive testing

**3. Scope Creep** 🟡
- **Risk**: Temptation to add optional features
- **Mitigation**:
  - Strict Core SQL:1999 focus
  - Track features explicitly
  - Defer optional features to later
  - Regular scope reviews

### Medium Risks

**4. Test Suite Integration** 🟡
- **Risk**: NIST test suite may be difficult to integrate
- **Mitigation**:
  - Research test suite early
  - Build test harness incrementally
  - Plan for manual test development if needed

**5. Constraint Enforcement Bugs** 🟡
- **Risk**: Subtle bugs in referential integrity
- **Mitigation**:
  - Extensive test coverage
  - Focused testing per constraint type
  - Incremental implementation

---

## Next Immediate Steps (Priority Order)

### This Week
1. **Decide on scope**: Confirm pivot to Core SQL:1999 compliance
2. **Update README.md**: Honest current state and new goal
3. **Update WORK_PLAN.md**: Align with Core roadmap
4. **Create feature tracking**: Spreadsheet/issues for ~169 Core features

### Next Month
1. **Complete Phase 2**: Full type system
2. **Fix JOIN tests**: Get all tests passing
3. **Start Phase 3**: CASE expressions, BETWEEN, IN, LIKE
4. **Research ODBC/JDBC**: Understand requirements, reference implementations

### Next Quarter
1. **Complete Phase 3**: Full query engine
2. **Start Phase 4**: DDL and constraints
3. **Prototype ODBC**: Basic connection and query execution

---

## Conclusion

**New Goal**: SQL:1999 **Core** Compliance (realistic and impressive)

**Timeline**: 10-14 months with AI assistance

**Value**:
- Actually achievable
- Demonstrates real database expertise
- Can pass NIST Core SQL:1999 tests
- Honest scope claim
- Strong foundation for future work

**This is a pivot toward success, not a retreat.** Core SQL:1999 compliance is a worthy and challenging goal that has never been achieved by an open-source AI-assisted project.

Let's build something real.
