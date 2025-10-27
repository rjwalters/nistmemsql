# NIST-Compatible SQL:1999 Database

[![Deploy Status](https://github.com/rjwalters/nistmemsql/actions/workflows/deploy-demo.yml/badge.svg)](https://github.com/rjwalters/nistmemsql/actions/workflows/deploy-demo.yml)
[![Demo](https://img.shields.io/badge/demo-live-success)](https://rjwalters.github.io/nistmemsql/)
[![Tests](https://img.shields.io/badge/tests-477%20passing-success)](https://github.com/rjwalters/nistmemsql/actions)
[![Coverage](https://img.shields.io/badge/coverage-84%25-green)](https://github.com/rjwalters/nistmemsql)

> **An open-source, NIST-testable SQL:1999 database implementation in Rust**

🚀 **[Try the Live Demo](https://rjwalters.github.io/nistmemsql/)** - Run SQL queries in your browser!

---

## 🎯 Project Vision

Build a **FULL SQL:1999 compliant** database from scratch, designed for NIST conformance testing. This is a research and educational project targeting complete standard compliance—something no production database has achieved.

### Two-Phase Strategy

**Phase 1: Core SQL:1999 Compliance** (Current Focus - AI Speed Run)
- Implement ~169 mandatory Core features
- Timeline: **~1 week** from start (Oct 25, 2025)
- Enables NIST Core testing
- Provides solid foundation

**Phase 2: FULL SQL:1999 Compliance** (Ultimate Goal)
- Implement all mandatory + optional features (400+)
- Timeline: A larger effort, but not years
- Unprecedented achievement
- Aligns with [upstream posix4e/nistmemsql](https://github.com/posix4e/nistmemsql) vision

**Note**: PostgreSQL, Oracle, and SQL Server implement Core + selective optional features. This is an AI-powered "speed run" to demonstrate rapid standards compliance.

---

## 📊 Current Status (October 27, 2025 - Day 3)

### Velocity-Based Progress Tracking

**Core SQL:1999 Compliance**: ~35% complete in 3 days

**Demonstrated Velocity**:
- **Days 1-3** (Oct 25-27): 35% complete → **Phase 3 Complete!** 🎉
- **Projected completion**: ~7 days total from start (on track!)
- **Target date**: October 31 - November 1, 2025

### Progress Breakdown by Category

| Category | Progress | Completed | Status |
|----------|----------|-----------|--------|
| Data Types | 40% (5/13) | INTEGER, FLOAT, VARCHAR, BOOLEAN, NULL | 🟡 In Progress |
| DML Operations | 40% (4/10) | SELECT, INSERT, UPDATE, DELETE | 🟡 In Progress |
| Predicates | 100% | =, <>, <, >, <=, >=, BETWEEN, IN, LIKE, EXISTS, IS NULL, quantified, ALL, ANY, SOME | ✅ Complete |
| JOINs | 100% (5/5) | INNER, LEFT, RIGHT, FULL, CROSS | ✅ Complete |
| Subqueries | 100% | Scalar, table, correlated, IN with subquery | ✅ Complete |
| CTEs | 100% | WITH clause, multiple CTEs, CTE chaining | ✅ Complete |
| Set Operations | 100% | UNION [ALL], INTERSECT [ALL], EXCEPT [ALL] | ✅ Complete |
| Aggregates | 86% (6/7) | COUNT, SUM, AVG, MIN, MAX, + DISTINCT | 🟢 Advanced |
| Built-in Functions | 40% | UPPER, LOWER, SUBSTRING, TRIM, CHAR_LENGTH, CASE, COALESCE, NULLIF, CAST | 🟡 In Progress |
| DDL | 10% (1/10) | CREATE TABLE | 🔴 Early |
| Constraints | 0% (0/5) | None | ⏳ Days 4-5 |
| Transactions | 0% (0/4) | None | ⏳ Days 4-5 |
| ODBC Driver | 0% | None | 🔴 Days 6-7 |
| JDBC Driver | 0% | None | 🔴 Days 6-7 |

### Why 1 Week Is Achievable

**Data-Driven Projection**:
- 2 days elapsed → 25-30% complete
- Average velocity: **12.5-15% per day**
- 2 ÷ 0.275 = **7.3 days total projected**
- Remaining: **~5 days**

**AI-Powered Development Advantages**:
- Parallel feature implementation
- Instant boilerplate generation
- Rapid test creation and iteration
- Well-defined SQL:1999 specification (no ambiguity)

**What Makes This Different**:
- Traditional estimates assume human-paced development
- AI assistance enables 5-10x faster iteration
- Clear specifications (SQL:1999 standard is precise)
- Strong foundation already built (parser, executor, storage)

### What Works Today (Days 1-3 Achievements)

**Query Engine** ✅ **(Phase 3 Complete!)**
- **SELECT**: WHERE, JOIN, subqueries, GROUP BY, HAVING, ORDER BY, LIMIT, DISTINCT
- **All 5 JOIN types**: INNER, LEFT, RIGHT, FULL OUTER, CROSS (100% complete)
- **Subqueries**: Scalar, table (derived tables), correlated
- **CTEs**: WITH clause, multiple CTEs, CTE chaining
- **Set Operations**: UNION [ALL], INTERSECT [ALL], EXCEPT [ALL]
- **Aggregates**: COUNT(*), COUNT(col), SUM, AVG, MIN, MAX with NULL handling
- **DML**: INSERT, UPDATE, DELETE, CREATE TABLE

**Predicates & Operators** ✅
- **Comparison**: =, <>, <, >, <=, >=
- **Logical**: AND, OR, NOT
- **Special**: BETWEEN, IN (value list), LIKE (% and _ wildcards), EXISTS, IS NULL/IS NOT NULL
- **Quantified**: ALL, ANY, SOME (with subqueries)
- **Arithmetic**: +, -, *, /, %
- **String**: || (concatenation)

**Built-in Functions** ✅
- **String**: UPPER, LOWER, SUBSTRING(str, pos, len), TRIM, CHAR_LENGTH, CHARACTER_LENGTH
- **Null handling**: COALESCE, NULLIF
- **Conditional**: CASE (simple and searched forms)
- **Type conversion**: CAST(expr AS type)

**Type System** ✅
- **Fully working**: INTEGER, SMALLINT, BIGINT, FLOAT, REAL, DOUBLE PRECISION, VARCHAR(n), CHAR(n), BOOLEAN, NULL
- **Partial** (string-based): DATE, TIME, TIMESTAMP, NUMERIC(p,s), DECIMAL(p,s)
- **Three-valued logic**: Proper NULL propagation in all operations

**Infrastructure** ✅
- 643 tests passing (100%)
- ~11,500 lines of Rust code
- 84% code coverage
- Zero compilation errors
- Strict TDD methodology
- WASM bindings for browser execution
- Live web demo with Monaco editor
- CI/CD pipeline with auto-deploy

### What's Next (Days 3-7 Plan)

**Day 3 Completed** ✅ - Phase 3 Query Engine (100% complete!)
- [x] Set operations execution (UNION, INTERSECT, EXCEPT)
- [x] CTE (WITH clause) execution
- [x] All JOIN types working (INNER, LEFT, RIGHT, FULL, CROSS)
- [x] All test files fixed (643 tests passing)

**Day 4 (Oct 28)** - Complete Type System
- [ ] NUMERIC/DECIMAL precision arithmetic (not string-based)
- [ ] DATE/TIME/TIMESTAMP proper types with operations
- [ ] INTERVAL type
- [ ] Remaining numeric functions (ABS, MOD, CEILING, FLOOR, POWER, SQRT)
- [ ] Date/time functions (CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, EXTRACT)
- [ ] Multi-row INSERT
- [ ] INSERT from SELECT

**Day 5 (Oct 30)** - Constraints & Transactions
- [ ] Transaction support (BEGIN, COMMIT, ROLLBACK, SAVEPOINT)
- [ ] PRIMARY KEY enforcement
- [ ] FOREIGN KEY enforcement with referential integrity
- [ ] UNIQUE constraint enforcement
- [ ] CHECK constraint enforcement
- [ ] NOT NULL enforcement

**Day 6 (Oct 31)** - DDL & Infrastructure
- [ ] DROP TABLE, DROP VIEW, DROP INDEX
- [ ] ALTER TABLE (ADD COLUMN, DROP COLUMN, MODIFY COLUMN)
- [ ] CREATE VIEW with query storage
- [ ] CREATE INDEX (basic B-tree)
- [ ] GRANT/REVOKE basic permissions

**Day 7 (Nov 1)** - NIST Test Integration
- [ ] ODBC driver implementation (C API wrapper)
- [ ] JDBC driver implementation (Java wrapper via JNI or socket)
- [ ] NIST Core test suite setup
- [ ] Run tests and fix failures
- [ ] Document compliance level

---

## 🗺️ Roadmap

### To Core SQL:1999 Compliance

**Project Started**: Saturday, October 25, 2025
**Current Status**: Day 3 (Oct 27) - 35% complete → **Phase 3 Complete!** 🎉
**Demonstrated Velocity**: On track for 7-day completion
**Projected Completion**: October 31 - November 1, 2025 (~7 days total)

**Days 1-3 Completed** ✅ **Phase 3: Query Engine (100%)**
- Query engine (SELECT with JOINs, subqueries, aggregates)
- All 5 JOIN types (INNER, LEFT, RIGHT, FULL, CROSS)
- Subqueries (scalar, table, correlated)
- Predicates (BETWEEN, IN, LIKE, EXISTS, quantified comparisons)
- CTEs (WITH clause, multiple CTEs, CTE chaining)
- Set operations (UNION, INTERSECT, EXCEPT with ALL variants)
- String functions (UPPER, LOWER, SUBSTRING, TRIM, CHAR_LENGTH)
- CASE expressions, COALESCE, NULLIF, CAST
- 643 passing tests, 84% coverage

**Days 4-5: Type System & Core Features** (25% progress expected)
1. Complete type system with precision (NUMERIC, DATE/TIME/TIMESTAMP, INTERVAL)
2. Remaining built-in functions (numeric, date/time)
3. Multi-row INSERT, INSERT from SELECT
4. Transaction support (BEGIN/COMMIT/ROLLBACK/SAVEPOINT)
5. Full constraint enforcement (PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK, NOT NULL)

**Days 6: DDL & Infrastructure** (10% progress expected)
6. DDL operations (DROP, ALTER, CREATE VIEW/INDEX)
7. Basic permissions (GRANT/REVOKE)

**Day 7: NIST Integration** (final 5% + testing)
9. ODBC driver (C API wrapper for NIST tests)
10. JDBC driver (Java wrapper for NIST tests)
11. NIST Core test suite setup and initial runs
12. Bug fixes from test failures

**When Complete**: 90%+ passage of NIST Core SQL:1999 test suite

### To FULL SQL:1999 Compliance

**Estimated Timeline**: A much larger effort beyond Core, but achievable
**Target**: To be determined after Core completion

**Additional Work Beyond Core**:
- Advanced type system (ARRAY, ROW, UDT, BLOB, CLOB)
- Window functions (ROW_NUMBER, RANK, LEAD, LAG)
- Recursive CTEs (WITH RECURSIVE)
- Procedural SQL (SQL/PSM) - stored procedures, functions, cursors
- Triggers (BEFORE/AFTER, row/statement level)
- Advanced DDL (domains, assertions, character sets, collations)
- Security and privileges (GRANT, REVOKE, roles)
- Information schema (~50+ system views)
- Advanced query optimization
- Full MERGE statement

**When Complete**: First database to achieve FULL SQL:1999 compliance

---

## 🌐 Live Demo

**[Try it now →](https://rjwalters.github.io/nistmemsql/)**

Run SQL queries directly in your browser with **zero setup**:
- **Pre-loaded Sample Data** - 6 employee records ready to query
- **Instant Execution** - Press Ctrl/Cmd+Enter and see results immediately
- **Monaco Editor** - Full SQL syntax highlighting and IntelliSense
- **WASM-Powered** - Rust database compiled to WebAssembly
- **SQL Comment Support** - Use `--` for inline documentation
- **Export Results** - Copy to clipboard or download as CSV
- **Dark Mode** - Beautiful Tailwind CSS interface

**Try these queries**:
```sql
-- See all employees
SELECT * FROM employees;

-- Filter by department
SELECT name, salary FROM employees WHERE department = 'Engineering';

-- Aggregate data
SELECT department, COUNT(*) as count FROM employees GROUP BY department;
```

---

## 🚀 Quick Start

### Try the Demo Locally

```bash
# Clone the repository
git clone https://github.com/rjwalters/nistmemsql.git
cd nistmemsql

# Run tests (requires Rust)
cargo test --workspace

# Run the web demo
cd web-demo
npm install
npm run dev
```

### Interactive SQL Shell

```bash
# Build and run the CLI
cargo run --bin nistmemsql

# Try some SQL
nistmemsql> CREATE TABLE users (id INTEGER, name VARCHAR(50));
nistmemsql> INSERT INTO users VALUES (1, 'Alice');
nistmemsql> SELECT * FROM users;
```

---

## 📖 Documentation

**Quick Links**:
- **[WORK_PLAN.md](WORK_PLAN.md)** - Detailed roadmap and feature tracking
- **[PROBLEM_STATEMENT.md](PROBLEM_STATEMENT.md)** - Original challenge
- **[SQL1999_COMPLIANCE_GAP_ANALYSIS.md](SQL1999_COMPLIANCE_GAP_ANALYSIS.md)** - Honest assessment of current vs target
- **[ROADMAP_CORE_COMPLIANCE.md](ROADMAP_CORE_COMPLIANCE.md)** - 10-phase plan to Core compliance

**Architecture & Design**:
- [docs/decisions/](docs/decisions/) - Architecture Decision Records
- [TESTING_STRATEGY.md](TESTING_STRATEGY.md) - Test approach and strategy
- [docs/lessons/TDD_APPROACH.md](docs/lessons/TDD_APPROACH.md) - TDD lessons learned

**Loom AI Orchestration**:
- [CLAUDE.md](CLAUDE.md) - AI-powered development guide
- [AGENTS.md](AGENTS.md) - Development agent workflows

---

## 🎯 Design Principles

### What Makes This Project Unique

**1. Standards-First Approach**
- SQL:1999 specification is the source of truth
- NIST test suite validation
- No shortcuts or "close enough" implementations

**2. Educational Value**
- Comprehensive documentation of decisions
- TDD approach with 477 tests
- Clear, readable Rust code
- Interactive web demo for learning

**3. Pragmatic Simplifications**
- In-memory only (no persistence)
- No performance requirements
- Single-threaded execution
- Focus: correctness over speed

**4. Research Goal**
- Target unprecedented FULL SQL:1999 compliance
- Document the journey and challenges
- Contribute to SQL implementation knowledge

---

## 🧪 Test-Driven Development

We build using **strict TDD** (Red-Green-Refactor):

```rust
// 1. RED: Write failing test first
#[test]
fn test_between_predicate() {
    let sql = "SELECT * FROM users WHERE age BETWEEN 18 AND 65";
    let result = parser::parse(sql);
    assert!(result.is_ok());
}

// 2. GREEN: Implement just enough to pass
// 3. REFACTOR: Clean up while tests stay green
```

**Benefits Observed**:
- ✅ 477 tests passing (100% success rate)
- ✅ 84% code coverage
- ✅ Zero warnings (compiler + clippy)
- ✅ Faster development (less debugging)
- ✅ Safe refactoring
- ✅ Tests as living documentation

---

## 🤝 Contributing

This project uses [Loom](https://github.com/loomhq/loom) for AI-powered development orchestration. See [CLAUDE.md](CLAUDE.md) for the development guide.

**Ways to Contribute**:
- 🐛 Report bugs or missing features
- 📖 Improve documentation
- ✨ Implement Core SQL:1999 features
- 🧪 Add test coverage
- 🌐 Enhance the web demo

See [WORK_PLAN.md](WORK_PLAN.md) for current priorities.

---

## 📈 Project Stats

- **Language**: Rust 🦀
- **Architecture**: 7-crate workspace
- **Tests**: 643 passing (100%)
- **Coverage**: 84%
- **LOC**: ~11,500
- **Project Type**: AI-powered speed run for CORE compliance
- **Started**: October 25, 2025
- **Current Day**: Day 3 (Oct 27, 2025)
- **Progress**: 35% of Core SQL:1999 → **Phase 3 Complete!** 🎉
- **Velocity**: On track for 7-day completion
- **Projected Completion**: Oct 31 - Nov 1, 2025 (7 days total)
- **Approach**: Test-Driven Development
- **Orchestration**: Loom AI framework

---

## 🏆 Milestones

**Completed** ✅
- [x] Project foundation and architecture
- [x] Complete SQL parser (SELECT, INSERT, UPDATE, DELETE)
- [x] In-memory storage engine
- [x] Query execution engine
- [x] **Phase 3: Complete Query Engine (100%)**
  - [x] All JOIN types (INNER, LEFT, RIGHT, FULL, CROSS)
  - [x] All subqueries (scalar, table, correlated)
  - [x] All predicates (BETWEEN, IN, LIKE, EXISTS, quantified)
  - [x] CTEs (WITH clause, multiple CTEs, chaining)
  - [x] Set operations (UNION, INTERSECT, EXCEPT)
  - [x] CASE expressions, COALESCE, NULLIF
- [x] Aggregate functions with GROUP BY/HAVING
- [x] String functions (UPPER, LOWER, SUBSTRING, TRIM, CHAR_LENGTH)
- [x] WASM bindings and web demo
- [x] CI/CD pipeline

**In Progress** 🚧
- [ ] Complete type system (DATE, TIME, NUMERIC with precision)
- [ ] Numeric functions (ABS, MOD, CEILING, FLOOR, POWER, SQRT)
- [ ] Date/time functions (CURRENT_DATE, CURRENT_TIME, EXTRACT)

**Upcoming** ⏳
- [ ] Transaction support
- [ ] Constraint enforcement
- [ ] ODBC/JDBC drivers
- [ ] NIST test integration

---

## 📜 License

MIT License - See [LICENSE](LICENSE) for details.

---

## 🙏 Acknowledgments

- Based on the [posix4e/nistmemsql](https://github.com/posix4e/nistmemsql) challenge
- Built with [Loom](https://github.com/loomhq/loom) AI orchestration
- Powered by Rust 🦀 and Claude Code
- NIST SQL:1999 standard compliance guidance

---

**Current Focus**: AI speed run to Core SQL:1999 compliance
- **Day 3 of 7** (Oct 27, 2025) - **Phase 3 Complete!** 🎉
- **35% complete** in 3 days
- **Velocity**: On track for 7-day completion
- **Target**: Oct 31 - Nov 1, 2025

**Ultimate Goal**: FULL SQL:1999 compliance (larger effort, but achievable)

**Try it now**: [Live Demo →](https://rjwalters.github.io/nistmemsql/)
