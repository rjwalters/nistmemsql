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

## 📊 Current Status (October 2025)

### Progress Overview

**Core SQL:1999 Compliance**: ~25-30% complete

| Category | Progress | Status |
|----------|----------|--------|
| Data Types | 38% (5/13) | 🟡 In Progress |
| DML Operations | 40% | 🟡 In Progress |
| Predicates | 35% (9/26) | 🟡 In Progress |
| JOINs | 100% (5/5) | ✅ Complete |
| Subqueries | 80% | 🟢 Advanced |
| Aggregates | 70% | 🟢 Advanced |
| Built-in Functions | 10% | 🔴 Early |
| DDL | 10% | 🔴 Early |
| Constraints | 0% | ⏳ Planned |
| Transactions | 0% | ⏳ Planned |
| ODBC Driver | 0% | 🔴 Required |
| JDBC Driver | 0% | 🔴 Required |

### What Works Today

**Query Engine** ✅
- Full SELECT support (WHERE, JOIN, subqueries, GROUP BY, HAVING, ORDER BY, LIMIT)
- All JOIN types (INNER, LEFT, RIGHT, FULL OUTER, CROSS)
- Scalar, table, and correlated subqueries
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- INSERT, UPDATE, DELETE operations
- Predicates: =, <>, <, >, <=, >=, BETWEEN, IN, IS NULL, AND, OR, NOT

**Type System** ✅
- INTEGER, VARCHAR, FLOAT, BOOLEAN, NULL
- Full three-valued logic support
- Type compatibility and comparisons

**Infrastructure** ✅
- 477 tests passing (100%)
- ~11,000 lines of Rust code
- 84% code coverage
- Zero compiler/clippy warnings
- TDD throughout
- WASM bindings for web demo
- CI/CD pipeline

### What's Next

**Immediate Priorities** (Next 3 months)
- [ ] LIKE pattern matching
- [ ] CASE expressions
- [ ] EXISTS predicate
- [ ] NUMERIC/DECIMAL types
- [ ] DATE, TIME, TIMESTAMP types
- [ ] Set operations (UNION, INTERSECT, EXCEPT)

**Critical Path to NIST Testing** (6-12 months)
- [ ] Transaction support (BEGIN, COMMIT, ROLLBACK)
- [ ] Constraint enforcement (PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK, NOT NULL)
- [ ] ODBC driver (required for tests)
- [ ] JDBC driver (required for tests)
- [ ] Built-in functions (string, numeric, date/time)
- [ ] DDL operations (DROP, ALTER TABLE, CREATE VIEW)

---

## 🗺️ Roadmap

### To Core SQL:1999 Compliance

**Project Started**: Saturday, October 25, 2025
**Estimated Timeline**: ~1 week (AI speed run)
**Target Completion**: Early November 2025

**Remaining Major Work**:
1. Complete type system (DATE, TIME, TIMESTAMP, NUMERIC, INTERVAL, etc.)
2. All Core predicates and operators (LIKE, EXISTS, CASE, COALESCE, etc.)
3. DDL with full constraint enforcement
4. Transaction support (ACID properties)
5. Core built-in functions (~30 functions)
6. ODBC driver implementation
7. JDBC driver implementation
8. NIST Core test integration

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
- **Tests**: 477 passing (100%)
- **Coverage**: 84%
- **LOC**: ~11,000
- **Project Type**: AI-powered speed run for CORE compliance
- **Started**: October 25, 2025
- **Approach**: Test-Driven Development
- **Orchestration**: Loom AI framework

---

## 🏆 Milestones

**Completed** ✅
- [x] Project foundation and architecture
- [x] Complete SQL parser (SELECT, INSERT, UPDATE, DELETE)
- [x] In-memory storage engine
- [x] Query execution engine
- [x] All JOIN types (INNER, LEFT, RIGHT, FULL, CROSS)
- [x] Subqueries (scalar, table, correlated)
- [x] Aggregate functions with GROUP BY/HAVING
- [x] WASM bindings and web demo
- [x] CI/CD pipeline

**In Progress** 🚧
- [ ] Core SQL:1999 predicates (LIKE, EXISTS, CASE)
- [ ] Complete type system (DATE, TIME, NUMERIC)
- [ ] Web demo feature showcase

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

**Current Focus**: AI speed run to Core SQL:1999 compliance (~25-30% complete, target: 1 week)

**Ultimate Goal**: FULL SQL:1999 compliance (larger effort, but achievable)

**Try it now**: [Live Demo →](https://rjwalters.github.io/nistmemsql/)
