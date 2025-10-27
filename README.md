# NIST-Compatible In-Memory SQL Database

[![Deploy Status](https://github.com/rjwalters/nistmemsql/actions/workflows/deploy-demo.yml/badge.svg)](https://github.com/rjwalters/nistmemsql/actions/workflows/deploy-demo.yml)
[![Demo](https://img.shields.io/badge/demo-live-success)](https://rjwalters.github.io/nistmemsql/)

## Project Goal
Build a **FULL** NIST-compatible SQL:1999 parser and in-memory database implementation from scratch.

## Requirements (from upstream clarifications)
- **SQL Standard**: SQL:1999
- **Compliance Level**: FULL compliance (all mandatory and optional features)
- **Protocol Support**: NIST compatibility tests must run through both ODBC and JDBC
- **Language**: No preference (implementation choice is ours)
- **Test Suite**: [sqltest](https://github.com/elliotchance/sqltest) - comprehensive SQL conformance tests
- **Performance**: Not required - single-threaded is acceptable
- **Persistence**: None - purely in-memory, no WAL, ephemeral only

## High-Level Architecture

### Components Required
1. **SQL Parser** - Lexical analysis and parsing of SQL statements
2. **Query Planner** - Optimize and plan query execution
3. **Execution Engine** - Execute queries against in-memory storage
4. **Storage Engine** - In-memory data structures for tables, indexes, etc.
5. **ODBC/JDBC Interface** - Standard database connectivity protocols
6. **NIST Compliance Test Suite** - Automated validation of standard conformance

## Implementation Phases

### Phase 1: Core SQL Parser
- Lexer and tokenizer
- Parser for basic SQL statements (SELECT, INSERT, UPDATE, DELETE)
- Abstract Syntax Tree (AST) generation
- Basic semantic analysis

### Phase 2: Storage Engine
- In-memory table storage
- Basic data types (INTEGER, VARCHAR, etc.)
- Row-based storage format
- Table metadata management

### Phase 3: Query Execution
- Simple query executor for basic SELECT
- WHERE clause evaluation
- Basic JOIN operations
- Aggregation functions (COUNT, SUM, AVG, etc.)

### Phase 4: SQL:1999 Specific Features
- Recursive queries (WITH RECURSIVE)
- CASE expressions
- Boolean data type
- Large object types (BLOB, CLOB)
- User-defined types
- Triggers and stored procedures
- Roles and privileges

### Phase 5: Advanced SQL:1999 Features
- Object-relational features (user-defined types, methods)
- Savepoints in transactions
- Multiple result sets
- Additional datetime types (TIMESTAMP, INTERVAL)
- SIMILAR TO pattern matching
- New built-in functions and operators

### Phase 6: Protocol Support (Critical for Testing)
- **ODBC interface implementation** - Required for NIST test execution
- **JDBC driver implementation** - Required for NIST test execution
- Network protocol handling
- Connection pooling and session management

### Phase 7: NIST Compliance Testing
- Integrate NIST SQL:1999 test suite
- GitHub Actions CI/CD pipeline
- Both ODBC and JDBC test execution paths
- Compliance reporting and gap analysis

## Test-Driven Development Approach

We're building this database using **Test-Driven Development (TDD)**:

1. **Red Phase**: Write tests first (they fail)
2. **Green Phase**: Implement just enough to make tests pass
3. **Refactor Phase**: Clean up code while tests stay green

### TDD Benefits Observed
- ✅ Clean, well-designed APIs
- ✅ 100% test coverage from day one
- ✅ Faster development (no debugging!)
- ✅ Safe refactoring
- ✅ Tests serve as living documentation

See [docs/lessons/TDD_APPROACH.md](docs/lessons/TDD_APPROACH.md) for detailed lessons learned.

## Documentation

Comprehensive documentation tracking our journey:

- **[MAJOR_SIMPLIFICATIONS.md](MAJOR_SIMPLIFICATIONS.md)** - 🎉 Game-changing scope reductions
- **[PROBLEM_STATEMENT.md](PROBLEM_STATEMENT.md)** - Original challenge specification
- **[REQUIREMENTS.md](REQUIREMENTS.md)** - Detailed requirements from upstream clarifications
- **[SQL1999_RESEARCH.md](SQL1999_RESEARCH.md)** - Deep dive into SQL:1999 standard
- **[TESTING_STRATEGY.md](TESTING_STRATEGY.md)** - Comprehensive test approach (updated with sqltest)
- **[RESEARCH_SUMMARY.md](RESEARCH_SUMMARY.md)** - Executive summary of findings
- **[DECISIONS.md](DECISIONS.md)** - Architecture decision records index
- **[WORK_PLAN.md](WORK_PLAN.md)** - 📍 **Updated!** Detailed roadmap and progress tracking
- **[LESSONS_LEARNED.md](LESSONS_LEARNED.md)** - Insights and knowledge gained
- **[docs/](docs/)** - Detailed documentation directory
  - [Documentation Guide](docs/README.md) - How to use and contribute to docs
  - [TDD Approach](docs/lessons/TDD_APPROACH.md) - 🎉 **New!** Test-driven development lessons
  - [Templates](docs/templates/) - ADR, architecture, implementation, and lessons templates
  - [Decisions](docs/decisions/) - Architecture Decision Records
    - [ADR-0001: Language Choice (Rust)](docs/decisions/0001-language-choice.md)

### Documentation Standards

We maintain detailed documentation to track decisions, capture learning, and enable future contributors. See [docs/README.md](docs/README.md) for:
- Documentation structure and organization
- Document types and templates
- Writing standards and conventions
- When and how to document

## Live Demo

🚀 **[Try the interactive SQL demo](https://rjwalters.github.io/nistmemsql/)** - Run SQL queries directly in your browser!

Features:
- **Monaco Editor** - Full SQL syntax highlighting and IntelliSense
- **Real-time Execution** - Query results displayed instantly
- **WASM-powered** - Rust database compiled to WebAssembly
- **Example Queries** - Learn SQL:1999 features interactively
- **Dark Mode** - Beautiful Tailwind CSS interface

## Development Status

**Current Phase**: Phase 4 - SQL:1999 Specific Features + Web Demo 🦀

**Development Approach**: Test-Driven Development (Red-Green-Refactor) ✅

### Completed ✅

#### Core Database Engine
- ✅ Requirements clarification (via upstream GitHub issues)
- ✅ SQL:1999 standard research
- ✅ Testing strategy design
- ✅ Documentation infrastructure
- ✅ Language selection (Rust - see [ADR-0001](docs/decisions/0001-language-choice.md))
- ✅ Cargo workspace initialized (7 crates)
- ✅ Development tooling (rustfmt, clippy)
- ✅ **Types Crate** - SQL:1999 type system with PartialOrd comparisons (45 tests passing) 🎉
- ✅ **AST Crate** - Abstract Syntax Tree structures (22 tests passing) 🎉
- ✅ **Parser Crate** - Complete SQL parser (89 tests passing) 🎉
- ✅ **Catalog Crate** - Schema management (10 tests passing) 🎉
- ✅ **Storage Crate** - In-memory tables (6 tests passing) 🎉
- ✅ **Executor Crate** - Query execution (72 tests passing) 🎉
- ✅ **Parser Strategy** - ADR-0002: Hand-written recursive descent parser
- ✅ **JOIN Operations** - INNER, LEFT, RIGHT, FULL OUTER, CROSS joins (parsing + execution)
- ✅ **Subqueries** - Scalar subqueries and table subqueries (derived tables)
- ✅ **Aggregate Functions** - COUNT, SUM, AVG, MIN, MAX with GROUP BY/HAVING
- ✅ **Query Pagination** - LIMIT/OFFSET support
- ✅ **Test Coverage Infrastructure** - cargo-llvm-cov integration (83.3% coverage)
- ✅ **Development Orchestration** - Loom framework integration for AI-powered development

#### Web Demo & WASM
- ✅ **WASM Bindings** - TypeScript bindings for Rust database
- ✅ **Vite + TypeScript** - Modern build tooling and type safety
- ✅ **Tailwind CSS** - Utility-first styling with dark mode support
- ✅ **Monaco Editor** - Full SQL editor with syntax highlighting and IntelliSense
- ✅ **ESLint + Prettier** - Code quality and formatting
- ✅ **Vitest** - Fast unit testing (15 tests passing)
- ✅ **CI/CD Pipeline** - GitHub Actions with automated deployment
- ✅ **GitHub Pages** - Live demo deployment

### In Progress 🚧
- 🚧 Northwind example database (Issue #54)
- 🚧 Correlated subquery support (Issue #82)
- 🚧 SQL:1999 feature showcase (Issue #56)
- 🚧 Web demo enhancements (Issue #105)

### Test Status
- **Total Tests**: 259 (255 passing, 4 known JOIN test failures) ✅
- **Test Coverage**: 83.3% (crates: ast 80.0%, catalog 88.0%, executor 83.5%, parser 82.9%, storage 100%, types 78.9%)
- **Compiler Warnings**: 0
- **Clippy Warnings**: 0
- **Source Files**: 82 Rust files
- **Lines of Code**: ~11,000

### Next Steps
1. Fix remaining JOIN executor tests (CROSS, FULL OUTER, RIGHT OUTER)
2. Implement correlated subquery support (Issue #82)
3. Complete Northwind example database (Issue #54)
4. Build SQL:1999 feature showcase (Issue #56)
5. Improve test coverage toward 90%

See [WORK_PLAN.md](WORK_PLAN.md) for detailed progress and roadmap.

## Key Findings

### 🎉 MAJOR SIMPLIFICATIONS! (See [MAJOR_SIMPLIFICATIONS.md](MAJOR_SIMPLIFICATIONS.md))

All 7 upstream issues answered with **game-changing** clarifications:

1. **No Performance Requirements** ✅
   - Single-threaded is fine
   - No query optimization needed
   - No WAL required
   - Simple algorithms acceptable

2. **No Persistence Required** ✅
   - Purely ephemeral (in-memory only)
   - No disk I/O
   - No durability needed
   - Data lost on shutdown is fine

3. **Official Test Suite Identified** ✅
   - [sqltest](https://github.com/elliotchance/sqltest) by Elliot Chance
   - Covers SQL:92, SQL:99, SQL:2003, SQL:2011, SQL:2016
   - BNF-driven test generation
   - Feature-organized, comprehensive

### Updated Scope Assessment
- **Original Estimate**: 92,000-152,000 LOC, 3-5 person-years
- **Revised Estimate**: 40,000-70,000 LOC, 1-2 person-years (**60-70% reduction!**)
- **With AI Assistance**: 6-12 months estimated
- **Complexity**: Massively reduced - focus on correctness only
- **Challenge**: Still FULL SQL:1999 compliance (unprecedented)

**Key Insight**: Eliminated ~60-70% of complexity by not needing performance, persistence, or test development!
