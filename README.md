# NIST-Compatible In-Memory SQL Database

## Project Goal
Build a **FULL** NIST-compatible SQL:1999 parser and in-memory database implementation from scratch.

## Requirements (from upstream clarifications)
- **SQL Standard**: SQL:1999
- **Compliance Level**: FULL compliance (all mandatory and optional features)
- **Protocol Support**: NIST compatibility tests must run through both ODBC and JDBC
- **Language**: No preference (implementation choice is ours)
- **Test Suite**: Must pass NIST compatibility tests via GitHub Actions

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

## Development Status
**Phase 0: Planning and Architecture** - see PROBLEM_STATEMENT.md for original specification.

Next steps: Language selection, architecture design, and SQL:1999 specification deep-dive.
