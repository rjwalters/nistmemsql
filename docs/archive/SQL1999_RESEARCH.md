# SQL:1999 Standard Research

## Overview

This document summarizes research on the SQL:1999 standard (ISO/IEC 9075:1999) to inform implementation planning.

## Standard Documentation

### Official Standard
- **Full Title**: ISO/IEC 9075:1999 - Database Language SQL
- **Parts**:
  - Part 1: Framework (SQL/Framework)
  - Part 2: Foundation (SQL/Foundation) - Contains Core SQL definition
  - Part 3: Call-Level Interface (SQL/CLI)
  - Part 4: Persistent Stored Modules (SQL/PSM)
  - Part 5: Host Language Bindings (SQL/Bindings)

### Purchasing
- Official standard must be purchased from ISO or national standards bodies
- Cost: Typically $100-200+ per part

### Free Resources

#### SQL-99 Complete, Really
- **URL**: Available at crate.io
- **Coverage**: Core SQL:1999 ONLY (not full standard)
- **Limitation**: Despite the title claiming "Complete, Really", it only covers Core SQL:1999
- **Note**: Still valuable for understanding the mandatory baseline

#### BNF Grammar
Two browsable versions of the complete SQL:1999 BNF grammar are freely available:

1. **Ron Savage's Repository**
   - URL: https://ronsavage.github.io/SQL/sql-99.bnf.html
   - Based on Appendix G from "SQL:1999 Understanding Relational Language Components" by Melton & Simon
   - Includes corrections from ISO 9075:1999/Cor.1:2000

2. **Jake Wheat's Grammar**
   - URL: https://jakewheat.github.io/sql-overview/sql-1999-grammar.html

#### Validator
- **Mimer SQL Validator**: https://developer.mimer.com/
- Can validate SQL:1999 syntax compliance

## SQL:1999 Core vs Full

### Core SQL:1999
Starting with SQL:1999, the standard abandoned the SQL-92 three-level approach (Entry, Intermediate, Full) in favor of:
- **Core SQL**: Mandatory features (baseline conformance)
- **Optional Features**: Organized by feature packages with feature IDs

### Feature Taxonomy
Features are organized with alphanumeric IDs:
- **B-series** (B011-B017): Embedded language support
- **E-series** (E011-E182): Essential SQL operations
- **F-series** (F031-F812): Extended functionality
- **S-series** (S011-S043): Distinct and structured types
- **T-series** (T031-T812): Advanced features (triggers, recursive queries, etc.)

### Example Features

#### Known SQL:1999 Core Features:
- **E011**: Numeric data types (INTEGER, SMALLINT, NUMERIC, etc.)
- **F031**: Basic schema manipulation operations (CREATE/DROP TABLE)
- **F041**: Basic joined tables (INNER JOIN, LEFT OUTER JOIN, etc.)
- **F051**: Basic date and time (DATE, TIME, TIMESTAMP)
- **F121**: Basic diagnostics management
- **F181**: Multiple module support
- **F201**: CAST function
- **F311**: Schema definition statement (CREATE SCHEMA)
- **F471**: Scalar subquery values
- **F812**: Basic flagging (standard compliance checking)

#### SQL:1999-Specific Features:
- **T031**: BOOLEAN data type (new in SQL:1999)
- **T121**: WITH (common table expressions, non-recursive)
- **T122**: WITH in subqueries
- **T131**: Recursive queries (WITH RECURSIVE)

**Note**: A comprehensive, authoritative list of all 169+ features requires access to the official standard or reverse-engineering from database documentation.

## SQL:1999 Grammar Summary

Based on the freely available BNF grammar, SQL:1999 includes:

### Statement Types

#### Data Definition Language (DDL)
- CREATE: SCHEMA, TABLE, VIEW, DOMAIN, TRIGGER, PROCEDURE, FUNCTION, ASSERTION, CHARACTER SET, COLLATION, TRANSLATION, TYPE, ORDERING
- ALTER: TABLE, DOMAIN
- DROP: All CREATE-able objects
- GRANT, REVOKE for privileges

#### Data Manipulation Language (DML)
- SELECT with complex query expressions
- INSERT (single row, multi-row, from query)
- UPDATE (searched and positioned via cursor)
- DELETE (searched and positioned via cursor)

#### Transaction Control
- START TRANSACTION / BEGIN
- COMMIT WORK
- ROLLBACK WORK
- SAVEPOINT (new in SQL:1999)
- RELEASE SAVEPOINT
- SET TRANSACTION (isolation levels, read-only, etc.)

#### Session Management
- CONNECT, DISCONNECT
- SET CATALOG, SET SCHEMA, SET NAMES
- SET SESSION CHARACTERISTICS
- SET CONSTRAINTS MODE

### Data Types

#### Predefined Types
- **Numeric**: INTEGER, SMALLINT, BIGINT, NUMERIC, DECIMAL, FLOAT, REAL, DOUBLE PRECISION
- **Character**: CHARACTER, CHAR, VARCHAR, CHARACTER LARGE OBJECT (CLOB)
- **National Character**: NCHAR, NVARCHAR, NCLOB
- **Binary**: BINARY LARGE OBJECT (BLOB)
- **Boolean**: BOOLEAN (new in SQL:1999)
- **Datetime**: DATE, TIME, TIMESTAMP
- **Interval**: INTERVAL (year-month and day-time variants)

#### Constructed Types (SQL:1999 additions)
- **ARRAY**: Collection type for ordered elements
- **ROW**: Structured type for composite values
- **REF**: Reference to user-defined type instances
- **User-Defined Types (UDT)**: DISTINCT types and structured types

### Query Features

#### FROM Clause
- Table references
- JOIN operations: CROSS, INNER, LEFT [OUTER], RIGHT [OUTER], FULL [OUTER], NATURAL, UNION
- Derived tables (subqueries in FROM)
- Common Table Expressions: WITH and WITH RECURSIVE (new in SQL:1999)

#### WHERE Clause Predicates
- Comparison: =, <>, <, <=, >, >=
- BETWEEN, IN, LIKE, SIMILAR TO (new in SQL:1999)
- NULL tests: IS NULL, IS NOT NULL
- Quantified comparisons: ALL, SOME, ANY
- EXISTS
- UNIQUE
- MATCH (for foreign key checking)
- OVERLAPS (for temporal overlap)
- Type predicates: IS OF TYPE (for UDT hierarchies)

#### SELECT List
- Column references
- Expressions and computed values
- Aggregate functions: COUNT, SUM, AVG, MIN, MAX
- Window functions (added in SQL:1999 amendment, 2000)
- CASE expressions
- CAST for type conversion
- Subqueries (scalar, row, table)

#### Grouping and Ordering
- GROUP BY (simple and complex grouping expressions)
- HAVING for group filtering
- ORDER BY with ASC/DESC, NULLS FIRST/LAST

#### Set Operations
- UNION [ALL]
- INTERSECT [ALL]
- EXCEPT [ALL] (or MINUS in some dialects)

### Advanced Features

#### Triggers (New in SQL:1999)
- CREATE TRIGGER
- BEFORE/AFTER timing
- INSERT/UPDATE/DELETE events
- Row-level and statement-level
- FOR EACH ROW
- OLD/NEW row references
- WHEN condition
- Triggered SQL statement

#### Procedures and Functions (SQL/PSM - Part 4)
- CREATE PROCEDURE
- CREATE FUNCTION
- BEGIN...END blocks
- DECLARE for variables
- Control flow: IF, CASE, LOOP, WHILE, REPEAT, FOR
- CALL for procedure invocation
- RETURN for functions

#### Security
- CREATE/DROP ROLE
- GRANT/REVOKE privileges
- WITH GRANT OPTION
- Role hierarchies
- Schema-level privileges

#### Constraints
- NOT NULL
- PRIMARY KEY
- FOREIGN KEY with MATCH FULL/PARTIAL/SIMPLE
- UNIQUE
- CHECK
- ASSERTION (cross-table constraints)
- Constraint naming
- DEFERRABLE/NOT DEFERRABLE
- INITIALLY DEFERRED/IMMEDIATE

#### Cursors
- DECLARE CURSOR
- OPEN, FETCH, CLOSE cursor
- Positioned UPDATE/DELETE (WHERE CURRENT OF cursor)
- SCROLL cursors (forward/backward navigation)
- Cursor sensitivity (SENSITIVE, INSENSITIVE, ASENSITIVE)
- WITH HOLD (preserve cursor across commits)

#### Information Schema
- Standard system views for metadata
- TABLES, COLUMNS, VIEWS, CONSTRAINTS, etc.
- Defined in Part 2 of the standard

## Major SQL:1999 Innovations

### 1. Recursive Queries
Common Table Expressions (CTEs) with recursion:
```sql
WITH RECURSIVE employee_hierarchy AS (
  SELECT emp_id, manager_id, emp_name
  FROM employees
  WHERE manager_id IS NULL
  UNION ALL
  SELECT e.emp_id, e.manager_id, e.emp_name
  FROM employees e
  INNER JOIN employee_hierarchy eh ON e.manager_id = eh.emp_id
)
SELECT * FROM employee_hierarchy;
```

### 2. Boolean Type
Native BOOLEAN data type with TRUE, FALSE, UNKNOWN values.

### 3. User-Defined Types
```sql
CREATE TYPE address_type AS (
  street VARCHAR(100),
  city VARCHAR(50),
  state CHAR(2),
  zip VARCHAR(10)
);

CREATE TYPE person_type AS (
  name VARCHAR(100),
  age INTEGER,
  address address_type
) NOT FINAL;
```

### 4. Large Objects
BLOB and CLOB types for binary and character large objects with special handling.

### 5. Triggers
Event-driven procedural code execution on INSERT, UPDATE, DELETE.

### 6. Arrays
```sql
CREATE TABLE schedule (
  week_id INTEGER,
  daily_hours INTEGER ARRAY[7]
);
```

### 7. SIMILAR TO Pattern Matching
Regular expression-like pattern matching (simpler than full regex):
```sql
SELECT * FROM products
WHERE product_code SIMILAR TO 'A[0-9]{3}-%';
```

### 8. SAVEPOINT
Nested transaction control within a transaction:
```sql
BEGIN;
  INSERT INTO orders VALUES (1, 'Widget');
  SAVEPOINT order_inserted;
  INSERT INTO order_items VALUES (1, 1, 'Blue');
  ROLLBACK TO order_inserted;
COMMIT;
```

## Implementation Complexity Assessment

### Core SQL:1999 (Mandatory)
- **Estimated Complexity**: Very High
- **Components**: ~169 core features
- **Major Work Areas**:
  - Complete SQL parser for full grammar
  - Type system including UDTs
  - Query execution engine with optimization
  - Transaction management (ACID)
  - Constraint enforcement
  - Catalog/metadata system

### Full SQL:1999 (Core + All Optional)
- **Estimated Complexity**: Extreme
- **Additional Components**:
  - Triggers and stored procedures (SQL/PSM)
  - Recursive query processing
  - Array operations
  - User-defined types and methods
  - Full cursor support with scrollability
  - Information schema views
  - Complete security model
  - Embedded SQL for multiple host languages
  - Call-Level Interface (CLI/ODBC-like)

### Comparison to Production Databases
- **PostgreSQL 16**: Claims ~180 of 179 mandatory SQL:2016 core features (exceeds baseline)
- **Oracle 12c**: High compliance but not 100% of any standard version
- **SQL Server 2019**: High compliance with many proprietary extensions
- **MySQL 8.0**: Improving compliance but gaps remain

**Reality Check**: No major commercial database claims FULL compliance with all optional features of any SQL standard version. The requirement for FULL SQL:1999 compliance is unprecedented.

## References

1. SQL:1999 BNF Grammar: https://ronsavage.github.io/SQL/sql-99.bnf.html
2. Modern SQL - SQL:1999: https://modern-sql.com/standard
3. PostgreSQL SQL Conformance: https://www.postgresql.org/docs/current/features.html
4. Oracle Standard SQL Documentation: https://docs.oracle.com/cd/B10500_01/server.920/a96540/ap_standard_sql.htm
5. "SQL:1999 Understanding Relational Language Components" by Melton & Simon
6. "SQL-99 Complete, Really" - Available at crate.io (Core SQL:1999 only)

## Next Steps for Implementation

1. **Acquire Standard**: Purchase official ISO/IEC 9075:1999 documentation (all parts)
2. **Study Core Features**: Deep dive into "SQL-99 Complete, Really" for baseline
3. **Grammar Implementation**: Use BNF grammar to build parser
4. **Feature Tracking**: Create comprehensive feature checklist from standard
5. **Incremental Development**: Implement features iteratively with continuous testing
6. **Test Against Mimer Validator**: Use free validator for syntax checking
7. **Study Production Implementations**: Review PostgreSQL, Derby, and other open-source SQL engines for reference
