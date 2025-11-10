# Feature Status

This document provides a detailed breakdown of implemented features in VibeSQL as of November 2025.

## Query Engine

- **Full SQL Support**: SELECT, INSERT, UPDATE, DELETE with all standard clauses
- **All JOIN types**: INNER, LEFT, RIGHT, FULL OUTER, CROSS
- **Advanced features**: Subqueries (scalar, table, correlated), CTEs, window functions
- **Set operations**: UNION, INTERSECT, EXCEPT (with ALL variants)
- **Aggregates**: COUNT, SUM, AVG, MIN, MAX with GROUP BY/HAVING
- **Window functions**: ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, aggregate OVER()

## DDL & Transactions

- **Schema operations**: CREATE/DROP TABLE, CREATE/DROP SCHEMA, ALTER TABLE
- **Transactions**: BEGIN, COMMIT, ROLLBACK, SAVEPOINT (nested transactions)
- **Constraints**: NOT NULL, PRIMARY KEY, UNIQUE, CHECK, FOREIGN KEY (all fully enforced)

## Security & Privileges

- **Role management**: CREATE/DROP ROLE
- **Access control**: GRANT/REVOKE with full privilege enforcement
- **Supported privileges**: SELECT, INSERT, UPDATE, DELETE on tables and schemas

## Built-in Functions (75+)

- **String**: UPPER, LOWER, SUBSTRING, TRIM, CHAR_LENGTH, POSITION, etc.
- **Date/Time**: CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, EXTRACT, date arithmetic
- **Math**: ABS, CEILING, FLOOR, SQRT, POWER, trigonometric, logarithmic functions
- **Conditional**: CASE, COALESCE, NULLIF, GREATEST, LEAST
- **Type conversion**: CAST

## Type System

- **Numeric**: INTEGER, SMALLINT, BIGINT, FLOAT, REAL, DOUBLE PRECISION
- **String**: VARCHAR(n), CHAR(n)
- **Temporal**: DATE, TIME, TIMESTAMP (string-based)
- **Other**: BOOLEAN, NUMERIC(p,s), DECIMAL(p,s)
- **Three-valued logic**: Proper NULL propagation

## Operators & Predicates

- **Comparison**: =, <>, <, >, <=, >=
- **Logical**: AND, OR, NOT
- **Special**: BETWEEN, IN, LIKE, EXISTS, IS NULL/IS NOT NULL
- **Quantified**: ALL, ANY, SOME (with subqueries)
- **Arithmetic**: +, -, *, /, %
- **String**: || (concatenation)

## CLI & Tools

- **Interactive REPL**: Full-featured SQL shell with readline and history
- **Execution modes**: Interactive, command (-c), file (-f), stdin
- **Meta-commands**: PostgreSQL-compatible \d, \dt, \ds, \di, \du
- **Import/Export**: \copy command for CSV and JSON
- **Output formats**: Table, JSON, CSV, Markdown, HTML
- **Configuration**: ~/.vibesqlrc with TOML format
- **Persistence**: \save command for SQL dumps

## Additional Features

- **WASM compilation**: Runs in browser with live demo
- **Python bindings**: DB-API 2.0 compatible interface
- **Test coverage**: 2,000+ tests, 86% code coverage
- **Code quality**: Zero warnings, strict TDD methodology

## See Also

- [SQL:1999 Conformance Report](https://rjwalters.github.io/vibesql/conformance.html) - Detailed conformance test results
- [Roadmap](../README.md#-roadmap) - Future development plans
- [CLI Guide](CLI_GUIDE.md) - Complete CLI documentation
