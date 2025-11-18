# vibesql-ast

Abstract Syntax Tree (AST) definitions for SQL:1999.

## Overview

This crate defines the structure of SQL statements and expressions as parsed from SQL text. The AST provides a tree representation that preserves the semantic structure of SQL queries.

## Features

- **DDL Statements**: CREATE, ALTER, DROP for tables, views, indexes, sequences, etc.
- **DML Statements**: INSERT, UPDATE, DELETE with full expression support
- **Query Expressions**: SELECT with joins, subqueries, CTEs, window functions
- **Expression Trees**: Binary/unary operators, functions, CASE expressions
- **Access Control**: GRANT and REVOKE statements
- **Transaction Control**: BEGIN, COMMIT, ROLLBACK, SAVEPOINT
- **Introspection**: SHOW and DESCRIBE statements

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
vibesql-ast = "0.1"
```

Basic example:

```rust
use vibesql_ast::{SelectStmt, Expression};

// AST nodes represent SQL structures
let expr = Expression::BinaryOp {
    left: Box::new(Expression::Identifier("age".to_string())),
    op: BinaryOperator::GreaterThan,
    right: Box::new(Expression::Literal(SqlValue::Integer(18))),
};
```

## Documentation

- [API Documentation](https://docs.rs/vibesql-ast)
- [Main VibeSQL Repository](https://github.com/rjwalters/vibesql)

## License

This project is licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE))
- MIT License ([LICENSE-MIT](../../LICENSE-MIT))

at your option.
