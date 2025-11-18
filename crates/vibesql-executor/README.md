# vibesql-executor

SQL query execution engine for VibeSQL database.

## Overview

This crate provides the query execution engine that processes SQL statements against the storage layer. It includes query optimization, expression evaluation, and execution of DDL, DML, and query operations.

## Features

- **Query Execution**: SELECT with joins, aggregates, window functions, CTEs
- **DDL Operations**: CREATE, ALTER, DROP for all database objects
- **DML Operations**: INSERT, UPDATE, DELETE with constraint validation
- **Expression Evaluation**: Full SQL expression support with type coercion
- **Query Optimization**: Join reordering, predicate pushdown, subquery optimization
- **Query Plan Cache**: LRU cache for compiled query plans
- **Common Subexpression Elimination**: Automatic CSE for repeated expressions
- **Transaction Support**: BEGIN, COMMIT, ROLLBACK, SAVEPOINT
- **Constraint Validation**: PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK
- **Triggers**: BEFORE/AFTER INSERT/UPDATE/DELETE triggers
- **Access Control**: GRANT/REVOKE privilege checking
- **Advanced Objects**: Functions, procedures, sequences, domains

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
vibesql-executor = "0.1"
```

Basic example:

```rust
use vibesql_executor::QueryExecutor;
use vibesql_storage::Database;
use vibesql_catalog::Catalog;

// Create database and executor
let catalog = Catalog::new();
let mut db = Database::new(catalog, Default::default());
let mut executor = QueryExecutor::new(&mut db);

// Execute a query
let result = executor.execute("SELECT * FROM users WHERE age > 18")?;

// Process results
for row in result.rows {
    println!("{:?}", row);
}
```

## Documentation

- [API Documentation](https://docs.rs/vibesql-executor)
- [Main VibeSQL Repository](https://github.com/rjwalters/vibesql)

## License

This project is licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE))
- MIT License ([LICENSE-MIT](../../LICENSE-MIT))

at your option.
