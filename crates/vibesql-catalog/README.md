# vibesql-catalog

Schema metadata storage and catalog management for VibeSQL.

## Overview

This crate provides metadata structures for database objects (tables, views, indexes, etc.) and the catalog registry that tracks schema information.

## Features

- **Schema Management**: Database, schema, and table metadata
- **Column Definitions**: Type information, constraints, and default values
- **Constraints**: PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK constraints
- **Indexes**: B-tree and spatial index metadata
- **Views**: View definitions and dependencies
- **Advanced Objects**: Functions, procedures, sequences, triggers, domains
- **Access Control**: Privilege tracking and grant management
- **User-Defined Types**: Support for custom type definitions

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
vibesql-catalog = "0.1"
```

Basic example:

```rust
use vibesql_catalog::{Catalog, TableSchema, ColumnSchema};
use vibesql_types::DataType;

// Create a catalog
let mut catalog = Catalog::new();

// Define table schema
let mut schema = TableSchema::new("users".to_string());
schema.add_column(ColumnSchema::new("id", DataType::Integer, false));
schema.add_column(ColumnSchema::new("name", DataType::Varchar(100), false));

// Register in catalog
catalog.add_table(schema);
```

## Documentation

- [API Documentation](https://docs.rs/vibesql-catalog)
- [Main VibeSQL Repository](https://github.com/rjwalters/vibesql)

## License

This project is licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE))
- MIT License ([LICENSE-MIT](../../LICENSE-MIT))

at your option.
