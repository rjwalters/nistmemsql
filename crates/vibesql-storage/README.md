# vibesql-storage

In-memory and persistent storage engine for VibeSQL.

## Overview

This crate provides the storage layer for VibeSQL, including in-memory data structures, buffer pool management, persistence, and indexing.

## Features

- **In-Memory Tables**: High-performance row storage with efficient access patterns
- **Buffer Pool**: Page-based buffer management with LRU eviction
- **Persistence**: SQL dump format for loading and saving databases
- **B-tree Indexes**: Efficient ordered index structures
- **Spatial Indexes**: R*-tree implementation for geometric queries
- **Statistics**: Column and table statistics for query optimization
- **Query Buffers**: Specialized buffer pools for query execution
- **WASM Support**: Cross-platform storage with OPFS backend for browsers
- **Native Storage**: File-based backend for native platforms

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
vibesql-storage = "0.1"
```

Basic example:

```rust
use vibesql_storage::{Database, DatabaseConfig};
use vibesql_catalog::{Catalog, TableSchema, ColumnSchema};
use vibesql_types::DataType;

// Create database
let catalog = Catalog::new();
let config = DatabaseConfig::default();
let mut db = Database::new(catalog, config);

// Create and use table
let mut schema = TableSchema::new("users".to_string());
schema.add_column(ColumnSchema::new("id", DataType::Integer, false));
db.create_table(schema)?;
```

## Documentation

- [API Documentation](https://docs.rs/vibesql-storage)
- [Main VibeSQL Repository](https://github.com/rjwalters/vibesql)

## License

This project is licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE))
- MIT License ([LICENSE-MIT](../../LICENSE-MIT))

at your option.
