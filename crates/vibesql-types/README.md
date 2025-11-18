# vibesql-types

SQL:1999 type system for VibeSQL database engine.

## Overview

This crate provides the foundational type system for SQL:1999 compliance, including data type definitions, SQL value representations, and type compatibility rules.

## Features

- **Data Types**: INTEGER, REAL, VARCHAR, BOOLEAN, DATE, TIME, TIMESTAMP, INTERVAL, DECIMAL, BLOB, and more
- **SQL Values**: Type-safe representation of SQL values with NULL support
- **Type Coercion**: SQL:1999 compliant type compatibility and conversion rules
- **Temporal Types**: Full support for DATE, TIME, TIMESTAMP, and INTERVAL types
- **SQL Mode Support**: Configurable type behavior for MySQL compatibility

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
vibesql-types = "0.1"
```

Basic example:

```rust
use vibesql_types::{DataType, SqlValue};

// Define a data type
let int_type = DataType::Integer;

// Create SQL values
let value = SqlValue::Integer(42);
let null_value = SqlValue::Null;

// Check type compatibility
assert!(value.is_compatible_with(&int_type));
```

## Documentation

- [API Documentation](https://docs.rs/vibesql-types)
- [Main VibeSQL Repository](https://github.com/rjwalters/vibesql)

## License

This project is licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE))
- MIT License ([LICENSE-MIT](../../LICENSE-MIT))

at your option.
