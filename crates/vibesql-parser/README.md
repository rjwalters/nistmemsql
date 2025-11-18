# vibesql-parser

SQL:1999 compliant parser for VibeSQL database engine.

## Overview

This crate provides tokenization and parsing of SQL statements into the shared AST. It implements a full SQL:1999 parser with support for modern SQL features.

## Features

- **Full SQL:1999 Support**: Complete coverage of SQL:1999 standard
- **Lexical Analysis**: Token-based lexer with keyword recognition
- **Syntax Parsing**: Recursive descent parser for SQL statements
- **Error Reporting**: Detailed parse error messages with location information
- **Performance**: Optimized for fast parsing of complex queries

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
vibesql-parser = "0.1"
```

Basic example:

```rust
use vibesql_parser::Parser;

let sql = "SELECT name, age FROM users WHERE age > 18";
let mut parser = Parser::new(sql);

match parser.parse() {
    Ok(statements) => {
        // Process parsed AST
        for stmt in statements {
            println!("{:?}", stmt);
        }
    }
    Err(e) => eprintln!("Parse error: {}", e),
}
```

## Documentation

- [API Documentation](https://docs.rs/vibesql-parser)
- [Main VibeSQL Repository](https://github.com/rjwalters/vibesql)

## License

This project is licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE))
- MIT License ([LICENSE-MIT](../../LICENSE-MIT))

at your option.
