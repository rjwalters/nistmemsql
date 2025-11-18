# VibeSQL Crates

This directory contains all the individual crates that make up the VibeSQL database engine.

## Crate Organization

### Core Engine Crates

- **`vibesql-types/`** - SQL:1999 type system
  - Complete data type implementation (INTEGER, VARCHAR, BOOLEAN, DATE, TIMESTAMP, NUMERIC, etc.)
  - Type checking and coercion rules
  - Spatial/geometric types (POINT, LINESTRING, POLYGON, etc.)
  - Full numeric precision handling

- **`vibesql-ast/`** - Abstract Syntax Tree definitions
  - SQL statement AST nodes (DDL, DML, DCL)
  - Expression AST nodes
  - Type-safe tree representation
  - Visitor pattern support

- **`vibesql-parser/`** - SQL:1999 parser
  - Hand-written lexer/tokenizer with full SQL:1999 syntax
  - Recursive descent parser
  - Comprehensive error reporting with position tracking
  - Support for advanced features (CTEs, window functions, spatial queries)

- **`vibesql-catalog/`** - Schema and metadata management
  - Database catalog with full schema support
  - Table, view, and index metadata
  - Information schema views (INFORMATION_SCHEMA)
  - Foreign key relationship tracking
  - Stored procedure/function registry

- **`vibesql-storage/`** - Storage engine
  - In-memory row storage with B-tree indexes
  - Spatial indexes (R-tree) for geometric data
  - Full-text indexes for text search
  - Efficient scan and lookup operations
  - Iterator-based execution support

- **`vibesql-executor/`** - Query execution engine
  - Complete statement executors (SELECT, INSERT, UPDATE, DELETE, MERGE, TRUNCATE)
  - Advanced expression evaluator with 200+ built-in functions
  - Multiple join algorithms (hash join, nested loop, merge join)
  - Aggregate and window functions
  - Subquery execution (correlated and uncorrelated)
  - Common Table Expressions (CTEs)
  - Query optimization (predicate pushdown, projection pruning)
  - Transaction support with MVCC

### Interface Crates

- **`vibesql-cli/`** - Command-line interface
  - Interactive SQL shell (REPL)
  - PostgreSQL-compatible meta-commands (\d, \dt, \l, etc.)
  - Multiple output formats (table, CSV, JSON, XML, HTML)
  - Import/export functionality
  - Query history and editing

- **`vibesql-wasm-bindings/`** - WebAssembly bindings
  - WASM-compatible API for browser execution
  - JavaScript interop layer
  - Powers the live web demo at https://rjwalters.github.io/vibesql/

- **`vibesql-python-bindings/`** - Python bindings
  - Python API for embedding VibeSQL
  - DB-API 2.0 compatible interface
  - PyO3-based implementation

### Testing Infrastructure

- **`sqllogictest/`** - SQL conformance testing
  - SQLLogicTest parser and runner
  - 617/623 test suites passing (99.0%, ~5.6M tests)
  - Validates SQL:1999 compliance

## Building

```bash
# Build all crates
cargo build

# Build specific crate
cargo build -p vibesql-executor

# Test all crates
cargo test

# Test specific crate
cargo test -p vibesql-types

# Run SQLLogicTest suite
cargo test -p sqllogictest

# Check without building (fast!)
cargo check

# Build CLI
cargo build -p vibesql-cli --release

# Build WASM bindings
wasm-pack build crates/vibesql-wasm-bindings
```

## Crate Dependencies

The crates form a layered architecture with clear dependency flow:

```
┌─────────────────────────────────────────────────┐
│  Interface Layer                                │
│  vibesql-cli, vibesql-wasm-bindings,           │
│  vibesql-python-bindings                        │
└────────────────┬────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────┐
│  Execution Layer                                │
│  vibesql-executor ◄─── sqllogictest            │
└────────┬────────────────────────────────────────┘
         │
┌────────▼────────┬───────────────────────────────┐
│                 │                               │
│  vibesql-       │  vibesql-       vibesql-     │
│  storage        │  catalog        parser       │
│                 │       │            │          │
└─────────────────┴───────┴────────────┴──────────┘
                          │            │
                 ┌────────▼────────────▼─────────┐
                 │  Foundation Layer             │
                 │  vibesql-types, vibesql-ast   │
                 └───────────────────────────────┘
```

**Key relationships:**
- `vibesql-types` and `vibesql-ast` are foundational with no internal dependencies
- `vibesql-parser` depends on types and ast
- `vibesql-catalog` depends on types and ast
- `vibesql-storage` depends on types and catalog
- `vibesql-executor` depends on all core crates and orchestrates everything
- Interface crates depend on executor for complete functionality

## Development Status

✅ **Complete and Production-Ready**
- All core SQL:1999 features implemented
- 100% sqltest conformance (739/739 mandatory tests)
- 99.0% SQLLogicTest coverage (617/623 suites, ~5.6M tests)
- Full test coverage across all crates
- Comprehensive documentation

## Documentation

- Each crate contains inline documentation accessible via `cargo doc`
- Build documentation: `cargo doc --no-deps --open`
- See main [README.md](../README.md) for project overview
- See [ARCHITECTURE.md](../ARCHITECTURE.md) for system design details
