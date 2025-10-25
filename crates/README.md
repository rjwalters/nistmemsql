# nistmemsql Crates

This directory contains all the individual crates that make up the nistmemsql database.

## Crate Organization

### Core Components (Phase 1-3)

- **`types/`** - SQL:1999 type system
  - Data types (INTEGER, VARCHAR, BOOLEAN, etc.)
  - Type checking and coercion
  - User-defined types

- **`ast/`** - Abstract Syntax Tree definitions
  - SQL statement AST nodes
  - Expression AST nodes
  - Type-safe tree representation

- **`parser/`** - SQL:1999 parser
  - Lexer/tokenizer
  - Parser (using pest or lalrpop)
  - AST builder
  - Semantic analysis

- **`catalog/`** - Schema and metadata management
  - Database catalog
  - Schema definitions
  - Table metadata
  - Information schema views

- **`storage/`** - In-memory storage engine
  - Table storage (HashMap-based)
  - Row storage
  - Simple indexes
  - No persistence (ephemeral only)

- **`executor/`** - Query execution engine
  - Statement executors (SELECT, INSERT, UPDATE, DELETE)
  - Expression evaluator
  - Join algorithms (nested loop is fine)
  - Aggregate functions

- **`transaction/`** - Transaction manager
  - ACI properties (no D - durability)
  - Isolation levels
  - Simple single-threaded transaction model

### Protocol Drivers (Phase 6)

- **`odbc-driver/`** - ODBC driver implementation (coming later)
- **`jdbc-driver/`** - JDBC driver implementation (coming later)

## Development Phases

### Phase 1: Parser (Current Focus)
Work in: `parser/`, `ast/`, `types/`

### Phase 2: Storage
Work in: `storage/`, `catalog/`

### Phase 3: Execution
Work in: `executor/`, `transaction/`

### Phase 4-5: Advanced Features
Extend: `parser/`, `executor/`, `types/`

### Phase 6: Protocols
Work in: `odbc-driver/`, `jdbc-driver/`

## Building

```bash
# Build all crates
cargo build

# Build specific crate
cargo build -p parser

# Test all crates
cargo test

# Test specific crate
cargo test -p types

# Check without building (fast!)
cargo check
```

## Dependencies

Crates can depend on each other. Typical dependency flow:

```
odbc-driver ─┐
             ├─> executor -> storage -> catalog -> types
jdbc-driver ─┘              ↑          ↑
                            │          │
                   transaction         ast
                            ↑          ↑
                            │          │
                           parser ─────┘
```

## Documentation

Each crate has its own README with specific implementation details.

See `docs/architecture/` for overall system design.
