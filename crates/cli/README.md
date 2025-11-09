# VibeSQL CLI

Command-line interface for VibeSQL, providing interactive SQL querying and database management.

## Features

### Phase 1: Basic REPL (Current)
- Interactive SQL prompt with line editing and history
- Execute SQL statements and display results
- Basic table formatting for query results
- Command history and error handling
- Meta-commands: `\help`, `\q`, `\d`, `\dt`, `\timing`

### Phase 2: Script Execution & File I/O
- Execute SQL from files: `vibesql -f script.sql`
- Support multiple statements in scripts
- Transaction control for scripts
- Read from stdin: `cat queries.sql | vibesql`

### Phase 3: Advanced Features
- More meta-commands (`\ds`, `\di`, `\du`)
- Output format options (table, CSV, JSON, markdown)
- Connection to persistent database files
- Query timing and performance statistics
- Syntax highlighting
- Multi-line statement editing

### Phase 4: Database Management
- Schema introspection
- Database backup/restore
- Query profiling
- Configuration file support (.vibesqlrc)

## Usage

### Interactive REPL

```bash
# Start interactive session
cargo run --bin vibesql

# Start with specific database
cargo run --bin vibesql -- --database mydb.vsql
```

### Command Execution

```bash
# Execute single SQL command
cargo run --bin vibesql -- -c "SELECT 1"
```

### File Execution

```bash
# Execute SQL from file
cargo run --bin vibesql -- -f script.sql

# Execute from stdin
cat queries.sql | cargo run --bin vibesql
```

## Meta-Commands

```
\d [table]    - Describe table or list all tables
\dt           - List tables
\h, \help     - Show help
\timing       - Toggle query timing
\q, \quit     - Exit
```

## Example Session

```
$ vibesql
VibeSQL v0.1.0 - SQL:1999 FULL Compliance Database
Type \help for help, \quit to exit

vibesql> CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100));
Table created successfully

vibesql> INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
2 rows inserted

vibesql> SELECT * FROM users;
+----+-------+
| id | name  |
+----+-------+
| 1  | Alice |
| 2  | Bob   |
+----+-------+
2 rows

vibesql> \d users
Table: users
Columns:
  id   INT PRIMARY KEY
  name VARCHAR(100)

vibesql> \quit
Goodbye!
```

## Architecture

The CLI is organized into modules:

- `main.rs` - Entry point and CLI argument parsing
- `repl.rs` - Interactive REPL implementation
- `executor.rs` - SQL execution wrapper
- `formatter.rs` - Result formatting (table, CSV, JSON)
- `commands.rs` - Meta-command parsing and handling
- `error.rs` - CLI-specific error types

## Dependencies

- `clap` - Command-line argument parsing
- `rustyline` - Line editing and history
- `prettytable-rs` - Table formatting
- `serde_json` - JSON formatting
- `csv` - CSV formatting
- `anyhow` - Error handling
