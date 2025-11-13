# NIST MemSQL Web Studio

Interactive SQL database demo running entirely in your browser using WebAssembly.

**[🚀 Live Demo](https://rjwalters.github.io/nistmemsql/)**

---

## Features

- **Zero Setup** - Start querying immediately with pre-loaded sample data
- **Monaco Editor** - Professional SQL editor with syntax highlighting and IntelliSense
- **WASM-Powered** - Rust database engine compiled to WebAssembly
- **Persistent Storage** - Data persists across browser sessions using OPFS (Origin Private File System)
- **SQL Comment Support** - Use `--` for inline documentation
- **Real-time Results** - See query results instantly with formatted tables
- **Export Options** - Copy to clipboard or download as CSV
- **Dark Mode** - Beautiful Tailwind CSS interface with theme toggle
- **Keyboard Shortcuts** - Ctrl/Cmd+Enter to execute, Ctrl/Cmd+/ to toggle comments

---

## Pre-loaded Sample Data

The demo includes a sample `employees` table with 6 records:

| id  | name           | department   | salary |
| --- | -------------- | ------------ | ------ |
| 1   | Alice Johnson  | Engineering  | 95000  |
| 2   | Bob Smith      | Engineering  | 87000  |
| 3   | Carol White    | Sales        | 72000  |
| 4   | David Brown    | Sales        | 68000  |
| 5   | Eve Martinez   | HR           | 65000  |
| 6   | Frank Wilson   | Engineering  | 92000  |

---

## Example Queries

### Basic SELECT

```sql
-- See all employees
SELECT * FROM employees;

-- Select specific columns
SELECT name, department FROM employees;
```

### Filtering with WHERE

```sql
-- Find engineering employees
SELECT name, salary FROM employees WHERE department = 'Engineering';

-- Find high earners
SELECT name, department, salary FROM employees WHERE salary > 80000;
```

### Aggregation

```sql
-- Count employees per department
SELECT department, COUNT(*) as count FROM employees GROUP BY department;

-- Average salary by department
SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department;
```

### Sorting

```sql
-- Highest paid employees first
SELECT name, salary FROM employees ORDER BY salary DESC;

-- Alphabetical by department, then name
SELECT * FROM employees ORDER BY department, name;
```

---

## Persistent Storage with OPFS

VibeSQL Web Demo uses the **Origin Private File System (OPFS)** API to persist database data across browser sessions. This means your tables, data, and indexes remain intact even after closing and reopening your browser.

### Browser Compatibility

| Browser  | OPFS Support | Minimum Version |
| -------- | ------------ | --------------- |
| Chrome   | ✅ Yes       | 86+             |
| Edge     | ✅ Yes       | 86+             |
| Firefox  | ✅ Yes       | 111+            |
| Safari   | ✅ Yes       | 15.2+           |

### How It Works

- **Automatic Persistence**: When you create tables or insert data, it's automatically saved to OPFS
- **Browser-Specific**: Each origin (domain) has its own isolated storage
- **Secure**: Data is private to your browser and cannot be accessed by other origins
- **Storage Status**: Check the "Storage" indicator next to the SQL Editor to see if OPFS is active

### Persistence Example

```sql
-- Create a table (persists across sessions)
CREATE TABLE my_notes (id INTEGER, note TEXT);

-- Insert data (persists across sessions)
INSERT INTO my_notes VALUES (1, 'Remember to buy milk');

-- Close browser and reopen demo

-- Your data is still here!
SELECT * FROM my_notes;
```

### Fallback Behavior

If OPFS is not supported in your browser, the demo automatically falls back to in-memory storage. The storage status indicator will show "Memory (Temporary)" in this case.

### Clearing Stored Data

To clear all persistent data:

1. Open browser DevTools (F12)
2. Go to Application/Storage tab
3. Find "Origin Private File System" or "File System"
4. Delete the vibesql data

Or clear all site data for the demo origin through browser settings.

---

## Development

### Prerequisites

- Node.js 20+
- pnpm 10+
- Rust toolchain
- wasm-pack

### Setup

```bash
# Install dependencies
pnpm install

# Build WASM module with OPFS support (from repository root)
./scripts/build-wasm.sh

# Or manually:
# wasm-pack build --target web --out-dir web-demo/public/pkg --release crates/vibesql-wasm-bindings

# Start development server
cd web-demo
pnpm dev
```

### Available Scripts

```bash
pnpm dev          # Start development server
pnpm build        # Build for production
pnpm preview      # Preview production build
pnpm test         # Run tests
pnpm test:ui      # Run tests with UI
pnpm test:coverage # Run tests with coverage
pnpm lint         # Lint code
pnpm lint:fix     # Lint and fix
pnpm format       # Format code
pnpm format:check # Check formatting
```

---

## Architecture

### Tech Stack

- **Frontend**: TypeScript, Vite, Tailwind CSS
- **Editor**: Monaco Editor (VS Code engine)
- **Database**: Rust compiled to WebAssembly via wasm-pack
- **Testing**: Vitest
- **CI/CD**: GitHub Actions

### Project Structure

```
web-demo/
├── src/
│   ├── components/     # UI components (Results, HelpModal)
│   ├── db/            # WASM database interface
│   ├── editor/        # SQL validation
│   ├── showcase/      # Feature showcase
│   ├── styles/        # Tailwind CSS
│   ├── main.ts        # Application entry point
│   └── theme.ts       # Dark/light mode
├── public/
│   ├── pkg/           # WASM bindings (generated)
│   └── examples/      # Sample SQL files
├── index.html         # HTML entry point
└── vite.config.ts     # Build configuration
```

### Key Components

#### Database Interface (`src/db/types.ts`)

```typescript
interface Database {
  execute(sql: string): ExecuteResult // DDL/DML
  query(sql: string): QueryResult // SELECT
  list_tables(): string[]
  describe_table(table: string): TableSchema
}
```

#### Results Component (`src/components/Results.ts`)

Displays query results with:

- Formatted tables for SELECT queries
- Success messages for DDL/DML operations
- Error display with syntax highlighting
- Export to CSV or clipboard

---

## Deployment

The web demo is automatically deployed to GitHub Pages via GitHub Actions:

1. On push to `main` branch:
   - Build WASM module with Rust
   - Run quality checks (lint, format, typecheck)
   - Run tests
   - Build Vite app
   - Deploy to GitHub Pages

2. Deployment URL: https://rjwalters.github.io/nistmemsql/

---

## Troubleshooting

### WASM module not loading

If you see "Use query() method for SELECT statements" error:

1. Rebuild WASM module:
   ```bash
   cd crates/wasm-bindings
   wasm-pack build --target web --out-dir ../../web-demo/public/pkg
   ```

2. Clear browser cache and reload

### TypeScript errors

```bash
pnpm exec tsc --noEmit  # Type check without building
```

### Formatting issues

```bash
pnpm format  # Auto-fix formatting
```

---

## Features in Development

- [ ] Multiple example databases (Northwind, etc.)
- [ ] SQL:1999 feature showcase
- [ ] Query history
- [ ] Save/load queries
- [ ] Schema explorer sidebar
- [ ] Query performance metrics

---

## Contributing

See the main [README.md](../README.md) for contribution guidelines.

For web demo specific improvements:

1. UI/UX enhancements
2. Additional example queries
3. Better error messages
4. Performance optimizations
5. Accessibility improvements

---

## License

MIT License - See [LICENSE](../LICENSE) for details.
