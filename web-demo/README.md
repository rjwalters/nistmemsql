# NIST MemSQL Web Studio

Interactive SQL database demo running entirely in your browser using WebAssembly.

**[🚀 Live Demo](https://rjwalters.github.io/nistmemsql/)**

---

## Features

- **Zero Setup** - Start querying immediately with pre-loaded sample data
- **Monaco Editor** - Professional SQL editor with syntax highlighting and IntelliSense
- **WASM-Powered** - Rust database engine compiled to WebAssembly
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

# Build WASM module (from repository root)
cd crates/wasm-bindings
wasm-pack build --target web --out-dir ../../web-demo/public/pkg

# Back to web-demo directory
cd ../../web-demo

# Start development server
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
