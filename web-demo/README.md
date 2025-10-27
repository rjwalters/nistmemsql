# NistMemSQL Web Demo

An interactive web-based SQL database demo powered by WebAssembly.

## Features

- **In-browser SQL database**: Runs entirely in your browser using WebAssembly
- **Full SQL support**: DDL (CREATE TABLE), DML (INSERT, UPDATE, DELETE), and SELECT queries
- **Interactive UI**: Clean, modern interface with syntax highlighting
- **Real-time execution**: Instant query execution without server round-trips
- **Example queries**: Pre-loaded examples to help you get started

## Getting Started

### Prerequisites

- Rust toolchain (1.70+)
- wasm-pack (`cargo install wasm-pack`)
- A modern web browser (Chrome, Firefox, Safari, or Edge)

### Building the WASM Module

From the repository root:

```bash
# Build the WASM package
wasm-pack build --target web --out-dir ../../web-demo/pkg crates/wasm-bindings
```

Alternatively, use the convenience script (from web-demo directory):

```bash
cd web-demo
./build.sh
```

### Running Locally

Start a local HTTP server from the `web-demo` directory:

```bash
# Using Python 3
python3 -m http.server 8080

# Or using Python 2
python -m SimpleHTTPServer 8080

# Or using Node.js http-server (npm install -g http-server)
http-server -p 8080
```

Then open your browser to: http://localhost:8080

## Usage

### Basic Workflow

1. **Create a table**: Start by defining your data structure
   ```sql
   CREATE TABLE users (
       id INTEGER,
       name VARCHAR,
       age INTEGER
   )
   ```

2. **Insert data**: Add rows to your table
   ```sql
   INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)
   ```

3. **Query data**: Retrieve and analyze your data
   ```sql
   SELECT * FROM users WHERE age > 25
   ```

### Supported SQL Features

- **DDL**: CREATE TABLE, DROP TABLE
- **DML**: INSERT, UPDATE, DELETE
- **Queries**: SELECT with WHERE, ORDER BY, GROUP BY, HAVING, DISTINCT
- **Joins**: INNER JOIN, LEFT OUTER JOIN
- **Aggregates**: COUNT, SUM, AVG, MIN, MAX
- **Operators**: Arithmetic (+, -, *, /), Comparison (=, <, >, <=, >=, <>), Logical (AND, OR, NOT)
- **Functions**: SQL:1999 compliant CASE expressions

### Keyboard Shortcuts

- **Ctrl/Cmd + Enter**: Execute query
- **Example button**: Load example queries
- **Clear button**: Clear the query input

## API Reference

The WASM module exposes the following JavaScript API:

### Database Class

```javascript
// Create a new database instance
const db = new Database();

// Execute DDL/DML statements (returns ExecuteResult)
const result = db.execute("CREATE TABLE users (id INTEGER, name VARCHAR)");
// result = { rows_affected: 0, message: "Table 'users' created successfully" }

// Execute SELECT queries (returns QueryResult)
const result = db.query("SELECT * FROM users");
// result = { columns: ['id', 'name'], rows: [...], row_count: 10 }

// List all tables
const tables = db.list_tables();
// returns: ['users', 'orders', ...]

// Get table schema
const schema = db.describe_table("users");
// returns: { name: 'users', columns: [{name: 'id', data_type: 'Integer', nullable: false}, ...] }

// Get version
const version = db.version();
// returns: "nistmemsql-wasm 0.1.0"
```

## File Structure

```
web-demo/
├── index.html          # Main HTML page
├── css/
│   └── style.css       # Styling
├── js/
│   └── app.js          # Application logic
├── pkg/                # WASM build output (generated)
│   ├── wasm_bindings.js
│   ├── wasm_bindings_bg.wasm
│   └── ...
└── README.md           # This file
```

## Browser Compatibility

The demo requires WebAssembly support and modern JavaScript features (ES6+):

- ✅ Chrome 57+
- ✅ Firefox 52+
- ✅ Safari 11+
- ✅ Edge 79+

## Troubleshooting

### WASM module fails to load

**Problem**: "Failed to initialize WASM module"

**Solution**:
- Ensure you're serving the files over HTTP(S), not file://
- Check browser console for CORS errors
- Rebuild the WASM module if it's outdated

### Query execution errors

**Problem**: "Parse error" or "Execution error"

**Solution**:
- Check SQL syntax (case-insensitive keywords)
- Ensure tables exist before querying them
- Verify column names match table schema
- Check the browser console for detailed error messages

### Styling issues

**Problem**: Demo looks unstyled or broken

**Solution**:
- Clear browser cache and reload
- Check that css/style.css is being loaded (Network tab in dev tools)
- Ensure HTTP server is serving from the correct directory

## Development

### Rebuilding After Changes

After modifying Rust code in `crates/wasm-bindings/`:

```bash
# Rebuild WASM module
cd /path/to/repo
wasm-pack build --target web --out-dir ../../web-demo/pkg crates/wasm-bindings

# Reload browser (hard refresh: Ctrl+Shift+R or Cmd+Shift+R)
```

### Debug Mode

For development, you can build in debug mode for better error messages:

```bash
wasm-pack build --dev --target web --out-dir ../../web-demo/pkg crates/wasm-bindings
```

Note: Debug builds are significantly larger (~2-3x) than release builds.

## Performance

- **WASM bundle size**: ~270KB (optimized with wasm-opt)
- **Load time**: < 100ms on modern hardware
- **Query execution**: Near-native performance for in-memory operations
- **Memory usage**: Efficient Rust memory management via WebAssembly

## License

See repository root LICENSE file.

## Contributing

Contributions welcome! Please see the main repository README for contribution guidelines.
