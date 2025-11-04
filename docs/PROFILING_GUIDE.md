# Performance Profiling Guide

This guide shows how to use nistmemsql's built-in profiling tools to understand performance characteristics.

## Quick Start

```python
import nistmemsql

# Enable profiling (prints detailed timing to stderr)
nistmemsql.enable_profiling()

conn = nistmemsql.connect()
cursor = conn.cursor()

# Execute queries - profiling output will show timing breakdown
cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
cursor.execute("SELECT COUNT(*) FROM users")
result = cursor.fetchall()

# Disable profiling when done
nistmemsql.disable_profiling()
```

## Understanding Profiling Output

### Example Output

```
[PROFILE] === Starting: execute() ===
[PROFILE]   SQL string copied | delta: 0.003ms | total: 0.003ms
[PROFILE]   Acquired cache lock | delta: 0.011ms | total: 0.014ms
[PROFILE]   Cache MISS - need to parse | delta: 0.009ms | total: 0.023ms
[PROFILE]   SQL parsed to AST | delta: 0.067ms | total: 0.090ms
[PROFILE]   AST cached | delta: 0.012ms | total: 0.102ms
[PROFILE]   Statement ready for execution | delta: 0.004ms | total: 0.106ms
[PROFILE]   Database lock acquired (SELECT) | delta: 0.008ms | total: 0.114ms
[PROFILE]   SelectExecutor created | delta: 0.008ms | total: 0.122ms
[PROFILE]   SELECT executed in Rust | delta: 0.123ms | total: 0.245ms
[PROFILE]   Result stored | delta: 0.007ms | total: 0.252ms
[PROFILE] === Completed: execute() in 0.260ms (260µs) ===

[PROFILE] === Starting: fetchall() ===
[PROFILE]   Starting fetch of 1 rows | delta: 0.002ms | total: 0.002ms
[PROFILE]   Empty PyList created | delta: 0.009ms | total: 0.011ms
[PROFILE]   All rows converted to Python | delta: 0.013ms | total: 0.024ms
[PROFILE] === Completed: fetchall() in 0.032ms (31µs) ===
```

### Reading the Output

Each checkpoint shows:
- **Checkpoint name**: What operation just completed
- **delta**: Time since last checkpoint (in milliseconds)
- **total**: Total time since operation started

The final line shows total execution time in both milliseconds and microseconds.

## Common Profiling Patterns

### Profile INSERT Performance

```python
import nistmemsql

nistmemsql.enable_profiling()
conn = nistmemsql.connect()
cursor = conn.cursor()

cursor.execute("CREATE TABLE test (id INTEGER, value INTEGER)")

print("\n=== First INSERT (includes table setup) ===")
cursor.execute("INSERT INTO test VALUES (1, 100)")

print("\n=== Subsequent INSERTs ===")
for i in range(2, 5):
    cursor.execute(f"INSERT INTO test VALUES ({i}, {i * 100})")

nistmemsql.disable_profiling()
```

**What to look for**:
- First INSERT is slower (table initialization)
- Subsequent INSERTs should be fast (~80-100µs)
- "INSERT executed in Rust" shows core operation time

### Profile UPDATE with PRIMARY KEY Optimization

```python
import nistmemsql

nistmemsql.enable_profiling()
conn = nistmemsql.connect()
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name VARCHAR(50),
        email VARCHAR(100)
    )
""")

# Insert test data
for i in range(1, 11):
    cursor.execute(f"INSERT INTO users VALUES ({i}, 'User{i}', 'user{i}@example.com')")

print("\n=== UPDATE with PRIMARY KEY ===")
cursor.execute("UPDATE users SET email = 'newemail@example.com' WHERE id = 5")

nistmemsql.disable_profiling()
```

**What to look for**:
- "Schema cache lookup" should be fast (~10-15µs)
- "UPDATE executed in Rust" shows optimized execution
- Total time should be ~150-200µs regardless of table size

### Profile COUNT(*) Fast Path

```python
import nistmemsql

nistmemsql.enable_profiling()
conn = nistmemsql.connect()
cursor = conn.cursor()

cursor.execute("CREATE TABLE items (id INTEGER, value INTEGER)")
for i in range(100):
    cursor.execute(f"INSERT INTO items VALUES ({i}, {i * 10})")

nistmemsql.disable_profiling()  # Disable during setup

print("\n=== COUNT(*) with fast path ===")
nistmemsql.enable_profiling()
cursor.execute("SELECT COUNT(*) FROM items")
result = cursor.fetchall()
nistmemsql.disable_profiling()

print(f"Result: {result}")
```

**What to look for**:
- "SELECT executed in Rust" should be fast (~100-150µs)
- Time should NOT scale with number of rows (fast path!)
- fetchall() should be minimal (only 1 row returned)

### Profile SELECT with Result Serialization

```python
import nistmemsql

nistmemsql.enable_profiling()
conn = nistmemsql.connect()
cursor = conn.cursor()

cursor.execute("CREATE TABLE data (id INTEGER, value INTEGER)")
for i in range(100):
    cursor.execute(f"INSERT INTO data VALUES ({i}, {i * 10})")

nistmemsql.disable_profiling()

print("\n=== SELECT with result serialization ===")
nistmemsql.enable_profiling()
cursor.execute("SELECT * FROM data")
result = cursor.fetchall()
nistmemsql.disable_profiling()

print(f"Fetched {len(result)} rows")
```

**What to look for**:
- "SELECT executed in Rust" shows query execution
- "All rows converted to Python" shows serialization overhead
- More rows = longer fetchall() time (linear scaling)

## Interpreting Performance

### Python Binding Overhead

Typical overhead breakdown for a simple operation:

```
SQL string handling:      ~2-4µs
Cache lock acquisition:   ~8-20µs
SQL parsing:              ~10-70µs (depends on query complexity)
Database lock:            ~8-15µs
Result serialization:     ~7-33µs (depends on result size)
```

**Total constant overhead**: ~50-140µs per operation

### Rust Execution Times

Core operation times (after warm-up):

```
INSERT:       ~10-15µs per row
UPDATE:       ~30-80µs (with PRIMARY KEY optimization)
DELETE:       ~20-30µs (with PRIMARY KEY optimization)
COUNT(*):     ~120-130µs (fast path, no row materialization)
SELECT:       ~5-10µs per row (plus serialization)
```

### Cache Effects

**Statement Cache**:
- First execution: Parse SQL (~30-70µs)
- Subsequent: Clone cached AST (~5-20µs)

**Schema Cache**:
- Cached lookup: ~10-15µs
- Full catalog scan: ~100-200µs

## Profiling in Tests

Add profiling to specific test cases:

```python
def test_insert_performance():
    import nistmemsql

    nistmemsql.enable_profiling()

    conn = nistmemsql.connect()
    cursor = conn.cursor()

    cursor.execute("CREATE TABLE test (id INTEGER)")

    # Profile a batch of inserts
    for i in range(10):
        cursor.execute(f"INSERT INTO test VALUES ({i})")

    nistmemsql.disable_profiling()
```

## Advanced: Custom Profiling

For more detailed profiling, you can wrap operations:

```python
import nistmemsql
import time

def profile_operation(name, func):
    """Profile a single operation"""
    nistmemsql.enable_profiling()
    start = time.perf_counter()

    result = func()

    end = time.perf_counter()
    nistmemsql.disable_profiling()

    print(f"\n{name}: {(end - start) * 1000:.3f}ms total")
    return result

# Usage
conn = nistmemsql.connect()
cursor = conn.cursor()

profile_operation("CREATE TABLE",
    lambda: cursor.execute("CREATE TABLE test (id INTEGER)"))

profile_operation("INSERT 100 rows",
    lambda: [cursor.execute(f"INSERT INTO test VALUES ({i})") for i in range(100)])

profile_operation("COUNT(*)",
    lambda: cursor.execute("SELECT COUNT(*) FROM test"))
```

## Performance Expectations

### Good Performance (Educational Database)

For nistmemsql, these are **excellent** numbers:

- INSERT: < 200µs per row
- UPDATE (with PK): < 300µs
- DELETE (with PK): < 200µs
- COUNT(*): < 500µs
- SELECT: < 100µs + ~5µs per row

### When to Investigate

Investigate if you see:

- INSERT > 1ms per row (check FK constraints)
- UPDATE > 2ms (verify PRIMARY KEY optimization)
- DELETE > 2ms (verify PRIMARY KEY optimization)
- COUNT(*) > 2ms (verify fast path is active)
- SELECT > 1ms for < 100 rows (check query complexity)

## Comparison with SQLite

Remember that SQLite's Python bindings are implemented in C with minimal overhead (~1-5µs), while nistmemsql uses PyO3 with ~50-140µs overhead.

**Example comparison** (1K rows, single operation):
```
Operation    SQLite    nistmemsql    Gap         Explanation
──────────────────────────────────────────────────────────────
INSERT       ~50µs     ~155µs        3.1x        ✅ Good (PyO3 overhead)
UPDATE       ~45µs     ~171µs        3.8x        ✅ Good (PyO3 overhead)
DELETE       ~40µs     ~148µs        3.7x        ✅ Good (PyO3 overhead)
COUNT(*)     ~6µs      ~234µs        39x         🟡 High multiplier, low absolute time
```

The high COUNT multiplier is expected:
- SQLite's C bindings have almost zero overhead
- Our ~137µs Python overhead dominates when the operation is tiny
- The Rust code itself is fast (~123µs)
- Absolute time is still acceptable (< 300µs)

## See Also

- [Performance Analysis](PERFORMANCE_ANALYSIS.md) - Detailed breakdown of performance characteristics
- [Benchmarking Guide](../benchmarks/README.md) - How to run comprehensive benchmarks
