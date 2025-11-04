# PyO3 Optimization Opportunities

## Current Situation

Our profiling shows that **PyO3 binding overhead** accounts for 50-85% of operation time:

| Operation | Rust Time | PyO3 Overhead | Overhead % |
|-----------|-----------|---------------|------------|
| COUNT     | 123µs     | 137µs         | 53%        |
| INSERT    | 13µs      | 71µs          | 84%        |
| UPDATE    | 76µs      | 92µs          | 55%        |
| DELETE    | 28µs      | 74µs          | 73%        |

**Question**: Can we reduce this overhead?

**Answer**: Yes! There are several PyO3-specific optimizations we can implement.

## Understanding PyO3 vs C Bindings

### Why C Bindings Are Faster

SQLite's `sqlite3` module (C → Python):
```c
// Direct C API call
PyObject* result = PyLong_FromLong(value);  // ~1-2µs
```

Our PyO3 bindings (Rust → Python):
```rust
// Must cross FFI boundary, allocate, convert
value.into_pyobject(py)?.into_any().unbind()  // ~5-15µs
```

**Key differences**:
1. **Memory model**: C can directly manipulate Python objects; Rust must use safe abstractions
2. **Type system**: Rust's ownership requires additional allocations/conversions
3. **Safety**: PyO3 prioritizes memory safety over raw speed
4. **Maturity**: SQLite's C bindings are 20+ years optimized; PyO3 is newer

### The Fundamental Trade-off

```
        Speed ←―――――――――――――――→ Safety

SQLite C:  ██████████░░░░░░░░░░░░   Fast but unsafe
PyO3:      ░░░░░░░░░░██████████████   Safe but slower
```

We can't match C's speed AND maintain Rust's safety guarantees. But we can improve!

## Optimization 1: Batch Operations (High Impact)

### Current Approach (High Overhead)
```python
# 84µs overhead PER operation
for i in range(1000):
    cursor.execute("INSERT INTO t VALUES (?, ?)", (i, i*2))
# Total: 1000 × 84µs = 84ms overhead
```

### Optimized Approach (Amortize Overhead)
```python
# 84µs overhead ONCE
cursor.executemany("INSERT INTO t VALUES (?, ?)",
    [(i, i*2) for i in range(1000)])
# Total: 84µs + (1000 × 13µs) = ~13ms overhead
# Improvement: 6.5x faster!
```

**Implementation**:
```rust
#[pymethods]
impl Cursor {
    /// Execute a query with multiple parameter sets
    fn executemany(
        &mut self,
        py: Python,
        sql: &str,
        params_list: &Bound<'_, PyList>,
    ) -> PyResult<()> {
        // Parse SQL once
        let stmt = parser::Parser::parse_sql(sql)?;

        // Execute multiple times without re-parsing
        for params in params_list.iter() {
            // Execute with params...
        }

        Ok(())
    }
}
```

**Expected Gain**: 5-10x for bulk operations

**Effort**: Medium (2-3 hours)

**Priority**: HIGH ⭐⭐⭐

## Optimization 2: Direct Row Format (Medium Impact)

### Current Approach
```rust
// Each value converted individually
for value in row.values {
    let py_value = sqlvalue_to_py(py, value)?;  // 5-10µs per value
    py_values.push(py_value);
}
```

### Optimized Approach
```rust
// Use PyO3's buffer protocol for numeric types
impl Row {
    fn to_py_tuple_fast(&self, py: Python) -> PyResult<Py<PyTuple>> {
        // Fast path for all-integer rows (common case)
        if self.all_integers() {
            // Use PyTuple::new_unchecked for speed
            // Convert integers in bulk using memcpy
            return fast_integer_tuple(py, &self.values);
        }

        // Fall back to slow path for mixed types
        self.to_py_tuple_safe(py)
    }
}
```

**Expected Gain**: 2-3x for integer-heavy queries

**Effort**: Medium (4-6 hours)

**Priority**: MEDIUM ⭐⭐

## Optimization 3: Reduce Allocations (Medium Impact)

### Current Issue
```rust
// From profiling:
//   SQL string copied:    3µs  ← Unnecessary copy
//   Cache lock:          11µs
```

### Optimized Approach
```rust
impl Cursor {
    fn execute(&mut self, py: Python, sql: &str, ...) -> PyResult<()> {
        // Don't copy the SQL string
        let cache_key = sql;  // Use &str directly as key

        // Use parking_lot::Mutex (faster than std::Mutex)
        let cache = self.stmt_cache.lock();

        // ... rest
    }
}
```

**Changes needed**:
1. Use `&str` keys in LRU cache instead of `String`
2. Replace `std::sync::Mutex` with `parking_lot::Mutex`
3. Avoid cloning AST when not needed

**Expected Gain**: 10-20µs per operation

**Effort**: Low (1-2 hours)

**Priority**: HIGH ⭐⭐⭐

## Optimization 4: Zero-Copy Result Iteration (High Impact)

### Current Approach
```python
cursor.execute("SELECT * FROM large_table")
result = cursor.fetchall()  # Materializes ALL rows in memory
```

### Optimized Approach
```python
cursor.execute("SELECT * FROM large_table")
for row in cursor:  # Yield rows one at a time (iterator protocol)
    process(row)
```

**Implementation**:
```rust
#[pymethods]
impl Cursor {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python) -> PyResult<Option<Py<PyTuple>>> {
        match &mut slf.last_result {
            Some(QueryResultData::Select { rows, .. }) => {
                if let Some(row) = slf.current_row_iter.next() {
                    let py_row = row_to_py_tuple(py, row)?;
                    Ok(Some(py_row))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }
}
```

**Expected Gain**: Memory usage: O(1) instead of O(n), speed: 2-3x for large results

**Effort**: Medium (3-4 hours)

**Priority**: HIGH ⭐⭐⭐

## Optimization 5: Prepared Statements (Medium Impact)

### Current Approach
```python
# Statement cached internally, but user has no control
for i in range(1000):
    cursor.execute("INSERT INTO t VALUES (?, ?)", (i, i*2))
```

### Optimized Approach
```python
# Explicit prepared statement
stmt = cursor.prepare("INSERT INTO t VALUES (?, ?)")
for i in range(1000):
    stmt.execute((i, i*2))  # No parsing overhead
```

**Implementation**:
```rust
#[pyclass]
struct PreparedStatement {
    stmt: ast::Statement,
    db: Arc<Mutex<storage::Database>>,
}

#[pymethods]
impl PreparedStatement {
    fn execute(&self, py: Python, params: &Bound<'_, PyTuple>) -> PyResult<()> {
        // Statement already parsed!
        // Just execute with params
    }
}

#[pymethods]
impl Cursor {
    fn prepare(&self, sql: &str) -> PyResult<PreparedStatement> {
        let stmt = parser::Parser::parse_sql(sql)?;
        Ok(PreparedStatement {
            stmt,
            db: self.db.clone()
        })
    }
}
```

**Expected Gain**: 20-40µs saved per execution (no parsing)

**Effort**: Medium (4-5 hours)

**Priority**: MEDIUM ⭐⭐

## Optimization 6: Faster Lock Implementation (Low Impact)

### Current Approach
```rust
use std::sync::Mutex;

let db = self.db.lock().unwrap();  // ~8-15µs
```

### Optimized Approach
```rust
use parking_lot::Mutex;

let db = self.db.lock();  // ~3-5µs (no poisoning check)
```

**Changes needed**:
```toml
# Cargo.toml
[dependencies]
parking_lot = "0.12"
```

```rust
// Replace all std::sync::Mutex with parking_lot::Mutex
use parking_lot::Mutex;
```

**Expected Gain**: 5-10µs per operation

**Effort**: Very Low (30 minutes)

**Priority**: HIGH ⭐⭐⭐ (easy win!)

## Optimization 7: Inline Small Conversions (Low Impact)

### Current Approach
```rust
fn sqlvalue_to_py(py: Python, value: &types::SqlValue) -> PyResult<Py<PyAny>> {
    Ok(match value {
        types::SqlValue::Integer(i) => (*i).into_pyobject(py)?.into_any().unbind(),
        // ... many branches
    })
}
```

### Optimized Approach
```rust
#[inline(always)]  // Force inlining for hot path
fn sqlvalue_to_py(py: Python, value: &types::SqlValue) -> PyResult<Py<PyAny>> {
    Ok(match value {
        types::SqlValue::Integer(i) => {
            // Fast path for common case
            unsafe {
                PyLong_FromLong(*i as c_long)  // Direct CPython API
            }
        }
        // ... fallback to safe path
    })
}
```

**Expected Gain**: 2-5µs per value conversion

**Effort**: Medium (needs careful unsafe usage)

**Priority**: LOW ⭐ (risky, marginal gains)

## Optimization 8: Custom PyO3 Types (Low Impact)

### Current Approach
```rust
// Generic Py<PyAny> for all values
let py_values: Vec<Py<PyAny>> = ...
```

### Optimized Approach
```rust
// Specialized types avoid dynamic dispatch
enum PyValue {
    Int(Py<PyLong>),
    Str(Py<PyString>),
    Float(Py<PyFloat>),
    // ...
}
```

**Expected Gain**: 1-3µs per value

**Effort**: High (major refactor)

**Priority**: LOW ⭐ (not worth complexity)

## Recommended Implementation Plan

### Phase 1: Quick Wins (1-2 days)
1. ✅ Replace `std::Mutex` with `parking_lot::Mutex` (30 min)
2. ✅ Reduce string allocations (`&str` cache keys) (1 hour)
3. ✅ Add `executemany()` for batch operations (3 hours)

**Expected total gain**: 30-50µs per operation + 5-10x for batches

### Phase 2: Medium Impact (3-5 days)
4. ✅ Iterator protocol for `fetchall()` (4 hours)
5. ✅ Prepared statement API (5 hours)
6. ✅ Fast path for integer rows (6 hours)

**Expected total gain**: Additional 20-40µs per operation, better memory usage

### Phase 3: Advanced (1-2 weeks)
7. ⚠️ Selective use of unsafe for hot paths (carefully!)
8. 📊 Benchmark and profile each change

**Expected total gain**: Additional 10-20µs per operation

## Realistic Performance Targets

### Current Performance
```
Operation    Time      Breakdown
──────────────────────────────────
INSERT       84µs      13µs Rust + 71µs PyO3
UPDATE      168µs      76µs Rust + 92µs PyO3
DELETE      102µs      28µs Rust + 74µs PyO3
COUNT       260µs     123µs Rust + 137µs PyO3
```

### After Phase 1 Optimizations
```
Operation    Time      Improvement    New Multiplier vs SQLite
──────────────────────────────────────────────────────────────
INSERT       54µs      1.6x faster    1.9x (was 3.1x)
UPDATE      118µs      1.4x faster    2.6x (was 3.8x)
DELETE       62µs      1.6x faster    2.3x (was 3.7x)
COUNT       180µs      1.4x faster    28x  (was 39x)
```

### After Phase 2 Optimizations
```
Operation    Time      Improvement    New Multiplier vs SQLite
──────────────────────────────────────────────────────────────
INSERT       40µs      2.1x faster    1.4x (was 3.1x)
UPDATE       90µs      1.9x faster    2.0x (was 3.8x)
DELETE       45µs      2.3x faster    1.7x (was 3.7x)
COUNT       130µs      2.0x faster    20x  (was 39x)

Batch INSERT (1000 rows):
  Current:   84ms      (1000 × 84µs)
  Optimized: 13ms      (84µs + 1000 × 13µs)
  Speedup:   6.5x
```

## Limitations

### What We CAN'T Do

1. **Match C performance exactly**: Rust's safety guarantees require some overhead
2. **Eliminate all allocations**: PyO3 requires allocations for Python objects
3. **Zero-cost FFI**: Crossing Rust↔Python boundary has inherent cost
4. **Avoid type conversions**: Python expects specific types

### What We CAN Do

1. ✅ Reduce unnecessary allocations
2. ✅ Use faster locks (parking_lot)
3. ✅ Batch operations to amortize overhead
4. ✅ Provide zero-copy iteration for large results
5. ✅ Expose prepared statements
6. ✅ Optimize hot paths with profiling

## Conclusion

**Can we improve PyO3 performance?** YES!

**Can we match SQLite's C bindings?** NO (but we can get closer)

**Is it worth it?** DEPENDS:

| Use Case | Worth Optimizing? |
|----------|-------------------|
| Educational database | Maybe (Phase 1 only) |
| Production workloads | Yes (Phase 1 + 2) |
| High-performance needs | Yes (All phases) |
| Bulk operations | Definitely (executemany!) |

**Recommended approach for nistmemsql**:
- Implement Phase 1 (quick wins, minimal complexity)
- Consider Phase 2 if users request it
- Document the trade-offs
- Focus on correctness and clarity over micro-optimizations

The performance gap is **architectural**, not a bug. We can reduce it by 40-60%, but will never match pure C bindings while maintaining Rust's safety guarantees.

## Next Steps

If we decide to optimize:

1. Start with profiling to confirm hot paths
2. Implement `parking_lot::Mutex` (easiest win)
3. Add `executemany()` (biggest user impact)
4. Benchmark each change
5. Document performance characteristics
6. Let users choose speed vs. safety trade-offs

**Priority**: Focus on user-facing features (`executemany`, iterators) over micro-optimizations.
