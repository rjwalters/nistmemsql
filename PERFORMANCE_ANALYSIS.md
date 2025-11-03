# Performance Analysis: nistmemsql vs SQLite3

**Date**: November 3, 2025
**Benchmark Environment**: macOS 15.7.1 (arm64), Python 3.12.11
**Dataset**: 1,000 rows per test

## Executive Summary

nistmemsql is **2.8x - 45x slower** than SQLite3 across different operations. We've already achieved **5-7x improvements** through HashMap optimization of GROUP BY operations. The remaining gaps are primarily in:

1. **UPDATE** (35x slower) - Most critical
2. **COUNT** (45x slower) - Still extremely slow
3. **DELETE/SELECT** (3-4x slower) - Moderate gaps

## Current Performance (After HashMap Optimization)

| Operation | nistmemsql (μs) | SQLite3 (μs) | Slowdown | Priority |
|-----------|-----------------|--------------|----------|----------|
| **UPDATE (1K)** | 28,517 | 812 | **35.1x** | 🔴 CRITICAL |
| **COUNT (1K)** | 56 | 1.2 | **45.1x** | 🔴 CRITICAL |
| **SELECT WHERE (1K)** | 146 | 41 | **3.5x** | 🟡 HIGH |
| **DELETE (1K)** | 1,647 | 463 | **3.6x** | 🟡 HIGH |
| **INSERT (1K)** | 3,364 | 1,212 | **2.8x** | 🟢 MEDIUM |
| **SUM (1K)** | 146 | 74 | **2.0x** | 🟢 MEDIUM |
| **AVG (1K)** | 126 | 60 | **2.1x** | 🟢 MEDIUM |

## Recent Optimizations

### HashMap GROUP BY Optimization (Nov 3, 2025)
**Commit**: `980dce3`
**Impact**: 5-7x improvement

Replaced O(n²) linear search with HashMap-based grouping:

```rust
// Before: O(n²) linear search through all groups
for (existing_key, group_rows) in &mut groups {
    if existing_key == &key { /* found */ }
}

// After: O(1) HashMap lookup
groups_map.entry(key).or_insert_with(Vec::new).push(row.clone());
```

**Results**:
- COUNT: 355.94 μs → 62.28 μs (5.7x faster)
- INSERT: 31.7 ms → 4.77 ms (6.6x faster)

**Files Modified**:
- `crates/executor/src/select/grouping.rs`

## Performance Gap Analysis

### 1. UPDATE Operations (35x slower) 🔴 CRITICAL

**Observation**: 1,000 individual `UPDATE ... WHERE id = ?` statements take 28.5ms vs 0.8ms in SQLite.

**Current Implementation** (`crates/executor/src/update/mod.rs`):
```rust
// For each UPDATE statement:
1. Get table schema (catalog lookup)
2. Select rows matching WHERE clause
3. Check foreign key constraints (if PK being updated)
4. Apply assignments
5. Validate constraints (NOT NULL, PK, UNIQUE, CHECK)
6. Validate foreign keys
7. Apply update
```

**Potential Bottlenecks**:
- Schema lookups on every UPDATE (should cache)
- Constraint validation overhead
- Foreign key checking even when no FKs exist
- Row selection might not use indexes
- Python binding overhead (1,000 calls)

**Optimization Opportunities**:
1. **Schema caching** - Already has `execute_with_schema()` but not used by Python bindings
2. **Skip FK checks** when no foreign keys defined
3. **Batch updates** - Process multiple UPDATEs in one call
4. **Index usage** - Ensure WHERE id = ? uses primary key index
5. **Reduce allocations** - Minimize cloning during constraint validation

### 2. COUNT Aggregates (45x slower) 🔴 CRITICAL

**Observation**: `SELECT COUNT(*) FROM table` takes 56μs vs 1.2μs in SQLite.

**Current Status**:
- Fast path exists for simple `COUNT(*)` without WHERE/GROUP BY
- But it seems not to be triggered in all cases
- Still doing unnecessary work even with HashMap optimization

**Investigation Needed**:
- Why isn't the fast path being hit?
- Is Python binding overhead the issue?
- Check if `table.row_count()` is O(1) or O(n)

**File**: `crates/executor/src/select/executor/aggregation/mod.rs:26-34`

### 3. DELETE Operations (3.6x slower) 🟡 HIGH

**Observation**: 1,000 individual `DELETE WHERE id = ?` statements take 1.6ms vs 0.46ms.

**Similar Issues to UPDATE**:
- Schema lookups
- Foreign key checking
- Constraint validation
- Index usage

### 4. SELECT WHERE (3.5x slower) 🟡 HIGH

**Observation**: `SELECT * FROM table WHERE id < 100` takes 146μs vs 41μs.

**Potential Issues**:
- No index usage on WHERE clauses?
- Full table scan even for simple predicates?
- Row materialization overhead?

### 5. INSERT Operations (2.8x slower) 🟢 MEDIUM

**Observation**: 1,000 individual INSERTs take 3.4ms vs 1.2ms.

**Status**: Acceptable gap, but room for improvement through batching.

## Optimization Roadmap

### Phase 1: Quick Wins (High Impact, Low Effort)

1. **Enable schema caching in Python bindings**
   - Use `execute_with_schema()` for UPDATE/DELETE
   - Cache schema in Python cursor object
   - **Expected Impact**: 2-3x improvement on UPDATE/DELETE

2. **Skip FK checks when no FKs defined**
   - Add early return in `ForeignKeyValidator`
   - **Expected Impact**: 10-20% improvement on UPDATE/DELETE

3. **Investigate COUNT fast path**
   - Debug why simple COUNT(*) isn't using O(1) path
   - Ensure `table.row_count()` is O(1)
   - **Expected Impact**: 10-20x improvement on COUNT

### Phase 2: Structural Improvements (High Impact, Medium Effort)

4. **Index-aware WHERE clause execution**
   - Detect `WHERE id = ?` patterns
   - Use primary key index for direct lookup
   - **Expected Impact**: 5-10x improvement on UPDATE/DELETE/SELECT

5. **Batch operation support**
   - Accept arrays of operations in Python bindings
   - Process multiple INSERTs/UPDATEs/DELETEs in one call
   - **Expected Impact**: 3-5x improvement on bulk operations

6. **Reduce unnecessary cloning**
   - Review all `row.clone()` calls
   - Use references where possible
   - **Expected Impact**: 10-30% improvement across the board

### Phase 3: Advanced Optimizations (Medium Impact, High Effort)

7. **Statement caching**
   - Cache parsed statements in Python bindings
   - Avoid re-parsing identical queries
   - **Expected Impact**: 20-40% improvement on repeated queries

8. **Memory pooling**
   - Reuse allocations for common operations
   - Reduce allocator pressure
   - **Expected Impact**: 10-20% improvement

## Testing & Validation

### Benchmark Suite

**Location**: `benchmarks/`

**Quick Benchmarks** (without DuckDB):
```bash
cd benchmarks
./quick_benchmark.sh
# or
pytest --no-duckdb --benchmark-only
```

**Full Benchmarks** (with DuckDB):
```bash
cd benchmarks
pytest --benchmark-only
```

**Profiling Script**:
```bash
cd benchmarks
python profile_count.py
```

### Performance Regression Detection

All optimizations must:
1. ✅ Pass existing test suite (`cargo test`)
2. ✅ Show improvement in benchmarks
3. ✅ Not regress other operations
4. ✅ Maintain SQL correctness

## Conclusion

We've made significant progress with the HashMap optimization (5-7x improvement), but substantial gaps remain. The top priorities are:

1. **UPDATE performance** - 35x gap is unacceptable
2. **COUNT performance** - 45x gap despite optimizations
3. **Schema caching** - Easy win for UPDATE/DELETE

With focused optimization on these areas, we can likely close the gap to **2-3x slower** than SQLite for most operations, which would be acceptable given the trade-offs of SQL:1999 compliance and educational focus.

---

**Next Steps**: Implement Phase 1 optimizations (schema caching and FK check skip).
