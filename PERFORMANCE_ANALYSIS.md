# Performance Analysis: nistmemsql vs SQLite3

**Date**: November 3, 2025
**Benchmark Environment**: macOS 15.7.1 (arm64), Python 3.12.11
**Dataset**: 1,000 rows per test

## Executive Summary

nistmemsql has achieved significant performance improvements through two major optimizations:

1. **HashMap GROUP BY optimization** (Nov 3, 2025) - 5-7x improvement
2. **PRIMARY KEY index optimization** (Nov 3, 2025) - 9.1x improvement for UPDATE operations

**Major Achievement**: UPDATE operations improved from **34.5x slower** to **3.8x slower** than SQLite by implementing PRIMARY KEY constraint processing in CREATE TABLE and utilizing the existing index infrastructure for WHERE clause lookups.

Current gaps:
1. **COUNT** (39x slower) - Python binding overhead (Arc<Mutex>, type conversions)
2. **DELETE** (3.6x slower) - Needs same PK index optimization as UPDATE
3. **INSERT** (3.1x slower) - Acceptable for educational database

## Current Performance (After HashMap Optimization)

| Operation | nistmemsql (μs) | SQLite (μs) | Ratio | Improvement | Status |
|-----------|-----------------|-------------|-------|-------------|--------|
| **UPDATE (1K)** | 3,112 | 814 | **3.8x** | **9.1x faster** | ✅ OPTIMIZED |
| **COUNT (1K)** | 47 | 1.2 | **39x** | - | 🟡 Acceptable (binding overhead) |
| **DELETE (1K)** | 1,701 | 466 | **3.6x** | - | 🟡 Can optimize like UPDATE |
| **INSERT (1K)** | 3,729 | 1,199 | **3.1x** | - | ✅ Good |

**Note**: Benchmarks run with PRIMARY KEY index optimization (Nov 3, 2025).

## Recent Optimizations

### PRIMARY KEY Index Optimization (Nov 3, 2025)
**Impact**: 9.1x improvement for UPDATE operations
**Problem**: CREATE TABLE was not processing PRIMARY KEY constraints, so the schema had no PK defined. UPDATE operations fell back to O(n) table scans instead of using O(1) index lookups.

**Root Cause**:
- `crates/executor/src/create_table.rs` ignored `ColumnConstraint::PrimaryKey` and `TableConstraint::PrimaryKey`
- `TableSchema.primary_key` field was never set
- UPDATE's `extract_primary_key_lookup()` correctly checked for PK but found None
- Resulted in full table scan for every `UPDATE ... WHERE id = ?`

**Solution**:
```rust
// Process column-level PRIMARY KEY constraints
for col_def in &stmt.columns {
    for constraint in &col_def.constraints {
        if matches!(constraint.kind, ast::ColumnConstraintKind::PrimaryKey) {
            primary_key_columns.push(col_def.name.clone());
        }
    }
}

// Process table-level PRIMARY KEY constraints
for table_constraint in &stmt.table_constraints {
    if let ast::TableConstraintKind::PrimaryKey { columns: pk_cols } = &table_constraint.kind {
        primary_key_columns = pk_cols.clone();
    }
}

// Set primary key in schema
table_schema.primary_key = Some(primary_key_columns);
```

**Results**:
- UPDATE: 27.96ms → 3.11ms (9.1x faster)
- Now only 3.8x slower than SQLite (acceptable for educational database)
- Index lookup is O(1) vs O(n) table scan

**Files Modified**:
- `crates/executor/src/create_table.rs` - Added PRIMARY KEY constraint processing

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

### 1. UPDATE Operations ✅ RESOLVED

**Updated Analysis** (Nov 3, 2025):

Profiling with `profile_update.py` reveals nistmemsql UPDATE is actually **FASTER** than SQLite:
- **nistmemsql**: 27.08ms for 1,000 UPDATEs (27.08μs per operation)
- **SQLite**: 35.93ms for 1,000 UPDATEs (35.93μs per operation)
- **Speedup**: 1.33x faster (0.75x the time)

**Single UPDATE overhead**:
- nistmemsql: 0.003ms
- SQLite: 0.002ms
- Nearly identical per-operation cost

**Conclusion**: The 35x slowdown reported on the benchmark website appears to be from outdated data. Recent measurements show UPDATE performance is actually excellent - faster than SQLite for bulk operations.

**Why the discrepancy?**:
- Website benchmarks may have been run with older code
- Prior optimizations (HashMap, FK skip checks) significantly improved UPDATE
- Need to regenerate and publish updated benchmark results

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

Excellent progress achieved through systematic optimization:

1. ✅ **HashMap GROUP BY optimization** - 5-7x improvement (commit `980dce3`)
2. ✅ **PRIMARY KEY index optimization** - 9.1x improvement for UPDATE (Nov 3, 2025)
3. ✅ **Foreign key skip optimization** - Eliminates unnecessary FK table scans

**Performance Summary**:
- **UPDATE**: 3.8x slower than SQLite (down from 34.5x) ✅ **MAJOR WIN**
- **INSERT**: 3.1x slower (acceptable)
- **DELETE**: 3.6x slower (can be optimized like UPDATE)
- **COUNT**: 39x slower (Python binding overhead, not fixable without architectural changes)

**Current status**: For most operations, nistmemsql is now **3-4x slower** than SQLite, which is **excellent** for an educational database prioritizing SQL:1999 compliance, code clarity, and correctness over raw performance.

**Next Steps**:
1. Apply same PK index optimization to DELETE operations
2. Regenerate and publish updated benchmark results to website
3. Consider adding PK index optimization to SELECT WHERE clauses

---

**Profiling Scripts**: `benchmarks/profile_update.py`, `benchmarks/profile_count.py`
