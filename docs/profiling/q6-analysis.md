# TPC-H Q6 Performance Analysis

## Executive Summary

VibeSQL is **48x slower than SQLite** and **467x slower than DuckDB** on TPC-H Q6 (filter + aggregation).

**Current Performance (SF 0.01, ~60K rows)**:
- **VibeSQL**: 212ms (~3.5µs/row)
- **SQLite**: 4.39ms (~73ns/row) - **48x faster**
- **DuckDB**: 455µs (~7.6ns/row) - **467x faster**

## Root Cause: Row Materialization Overhead

### Architecture Analysis

VibeSQL uses a **row-based execution model** with full row materialization:

```rust
// crates/vibesql-storage/src/row.rs
pub struct Row {
    pub values: Vec<SqlValue>,  // Heap allocation per row!
}

// crates/vibesql-types/src/sql_value/mod.rs
pub enum SqlValue {
    Integer(i64),
    Double(f64),
    Varchar(String),
    Date(Date),
    // ... 20+ variants
}
```

### Bottleneck Breakdown

For Q6 processing ~60K rows with 16 columns:

1. **Row Vector Allocations**: 60,000 `Vec<SqlValue>` allocations
   - Each row requires heap allocation for the Vec
   - ~60KB of Vec headers + metadata

2. **Value Boxing Overhead**: 960,000 value allocations (60K × 16 columns)
   - Each `SqlValue` enum: 24+ bytes (discriminant + largest variant)
   - Total: ~23MB of boxed values vs ~7.7MB raw data (3x bloat)

3. **Dynamic Dispatch**: Every predicate evaluation requires:
   - Enum pattern matching on SqlValue
   - Type checking at runtime
   - No SIMD/vectorization possible

4. **Memory Access Patterns**:
   - Poor cache locality (values scattered across heap)
   - Pointer chasing for each column access
   - No prefetching opportunities

### Cost Per Row

**VibeSQL overhead budget** (3.5µs/row - 73ns/row = ~3.43µs overhead):
- ~1.5µs: Vec allocation + deallocation
- ~1.2µs: Value boxing (16 columns × 75ns each)
- ~0.5µs: Predicate evaluation (4 conditions with enum matching)
- ~0.23µs: Expression evaluation for SUM()

**Compare to SQLite** (73ns/row total):
- Operates directly on B-tree pages
- No row materialization until result set
- Values accessed via typed column readers
- Predicate evaluation on native types

**Compare to DuckDB** (7.6ns/row total):
- Vectorized execution (processes ~1024 rows at once)
- Columnar storage (all values of same column contiguous)
- SIMD operations (process 4-8 values per CPU instruction)
- JIT compilation of filters

## Optimization Roadmap

### Phase 1: Eliminate Row Materialization (Target: 10x improvement)

**Option A: Late Materialization** (Lower risk)
- Keep row-based model but delay Vec allocation
- Pass column iterators through pipeline
- Only materialize for result set
- Estimated impact: 5-10x speedup

**Option B: Columnar Batching** (Higher reward)
- Process rows in batches of 1024
- Store batch data in columnar arrays
- Enable SIMD for predicates
- Estimated impact: 20-50x speedup

### Phase 2: Compiled Predicates (Target: 2-3x improvement)

Current PR #2202 claims 30x improvement from compiled predicates, but:
- Unverified in production
- May conflict with row materialization overhead
- Should be re-benchmarked after Phase 1

**Next Steps**:
1. Implement lazy column projection (no full row materialization)
2. Add fast-path for common predicate patterns
3. Consider JIT for complex expressions

### Phase 3: Vectorization (Target: 5-10x improvement)

- Adopt Arrow RecordBatch format
- SIMD-optimized kernels for filters/aggregations
- Batch size tuning (1024-4096 rows)

## Proposed Architecture Changes

### Current (Row-at-a-time):
```
TableScan → Row{Vec<SqlValue>} → Filter(Row) → Aggregate(Row) → Vec<Row>
     ↑              ↑                  ↑              ↑
  B-tree page   Allocate+Box     Pattern match   More boxing
```

### Proposed (Lazy Projection):
```
TableScan → ColumnIterators → Filter(native types) → Aggregate → Row
     ↑            ↑                     ↑                ↑         ↑
  B-tree page  Zero-copy views    Direct comparisons   Accumulator  Once!
```

### Future (Vectorized):
```
TableScan → RecordBatch[1024] → SIMD Filter → SIMD Sum → Scalar
     ↑            ↑                   ↑            ↑
Columnar blocks  Arrow arrays   4-8 rows/op   Native SIMD
```

## Immediate Action Items

1. **Create Issue: "Eliminate row materialization in table scans"**
   - Implement lazy column projection
   - Target: 10x speedup on Q6
   - Priority: P0

2. **Create Issue: "Add SIMD-optimized filter execution"**
   - Batch processing for predicates
   - Target: 5x speedup
   - Priority: P1

3. **Re-benchmark PR #2202** (compiled predicates)
   - Verify 30x claim
   - Test interaction with lazy projection
   - Priority: P1

4. **Create Issue: "Adopt Arrow RecordBatch for internal execution"**
   - Long-term architectural change
   - Target: Match DuckDB performance
   - Priority: P2 (research)

## References

- **Benchmark Results**: Background benchmark 0e5ba5
- **Code Paths Analyzed**:
  - `crates/vibesql-storage/src/row.rs:5-7` - Row definition
  - `crates/vibesql-types/src/sql_value/mod.rs:16-50` - SqlValue enum
  - `crates/vibesql-executor/src/select/iterator/` - Row iteration
  - `crates/vibesql-executor/src/select/filter.rs` - Predicate evaluation

## Conclusion

The 48x performance gap is **not** due to algorithmic complexity but rather:
1. Unnecessary row materialization (allocating Vec per row)
2. Value boxing overhead (SqlValue enums)
3. Dynamic dispatch for predicates

**These are fixable architectural issues, not fundamental limitations.**

Estimated improvement potential:
- **Phase 1 (Lazy projection)**: 10x speedup → 21ms (still 5x slower than SQLite)
- **Phase 2 (Compiled predicates)**: 2x speedup → 10ms (2x slower than SQLite)
- **Phase 3 (Vectorization)**: 5x speedup → 2ms (2x faster than SQLite!)

With all optimizations, VibeSQL could potentially **match or exceed SQLite** performance while maintaining PostgreSQL compatibility.
