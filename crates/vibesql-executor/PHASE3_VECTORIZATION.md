# Phase 3: Vectorized Execution with SIMD (Design Document)

## Overview

This document outlines the design for Phase 3 vectorized execution using Apache Arrow RecordBatches and SIMD instructions.

**Status**: Foundation work in progress
**Target**: 5-10x performance improvement for TPC-H queries
**Depends on**: Phase 1 (lazy projection) and Phase 2 (compiled predicates)

## Architecture

### Current State (Post Phase 1+2)
```
TableScan → Row Iterator → Filter (row-at-a-time) → Aggregate (row-at-a-time) → Result
                                   ↓                         ↓
                            ~10ms for Q6            Still 2x slower than SQLite
```

### Target State (Phase 3)
```
TableScan → RecordBatch[1024 rows] → SIMD Filter → SIMD Aggregate → Result
     ↑              ↑                      ↑              ↑
Columnar     Arrow arrays           4-8 rows/op    Native SIMD
```

## Key Components

### 1. RecordBatch Adapter (`batch.rs`)

Converts vibesql's row-based format to Arrow's columnar RecordBatch format:

```rust
pub struct BatchIterator {
    row_iter: TableScanIterator,
    batch_size: usize,  // Default: 1024 rows
    column_names: Vec<String>,
}

pub fn rows_to_record_batch(
    rows: &[Row],
    column_names: &[String],
) -> Result<RecordBatch>
```

**Benefits**:
- Columnar layout enables SIMD operations
- Better cache utilization (data locality)
- Batch overhead amortized over many rows

### 2. SIMD Filter Kernel (`filter.rs`)

Evaluates WHERE clauses using Arrow compute kernels with SIMD:

```rust
pub fn filter_record_batch_simd(
    batch: &RecordBatch,
    predicate: &Expression,
) -> Result<RecordBatch>
```

**SIMD Operations**:
- `arrow::compute::eq()` - equality comparison (4-8 values per instruction)
- `arrow::compute::gt()` - greater than
- `arrow::compute::lt()` - less than
- `arrow::compute::and()` - logical AND
- `arrow::compute::or()` - logical OR

**Performance**: 5-10x faster than row-at-a-time for numeric predicates

### 3. SIMD Aggregation (`aggregate.rs`)

Computes aggregates using Arrow's SIMD-accelerated kernels:

```rust
pub enum AggregateFunction {
    Sum,
    Avg,
    Count,
    Min,
    Max,
}

pub fn aggregate_column_simd(
    batch: &RecordBatch,
    column_name: &str,
    function: AggregateFunction,
) -> Result<SqlValue>
```

**SIMD Kernels**:
- `arrow::compute::sum()` - vectorized summation
- `arrow::compute::min/max()` - parallel min/max
- Column-oriented processing for better cache hits

## Implementation Plan

### Step 1: Add Arrow Dependencies ✅

```toml
arrow = "53.0"
arrow-array = "53.0"
arrow-select = "53.0"
```

### Step 2: Create RecordBatch Adapter

- [x] Design BatchIterator interface
- [ ] Implement row → columnar conversion
- [ ] Handle all vibesql SqlValue types
- [ ] Test round-trip conversion

### Step 3: SIMD Filter Kernel

- [x] Design filter interface
- [ ] Map Expression AST to Arrow compute ops
- [ ] Implement comparison operators (=, <, >, !=, <=, >=)
- [ ] Implement logical operators (AND, OR, NOT)
- [ ] Benchmark vs row-at-a-time

### Step 4: SIMD Aggregation

- [x] Design aggregation interface
- [ ] Implement SUM, AVG, COUNT, MIN, MAX
- [ ] Handle grouped aggregations
- [ ] Integrate with existing GROUP BY logic

### Step 5: Integration

- [ ] Modify SelectExecutor to use vectorized path
- [ ] Add heuristics for vectorization (dataset size, query type)
- [ ] Fallback to row-based for unsupported operations
- [ ] Benchmark full TPC-H suite

### Step 6: Tuning

- [x] **Adaptive Batch Sizing** - Query-aware batch size selection
  - `SCAN_BATCH_SIZE` (4096): Larger batches for pure scans → better SIMD utilization
  - `JOIN_BATCH_SIZE` (512): Smaller batches for joins → less memory pressure
  - `L1_CACHE_BATCH_SIZE` (1024): Cache-optimized for GROUP BY operations
  - `QueryContext` enum to select appropriate batch size based on query type
- [x] **Column Pruning** - Only convert referenced columns to columnar format
  - `rows_to_record_batch_with_columns()` API with column selection
  - Reduces conversion overhead proportional to pruning ratio
  - Smaller RecordBatch footprint improves cache utilization
  - Less memory allocation overhead
- [x] **Predicate Fusion** - Short-circuit evaluation for AND/OR operations
  - AND: Skip right-side evaluation if left mask is all-false
  - OR: Skip right-side evaluation if left mask is all-true
  - Reduces unnecessary SIMD operations for selective predicates
  - Inline helper functions `is_all_false()` and `is_all_true()`
- [ ] Profile memory usage with optimizations
- [ ] Measure actual SIMD utilization improvement
- [ ] Benchmark optimizations on TPC-H queries
- [ ] Compare with SQLite and DuckDB

## Success Criteria

- [ ] Q6 performance: 10ms → ≤2ms (5x improvement)
- [ ] Q1 performance: 30ms → ≤5ms (6x improvement)
- [ ] **Overall: Match or exceed SQLite performance**
- [ ] All TPC-H queries benefit from vectorization
- [ ] Memory usage < 2x increase
- [ ] No correctness regressions

## API Challenges

### SqlValue Type Mapping

vibesql uses `SqlValue` enum which must map to Arrow types:

| vibesql SqlValue | Arrow DataType |
|------------------|----------------|
| Integer(i64) | Int64 |
| Float(f32), Double(f64) | Float64 |
| Varchar(String), Character(String) | Utf8 |
| Boolean(bool) | Boolean |
| Timestamp | Timestamp(Microsecond) |
| Date | Date32 |
| Time | Time64(Microsecond) |
| Numeric(f64) | Float64 (approximate) |

### Expression AST Mapping

vibesql Expression → Arrow compute kernels:

- `Expression::BinaryOp { Equal, .. }` → `arrow::compute::eq()`
- `Expression::BinaryOp { GreaterThan, .. }` → `arrow::compute::gt()`
- `Expression::BinaryOp { And, .. }` → `arrow::compute::and()`
- `Expression::ColumnRef { .. }` → Column lookup by name

## Performance Expectations

### TPC-H Q6 Breakdown

**Current (Post Phase 2)**: ~10ms
- Table scan: 2ms
- Filter evaluation: 6ms (row-at-a-time)
- Aggregation: 2ms (row-at-a-time)

**Target (Phase 3)**: ~2ms (5x improvement)
- Table scan: 2ms (unchanged)
- RecordBatch conversion: 0.5ms
- SIMD filter: 1ms (6x faster: 6ms → 1ms)
- SIMD aggregation: 0.5ms (4x faster: 2ms → 0.5ms)

### Why SIMD Wins

1. **Instruction-level parallelism**: Process 4-8 values per CPU cycle
   - SSE: 128-bit registers (4x f32 or 2x f64)
   - AVX: 256-bit registers (8x f32 or 4x f64)
   - AVX-512: 512-bit registers (16x f32 or 8x f64)

2. **Cache locality**: Columnar data fits better in L1/L2 cache
   - Row-based: 64B cache line holds ~1 row (poor utilization)
   - Columnar: 64B cache line holds 8-16 values (excellent utilization)

3. **Amortized overhead**: Batch processing spreads fixed costs
   - Function call overhead: amortized over 1024 rows
   - Null checks: vectorized with bitmasks
   - Type dispatches: once per batch vs per row

## References

- **Parent Issue**: #2203 (Performance optimization plan)
- **Dependencies**: #2206 (Phase 1 - Lazy projection), #2207 (Phase 2 - Compiled predicates)
- **Arrow Compute**: https://docs.rs/arrow/latest/arrow/compute/
- **DuckDB Vectorization**: https://duckdb.org/2021/08/27/external-sorting.html

## Next Steps

1. Complete API integration (SqlValue ↔ Arrow type mapping)
2. Implement RecordBatch conversion with proper type handling
3. Implement filter and aggregate kernels
4. Add integration tests for correctness
5. Run TPC-H benchmarks to measure actual speedup
6. Iterate based on profiling results

---

**Note**: This is foundational work for Phase 3. Full implementation and benchmarking will follow.
