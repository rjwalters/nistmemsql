# TPC-H Q3 Performance Analysis

## Summary

TPC-H Q3 (Shipping Priority Query) has been verified to run without crashing and benchmarked against SQLite and DuckDB. Current performance shows significant room for optimization.

## Benchmark Results (SF 0.01)

| Engine   | Time (median) | vs VibeSQL |
|----------|---------------|------------|
| VibeSQL  | 744.85 ms     | 1.0x       |
| SQLite   | 26.654 ms     | 28.0x faster |
| DuckDB   | 4.8563 ms     | 153.4x faster |

## Query Details

**TPC-H Q3**: Shipping Priority Query
- **Tables**: customer, orders, lineitem (3-way join)
- **Filters**: c_mktsegment, date ranges
- **Aggregation**: SUM(l_extendedprice * (1 - l_discount))
- **Grouping**: l_orderkey, o_orderdate, o_shippriority
- **Output**: Top 10 orders by revenue

## Analysis

### Current Implementation
- Uses general-purpose join executor with hash joins
- Join order optimization enabled (via cost-based optimizer)
- Table statistics computed for join planning

### Performance Gap
The 28x performance gap vs SQLite suggests Q3 would benefit from query-specific optimization similar to Q1 and Q6.

### Monomorphic Plan Approach
Q1 and Q6 achieve SQLite parity through specialized monomorphic execution plans that:
1. Use unchecked typed accessors (no enum matching)
2. Pre-compute column indices
3. Inline filter predicates
4. Optimize aggregation loops

A similar approach for Q3 would require:
1. Specialized 3-table hash join implementation
2. Fused filter + join + aggregate execution
3. Type-specialized group-by hash table
4. Direct column access without bounds checking

## Next Steps

To achieve parity with SQLite (~27ms), Q3 needs a monomorphic plan implementation:

1. **TpchQ3Plan struct**: Pre-computed column indices for all 3 tables
2. **Optimized join execution**: Type-specialized hash joins
3. **Fused aggregation**: Combine join + filter + group-by in single pass
4. **TpchQ3PatternMatcher**: Detect Q3 queries
5. **Integration**: Add to `try_create_tpch_plan()`

## Verification

- ✅ Q3 runs without crashing
- ✅ Returns correct results (10 rows)
- ✅ Benchmarked against SQLite and DuckDB
- ❌ Performance parity not yet achieved (28x gap)

## Testing

Run benchmarks:
```bash
# Quick profiling (60s timeout per query)
./scripts/bench-tpch.sh 60 summary

# Criterion benchmarks with comparison
cargo bench --package vibesql-executor --bench tpch_benchmark \
  --features benchmark-comparison -- q3
```
