# TPC-H Q3 Optimization Analysis

## Issue #2337: Further optimization to reach <100ms

### Current Performance
- **Current**: 724ms (SF 0.01) after PR #2336 optimizations
- **Target**: <100ms
- **Gap**: ~7x improvement needed

## Investigation Findings

### Optimizations Already Implemented ✅

After deep code analysis, I found that most suggested optimizations are already in place:

#### 1. Predicate Pushdown During Scan ✅
**Location**: `crates/vibesql-executor/src/select/scan/table.rs:150-176`

Table scans already apply table-local predicates early:
```rust
// Check if we need to apply table-local predicates
if let Some(where_expr) = where_clause {
    let predicate_plan = PredicatePlan::from_where_clause(Some(where_expr), &schema)?;
    if predicate_plan.has_table_filters(table_name) {
        // Try columnar filter optimization first
        if let Some(column_predicates) = extract_column_predicates(where_expr, &schema) {
            let indices = apply_columnar_filter(row_slice, &column_predicates)?;
            // ... use filtered indices
        }
        // Fall back to generic predicate evaluation
        let filtered_rows = apply_table_local_predicates_ref(...)?;
    }
}
```

**Features**:
- Predicates pushed down to scan phase
- Columnar optimization for simple predicates (lines 158-162)
- Generic evaluation fallback for complex expressions
- Applied for regular tables, CTEs, and views

#### 2. Join Order Optimization with Selectivity ✅
**Location**: `crates/vibesql-executor/src/select/scan/reorder.rs:505-526`

Join ordering already considers predicate selectivity:
```rust
// Extract table-local predicates for cardinality estimation
let mut table_local_predicates = if let Some(where_expr) = where_clause {
    extract_table_local_predicates(where_expr, &table_set)
} else {
    HashMap::new()
};

// Use search to find optimal join order (with real statistics + selectivity)
let search = JoinOrderSearch::from_analyzer_with_predicates(
    &analyzer, database, &table_local_predicates
);
let optimal_order = search.find_optimal_order();
```

**Features**:
- Extracts table-local predicates from WHERE clause
- Passes to join order optimizer for cardinality estimation
- Cost-based search considers filtered table sizes
- Already handles Q3's `c_mktsegment = 'BUILDING'` filter

#### 3. Hash Join Build Side Selection ✅
**Location**: `crates/vibesql-executor/src/select/join/hash_join.rs:140-146`

Hash joins always use the smaller table as build side:
```rust
// Choose build and probe sides (build hash table on smaller table)
let (build_rows, probe_rows, build_col_idx, probe_col_idx, left_is_build) =
    if left.rows().len() <= right.rows().len() {
        (left.rows(), right.rows(), left_col_idx, right_col_idx, true)
    } else {
        (right.rows(), left.rows(), right_col_idx, left_col_idx, false)
    };
```

**Features**:
- Automatic build/probe side selection
- Always builds hash table on smaller input
- Parallel hash table construction (when feature enabled)
- O(smaller_table) memory usage

### Optimizations NOT Yet Implemented ❌

Based on my analysis, these would require significant architectural changes:

#### 1. Monomorphic Q3 Plan ❌
**Challenge**: Q1 and Q6 monomorphic plans work on single-table queries. Q3 is a 3-way join.

**Current architecture**:
```rust
pub trait MonomorphicPlan {
    fn execute(&self, rows: &[Row]) -> Result<Vec<Row>, ExecutorError>;
}
```

This interface expects a single table's rows as input. Q3 requires:
- customer table (filtered by c_mktsegment)
- orders table (filtered by o_orderdate)
- lineitem table (filtered by l_shipdate)
- Hash joins between them
- GROUP BY aggregation

**Required changes** for monomorphic Q3:
1. New plan interface that accepts multiple tables
2. Custom join implementation (can't reuse generic hash_join)
3. Inline all predicates and aggregation logic
4. Pattern matcher to detect Q3 queries

**Estimated complexity**: High - requires new plan architecture

#### 2. Columnar Execution for Aggregation ❌
**What it means**: Process columns instead of rows during aggregation phase

**Current**: Row-oriented aggregation
```rust
for row in rows {
    let key = (row[l_orderkey], row[o_orderdate], row[o_shippriority]);
    let revenue = row[l_extendedprice] * (1.0 - row[l_discount]);
    groups.entry(key).or_default().sum += revenue;
}
```

**Columnar approach**:
```rust
// Extract columns
let l_extendedprice: Vec<f64> = ...;
let l_discount: Vec<f64> = ...;

// SIMD-friendly operations
let revenue: Vec<f64> = l_extendedprice
    .iter()
    .zip(&l_discount)
    .map(|(p, d)| p * (1.0 - d))
    .collect();
```

**Estimated complexity**: High - requires columnar storage format

#### 3. SIMD Aggregation ❌
**What it means**: Use SIMD instructions for `SUM(l_extendedprice * (1 - l_discount))`

**Benefits**:
- 4-8x speedup for arithmetic operations
- Requires aligned columnar data

**Estimated complexity**: Medium-High - needs columnar format + SIMD implementation

#### 4. Materialization Reduction ❌
**What it means**: Avoid creating intermediate Row objects between join stages

**Current**: Each join produces `Vec<Row>` output
**Optimized**: Stream tuples through pipeline without materialization

**Estimated complexity**: Very High - requires iterator-based execution model

## Recommendations

### Option 1: Accept Current Performance as Near-Optimal
Given that all basic optimizations are implemented, the current 724ms may be close to optimal for the generic execution architecture.

**Pros**:
- No additional work required
- Architecture remains simple and maintainable

**Cons**:
- Doesn't meet <100ms target
- SQLite/DuckDB may be faster due to SIMD/columnar execution

### Option 2: Implement Simplified Monomorphic Q3 Plan
Focus on the most impactful optimization: specialized execution for Q3 pattern.

**Approach**:
1. Keep generic join infrastructure
2. Add Q3-specific fast path for:
   - Predicate evaluation (inlined, no dispatch)
   - Aggregation (specialized for revenue calculation)
   - Use unchecked typed accessors like Q1/Q6

**Estimated speedup**: 2-3x (may get to ~250ms, still not <100ms)
**Effort**: High
**Risk**: Moderate

### Option 3: Full Columnar/SIMD Implementation
Major architectural change to support columnar execution with SIMD.

**Estimated speedup**: 5-10x (could reach <100ms target)
**Effort**: Very High
**Risk**: High - major architectural change

## Next Steps

**Recommended**: Discuss with team which option aligns with project goals:
- If target is competitive with SQLite/DuckDB → Option 3 (columnar/SIMD)
- If target is reasonable performance improvement → Option 2 (simplified monomorphic Q3)
- If current performance acceptable → Option 1 (close issue)

## Conclusion

The issue's suggested optimizations (#1, #2, #3 from the list) are already implemented. The remaining 7x performance gap requires either:
- Accepting that 724ms is near-optimal for generic architecture, OR
- Investing in major architectural changes (columnar, SIMD, monomorphic multi-table plans)

The <100ms target may not be achievable without fundamental execution model changes.
