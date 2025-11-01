# Performance Optimization Guide

This document outlines performance optimization opportunities for the NIST-compatible SQL:1999 database implementation.

## Current Performance Profile

### Design Philosophy
Per ADR-0001, this database prioritizes **correctness over performance**:
- No performance requirements
- Single-threaded execution
- In-memory storage
- Focus on SQL:1999 standard compliance

### Current State
- **SQL:1999 Conformance**: 95.1% (703/739 tests passing)
- **Test Pass Rate**: 100% (1,306+ tests)
- **Optimization Level**: Zero (by design)
- **Target Workload**: Small datasets (<100K rows), educational/research use

### Performance Characteristics

| Operation | Implementation | Complexity | Notes |
|-----------|----------------|-----------|-------|
| **Table Scan** | Linear Vec iteration | O(n) | No indexes |
| **INSERT** | Vec append + constraint check | O(k*m) | k=constraints, m=table rows |
| **UPDATE** | Linear scan + mutation | O(n*k) | Full table scan for WHERE |
| **DELETE** | Vec retain predicate | O(n) | Full table scan |
| **JOIN** | Nested loop | O(n*m) | No optimization, no indexing |
| **GROUP BY** | HashMap accumulation | O(n) | Single pass grouping |
| **ORDER BY** | Vec sort | O(n log n) | Standard Rust sort |
| **Constraint Check** | Linear scan | O(n) | Full table scan per INSERT/UPDATE |

## Identified Bottlenecks

### 1. Nested Loop Joins (Critical)

**Location**: `crates/executor/src/select/join.rs`

**Issue**: O(n*m) complexity for all joins, including equi-joins
```rust
// Current implementation
pub fn nested_loop_inner_join(left: FromResult, right: FromResult) -> Result<FromResult> {
    let mut result_rows = Vec::new();
    for left_row in &left.rows {           // O(n)
        for right_row in &right.rows {     // O(m)
            // Cartesian product, then filter
            if condition_matches {
                result_rows.push(combined_row);
            }
        }
    }
    Ok(FromResult { rows: result_rows })
}
```

**Impact**:
- 10,000 × 10,000 row join = 100,000,000 comparisons
- Becomes unusable beyond ~50K rows per table

---

### 2. Constraint Validation (Critical)

**Location**: `crates/executor/src/insert.rs`

**Issue**: Full table scan on every INSERT/UPDATE to check PRIMARY KEY and UNIQUE constraints
```rust
// Current implementation
fn check_unique_constraint(&self, table: &Table, row: &Row, col_idx: usize) -> Result<()> {
    for existing_row in &table.rows {  // O(n) scan
        if existing_row.values[col_idx] == row.values[col_idx] {
            return Err(ExecutorError::UniqueConstraintViolation);
        }
    }
    Ok(())
}
```

**Impact**:
- Inserting 10,000 rows with UNIQUE constraint = ~50,000,000 comparisons
- Bulk inserts become O(n²)

---

### 3. Row Value Cloning (High Impact)

**Locations**:
- `crates/executor/src/select/join.rs`
- `crates/executor/src/evaluator/window/*`
- `crates/executor/src/select/cte.rs`

**Issue**: 462 `clone()` calls throughout codebase
```rust
// Join implementation clones every row value
let mut combined_values = left_row.values.clone();  // Clone #1
combined_values.extend(right_row.values.clone());   // Clone #2

// Window functions clone entire partitions
let partition: Vec<Row> = rows
    .iter()
    .map(|row| row.clone())  // Clone every row in partition
    .collect();
```

**Impact**:
- Memory overhead: 2-3x actual data size
- CPU overhead: Unnecessary allocations and copies
- Cache pollution: Reduced locality of reference

---

### 4. String-Based Numeric Types (Medium Impact)

**Location**: `crates/types/src/lib.rs`

**Issue**: NUMERIC, DECIMAL, DATE, TIME, TIMESTAMP use `String` representation
```rust
pub enum SqlValue {
    Numeric(String),      // "123.45"
    Date(String),         // "2024-10-30"
    Time(String),         // "12:30:45"
    Timestamp(String),    // "2024-10-30 12:30:45"
    Interval(String),     // "1 day 2 hours"
}
```

**Impact**:
- Arithmetic requires: string parse → float conversion → calculate → format back
- ~1μs per operation vs ~10ns for native types (100x slower)
- No precision guarantees (float rounding errors)

---

### 5. No Expression Optimization (Medium Impact)

**Location**: `crates/executor/src/evaluator/core.rs`

**Issue**: No constant folding, no subquery caching, no expression simplification
```rust
// SELECT 1+1, name FROM users WHERE 2+2=4
// Current behavior:
// - Evaluates 1+1 for EVERY row in users table
// - Evaluates 2+2=4 for EVERY row in WHERE clause
// - No detection that these are constants
```

**Impact**:
- Redundant calculations on every row
- Subqueries re-executed multiple times
- No optimization across query execution

---

## Optimization Opportunities

### High-Impact, Low-Effort (Priority 1)

#### 1.1 Hash Indexes for Constraints

**Effort**: 1 day
**Impact**: 100-1000x speedup for INSERT/UPDATE with UNIQUE/PRIMARY KEY constraints

**Implementation**:
```rust
// crates/storage/src/table.rs
pub struct Table {
    pub schema: catalog::TableSchema,
    rows: Vec<Row>,

    // NEW: Index structures
    primary_key_index: Option<HashMap<Vec<SqlValue>, usize>>,  // Composite key → row index
    unique_indexes: HashMap<usize, HashMap<SqlValue, usize>>,   // Col index → value → row index
}

impl Table {
    pub fn insert(&mut self, row: Row) -> Result<()> {
        // Check PRIMARY KEY uniqueness: O(1) instead of O(n)
        if let Some(pk_cols) = &self.schema.primary_key {
            let pk_values: Vec<SqlValue> = pk_cols.iter()
                .map(|&idx| row.values[idx].clone())
                .collect();

            if self.primary_key_index.as_ref().unwrap().contains_key(&pk_values) {
                return Err(Error::PrimaryKeyViolation);
            }

            // Insert into index
            self.primary_key_index.as_mut().unwrap().insert(pk_values, self.rows.len());
        }

        // Similar for UNIQUE constraints
        self.rows.push(row);
        Ok(())
    }
}
```

**Files to modify**:
- `crates/storage/src/table.rs` - Add index fields and maintenance
- `crates/executor/src/insert.rs` - Use indexes for constraint checks
- `crates/executor/src/update.rs` - Update indexes on modification
- `crates/executor/src/delete.rs` - Remove from indexes on deletion

**Benefits**:
- INSERT with constraint validation: O(n) → O(1)
- Bulk insert of 10,000 rows: ~100x faster

---

#### 1.2 Hash Join for Equi-Joins

**Effort**: 2 days
**Impact**: 100-10,000x speedup for equi-join queries (most common JOIN type)

**Implementation**:
```rust
// crates/executor/src/select/join.rs

pub fn hash_join_inner(
    left: FromResult,
    right: FromResult,
    left_col: usize,
    right_col: usize,
) -> Result<FromResult> {
    // Build phase: Hash smaller table (O(n))
    let (build_side, probe_side, build_col, probe_col) =
        if left.rows.len() <= right.rows.len() {
            (left, right, left_col, right_col)
        } else {
            (right, left, right_col, left_col)
        };

    let mut hash_table: HashMap<SqlValue, Vec<Row>> = HashMap::new();
    for row in build_side.rows {
        let key = row.values[build_col].clone();
        hash_table.entry(key).or_default().push(row);
    }

    // Probe phase: Look up matches (O(m))
    let mut result_rows = Vec::new();
    for probe_row in probe_side.rows {
        let key = &probe_row.values[probe_col];
        if let Some(build_rows) = hash_table.get(key) {
            for build_row in build_rows {
                let combined = combine_rows(&probe_row, build_row);
                result_rows.push(combined);
            }
        }
    }

    Ok(FromResult { schema, rows: result_rows })
}
```

**Strategy**:
1. Detect equi-join conditions (e.g., `t1.id = t2.id`)
2. Use hash join for equi-joins: O(n + m)
3. Fall back to nested loop for complex predicates: O(n*m)

**Files to modify**:
- `crates/executor/src/select/join.rs` - Add hash join implementation
- Add join condition analyzer to detect equi-join predicates

**Benefits**:
- 10,000 × 10,000 row equi-join: 100M comparisons → 20K operations (5,000x faster)
- Memory overhead: ~1.5x smaller table size (acceptable)

---

#### 1.3 Reduce Row Cloning

**Effort**: 1 day
**Impact**: 50-70% reduction in memory allocations, 20-30% query speedup

**Implementation Strategy**:
```rust
// Use references throughout execution pipeline
// Clone only for final output

// Before: Clone at every step
fn execute_join(left: FromResult, right: FromResult) -> FromResult {
    let combined = left_row.values.clone();  // ❌ Clone
    combined.extend(right_row.values.clone()); // ❌ Clone
}

// After: Use references until final materialization
struct RowRef<'a> {
    left: Option<&'a Row>,
    right: Option<&'a Row>,
}

impl<'a> RowRef<'a> {
    fn get_value(&self, col_idx: usize) -> &SqlValue {
        // Resolve column to correct side without cloning
    }

    fn materialize(&self) -> Row {
        // Clone only once, at end of pipeline
    }
}
```

**Areas to target**:
1. JOIN operations (45+ clones in `join.rs`)
2. Window function partitioning (cloning entire partitions)
3. CTE result materialization
4. Projection operations in SELECT

**Files to modify**:
- `crates/executor/src/select/join.rs`
- `crates/executor/src/evaluator/window/*`
- `crates/executor/src/select/projection.rs`

---

#### 1.4 Expression Constant Folding

**Effort**: 0.5 days
**Impact**: Eliminate redundant calculations, 10-50% speedup for constant-heavy queries

**Implementation**:
```rust
// crates/executor/src/evaluator/optimizer.rs (NEW FILE)

pub fn fold_constants(expr: &ast::Expression) -> ast::Expression {
    match expr {
        // Literal arithmetic
        ast::Expression::BinaryOp {
            op: ast::BinaryOperator::Plus,
            left: box ast::Expression::Literal(Literal::Integer(a)),
            right: box ast::Expression::Literal(Literal::Integer(b)),
        } => ast::Expression::Literal(Literal::Integer(a + b)),

        // Constant comparisons
        ast::Expression::BinaryOp {
            op: ast::BinaryOperator::Equals,
            left: box ast::Expression::Literal(a),
            right: box ast::Expression::Literal(b),
        } => ast::Expression::Literal(Literal::Boolean(a == b)),

        // Recursively fold nested expressions
        ast::Expression::BinaryOp { op, left, right } => {
            ast::Expression::BinaryOp {
                op: *op,
                left: Box::new(fold_constants(left)),
                right: Box::new(fold_constants(right)),
            }
        }

        // Pass through other expressions
        other => other.clone(),
    }
}
```

**Usage**:
```rust
// In SelectExecutor::execute()
let optimized_where = fold_constants(&select.where_clause);
let optimized_select_list = select.select_list.iter()
    .map(|expr| fold_constants(expr))
    .collect();
```

**Files to modify**:
- Create `crates/executor/src/evaluator/optimizer.rs`
- Integrate into `crates/executor/src/select/executor/execute.rs`

**Benefits**:
- `SELECT 1+1, name FROM users` - calculate `1+1` once, not per row
- `WHERE 2+2=4` - evaluate to `WHERE true` before execution
- Subquery result caching (phase 2)

---

### Medium-Impact Optimizations (Priority 2)

#### 2.1 Native Decimal Type

**Effort**: 2 days
**Impact**: 10-100x faster numeric calculations, exact precision

**Implementation**:
```rust
// crates/types/src/lib.rs

// Add dependency in Cargo.toml:
// rust_decimal = "1.33"

use rust_decimal::Decimal;

pub enum SqlValue {
    // Before: Numeric(String)
    // After:
    Numeric(Decimal),  // 128-bit fixed-point arithmetic
    Decimal(Decimal),

    // Keep other types
    Integer(i64),
    Float(f32),
    Double(f64),
    // ...
}
```

**Benefits**:
- Exact precision (no float rounding errors)
- Native arithmetic operations (no string parsing)
- Benchmark: ~100x faster than string-based calculations
- Standards-compliant precision handling

**Considerations**:
- Breaking change to SqlValue enum
- Must update all arithmetic operators in evaluator
- Update display formatting for backward compatibility

---

#### 2.2 Predicate Pushdown

**Effort**: 2 days
**Impact**: 50-90% reduction in intermediate result sizes

**Implementation**:
```rust
// crates/executor/src/select/optimizer.rs (NEW)

pub fn pushdown_predicates(
    from_clause: &ast::FromClause,
    where_clause: &Option<ast::Expression>,
) -> (ast::FromClause, Option<ast::Expression>) {
    // Extract table-specific predicates from WHERE
    let mut table_predicates: HashMap<String, Vec<ast::Expression>> = HashMap::new();
    let mut remaining_predicates = Vec::new();

    if let Some(where_expr) = where_clause {
        for predicate in split_and_predicates(where_expr) {
            if let Some(table) = get_single_table_reference(predicate) {
                // Can push down (references only one table)
                table_predicates.entry(table).or_default().push(predicate);
            } else {
                // Must evaluate after join
                remaining_predicates.push(predicate);
            }
        }
    }

    // Apply predicates to table scans
    let optimized_from = apply_pushed_predicates(from_clause, table_predicates);
    let optimized_where = combine_predicates(remaining_predicates);

    (optimized_from, optimized_where)
}
```

**Example**:
```sql
-- Before optimization:
SELECT * FROM users JOIN orders ON users.id = orders.user_id
WHERE users.active = true AND orders.total > 100;

-- After pushdown:
SELECT * FROM
  (SELECT * FROM users WHERE active = true) u  -- Filter early
JOIN
  (SELECT * FROM orders WHERE total > 100) o   -- Filter early
ON u.id = o.user_id;
```

**Benefits**:
- Reduce JOIN input sizes by filtering first
- Often 50-90% fewer rows in intermediate results
- Compound with hash join for massive speedups

---

#### 2.3 Query Plan Caching

**Effort**: 1 day
**Impact**: 10-100x speedup for repeated queries (common in applications)

**Implementation**:
```rust
// crates/executor/src/plan_cache.rs (NEW)

use lru::LruCache;

pub struct QueryPlanCache {
    cache: LruCache<String, Arc<ExecutionPlan>>,
}

impl QueryPlanCache {
    pub fn get_or_create(&mut self, sql: &str) -> Result<Arc<ExecutionPlan>> {
        if let Some(plan) = self.cache.get(sql) {
            return Ok(Arc::clone(plan));
        }

        // Parse and optimize query
        let ast = parser::parse(sql)?;
        let optimized = optimizer::optimize(ast)?;
        let plan = Arc::new(ExecutionPlan::from_ast(optimized));

        self.cache.put(sql.to_string(), Arc::clone(&plan));
        Ok(plan)
    }
}
```

**Usage**:
```rust
// In Database
pub struct Database {
    catalog: Catalog,
    tables: HashMap<String, Table>,
    plan_cache: QueryPlanCache,  // NEW
}

impl Database {
    pub fn execute(&mut self, sql: &str) -> Result<Vec<Row>> {
        let plan = self.plan_cache.get_or_create(sql)?;
        self.execute_plan(&plan)
    }
}
```

**Benefits**:
- Amortize parsing cost across multiple executions
- Perfect for web applications (same queries, different parameters)
- Prepare/execute pattern support

---

### Advanced Optimizations (Priority 3)

#### 3.1 B-Tree Indexes for Range Queries

**Effort**: 3-5 days
**Impact**: Enable efficient range scans, ORDER BY optimization

- Implement B-Tree index structure
- Support range predicates: `WHERE age > 21 AND age < 65`
- Integrate with ORDER BY to avoid sorting

---

#### 3.2 Join Reordering

**Effort**: 3-4 days
**Impact**: Optimal join order based on selectivity

- Collect table statistics (row counts, distinct values)
- Cost-based optimizer to choose join order
- Heuristics: smaller tables first, most selective predicates first

---

#### 3.3 Parallel Query Execution

**Effort**: 1-2 weeks
**Impact**: 2-8x speedup on multi-core systems (requires architectural changes)

- Thread-safe storage layer
- Parallel scan operators
- Parallel aggregation
- Requires careful design to avoid data races

---

#### 3.4 Columnar Storage for Analytics

**Effort**: 2-3 weeks
**Impact**: 10-100x speedup for analytical queries (OLAP)

- Store columns separately instead of row-oriented
- Better compression ratios
- SIMD-friendly data layout
- Major architectural change

---

## Performance Testing Strategy

### Benchmark Suite

Create comprehensive benchmarks before optimizing:

```rust
// benches/performance.rs

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};

fn bench_join_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("join");
    for size in [100, 1_000, 10_000, 100_000] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let db = setup_tables(size);
            b.iter(|| {
                db.execute(black_box(
                    "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id"
                ))
            });
        });
    }
    group.finish();
}

fn bench_insert_with_constraints(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_unique");
    for rows in [100, 1_000, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, &rows| {
            b.iter_batched(
                || setup_table_with_unique(*rows),
                |mut db| db.execute("INSERT INTO users VALUES (99999, 'test')"),
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_complex_query(c: &mut Criterion) {
    c.bench_function("tpc-h_q1", |b| {
        let db = setup_tpch_lineitem(1_000);
        b.iter(|| {
            db.execute(black_box(
                "SELECT l_returnflag, l_linestatus, \
                 SUM(l_quantity) as sum_qty, \
                 COUNT(*) as count_order \
                 FROM lineitem \
                 WHERE l_shipdate <= '1998-12-01' \
                 GROUP BY l_returnflag, l_linestatus \
                 ORDER BY l_returnflag, l_linestatus"
            ))
        });
    });
}

criterion_group!(benches, bench_join_sizes, bench_insert_with_constraints, bench_complex_query);
criterion_main!(benches);
```

### Profiling Setup

```bash
# CPU profiling with flamegraph
cargo install flamegraph
cargo flamegraph --bench performance

# Memory profiling
cargo install cargo-instruments
cargo instruments -t alloc --bench performance

# Detailed profiling with perf (Linux)
perf record -g --call-graph=dwarf cargo bench
perf report
```

### Test Data Generators

```rust
// tests/helpers/data_generator.rs

pub fn generate_table(rows: usize, columns: usize) -> Table {
    let mut table = Table::new(schema);
    for i in 0..rows {
        let row = Row {
            values: (0..columns).map(|_| random_value()).collect(),
        };
        table.insert(row).unwrap();
    }
    table
}

pub fn generate_tpch_lineitem(scale_factor: usize) -> Table {
    // Generate TPC-H lineitem table for realistic benchmarks
}
```

---

## Recommended Implementation Roadmap

### Phase 1: Foundation (Week 1)

**Goal**: Establish performance measurement infrastructure

1. **Day 1-2**: Create benchmark suite
   - Basic operation benchmarks (scan, insert, join)
   - Varying dataset sizes (100, 1K, 10K, 100K rows)
   - Baseline measurements

2. **Day 3-4**: Implement hash indexes for constraints
   - PRIMARY KEY index
   - UNIQUE constraint indexes
   - Update insert/update/delete logic

3. **Day 5**: Validate and measure
   - Ensure all 1,306 tests still pass
   - Measure benchmark improvements
   - Profile memory usage

**Expected outcome**: 100-1000x INSERT/UPDATE speedup with constraints

---

### Phase 2: Query Optimization (Week 2)

**Goal**: Optimize query execution

1. **Day 1-2**: Implement hash join
   - Detect equi-join conditions
   - Build hash join implementation
   - Integrate with existing join logic

2. **Day 3**: Reduce cloning overhead
   - Analyze clone hotspots with profiler
   - Implement reference-based execution where possible
   - Measure memory reduction

3. **Day 4**: Expression optimization
   - Constant folding implementation
   - Dead code elimination (WHERE false)
   - Integration with executor

4. **Day 5**: Measure and validate
   - Run full benchmark suite
   - Ensure all tests pass
   - Document performance improvements

**Expected outcome**: 10-100x JOIN speedup, 50% memory reduction

---

### Phase 3: Advanced Features (Week 3-4)

**Goal**: Production-ready optimizations

1. **Week 3**: Native types and plan caching
   - Replace string-based NUMERIC with Decimal
   - Implement query plan cache
   - Update all arithmetic operations

2. **Week 4**: Query planner
   - Predicate pushdown
   - Simple cost model
   - Join reordering heuristics

**Expected outcome**: 2-10x additional speedup for complex queries

---

## Success Metrics

### Performance Targets

| Workload | Current | Target (Phase 1) | Target (Phase 2) | Target (Phase 3) |
|----------|---------|------------------|------------------|------------------|
| **INSERT 10K rows (w/ UNIQUE)** | ~50s | ~0.5s (100x) | ~0.3s | ~0.1s |
| **10K × 10K equi-join** | ~60s | ~50s | ~0.5s (100x) | ~0.2s |
| **Complex GROUP BY (10K rows)** | ~2s | ~1.5s | ~0.5s | ~0.2s |
| **Memory usage (10K row join)** | ~100MB | ~80MB | ~40MB (2x) | ~30MB |

### Quality Metrics

- **Test pass rate**: Maintain 100% (1,306+ tests)
- **Code coverage**: Maintain >85%
- **Compiler warnings**: Remain at 0
- **Clippy warnings**: Remain at 0

---

## Non-Goals

The following optimizations are **explicitly out of scope** for maintaining the educational/research focus:

1. **Disk-based storage** - Remain in-memory only
2. **Multi-threaded execution** - Keep single-threaded for simplicity
3. **Network protocol** - Local execution only
4. **Distributed query execution** - Single-node only
5. **MVCC concurrency control** - Single-transaction model sufficient

---

## Appendix: Performance Analysis Tools

### Recommended Profiling Tools

```bash
# Criterion benchmarking
cargo install cargo-criterion
cargo criterion

# Flamegraph visualization
cargo install flamegraph
cargo flamegraph --bench <benchmark-name>

# Memory profiling (macOS)
cargo install cargo-instruments
cargo instruments -t alloc --bench <benchmark-name>

# Detailed profiling (Linux)
perf record -g cargo bench
perf report

# Valgrind cachegrind (Linux)
valgrind --tool=cachegrind target/release/nistmemsql
```

### Sample Profiling Session

```bash
# 1. Establish baseline
cargo bench --bench join_performance > baseline.txt

# 2. Implement optimization
# ... code changes ...

# 3. Measure improvement
cargo bench --bench join_performance > optimized.txt

# 4. Compare results
cargo install critcmp
critcmp baseline.txt optimized.txt

# 5. Profile hot paths
cargo flamegraph --bench join_performance

# 6. Analyze memory
cargo instruments -t alloc --bench join_performance --open
```

---

## Document History

- **2024-10-30**: Initial document created based on comprehensive architecture analysis
- **2025-10-31**: Quality metrics (issues 768-769) confirmed maintained at required levels
- **2025-10-31**: Created issue #770 for Phase 1 optimization: hash indexes for constraints
- **Version**: 1.1
- **Status**: Phase 1 optimization planning initiated

---

**Note**: All performance numbers are estimates based on algorithmic complexity analysis. Actual improvements will vary based on workload characteristics and should be validated with benchmarks.
