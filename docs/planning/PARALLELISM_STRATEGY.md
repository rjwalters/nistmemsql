# VibeSQL Parallelism Strategy: Heuristic-Based Automatic Execution

**TL;DR**: Modern computers have 8+ cores. We should use them automatically, not require manual configuration.

---

## Core Philosophy

### Problem with Current Approach

**Current**: `PARALLEL_EXECUTION=true` environment variable
- ❌ Requires manual opt-in
- ❌ All-or-nothing (can't tune per-operation)
- ❌ Doesn't adapt to hardware
- ❌ No intelligence about query characteristics

### New Approach: Smart Heuristics

**Proposed**: Automatic parallelism based on query characteristics and hardware
- ✅ **Zero configuration** for 99% of users
- ✅ **Hardware-aware**: Auto-detects core count
- ✅ **Operation-specific** thresholds (scans, joins, aggregates)
- ✅ **Query-aware**: Considers entire query, not just individual operations
- ✅ **Override-friendly**: `PARALLEL_THRESHOLD` for power users

---

## Decision Logic

### Hardware Detection

```rust
let num_threads = rayon::current_num_threads();

match num_threads {
    1 => "Never parallelize",
    2..=3 => "Conservative thresholds (20k+ rows)",
    4..=7 => "Moderate thresholds (5k+ rows)",
    8+ => "Aggressive thresholds (2k+ rows)",  // ← Most modern hardware
}
```

**Key insight**: With 8+ cores (laptops, desktops, servers), we have headroom for aggressive parallelism.

### Operation-Specific Thresholds (8+ cores)

| Operation | Threshold | Rationale |
|-----------|-----------|-----------|
| **Scan/Filter** | 2,000 rows | Low overhead, high benefit |
| **Aggregate** | 3,000 rows | Merge cost higher |
| **Sort** | 5,000 rows | Coordination overhead |
| **Join** | 5,000 rows | Complex partitioning |

**Why low thresholds work**:
- Rayon uses thread pools (amortized startup cost)
- Modern CPUs have 8+ cores idle most of the time
- Memory bandwidth is the real bottleneck, not CPU

### Query-Level Heuristics

Look at the **entire query**, not just row counts:

```rust
pub fn should_use_parallelism(query: &SelectStatement) -> bool {
    // Quick exit: tiny queries never benefit
    if estimated_rows < 1_000 { return false; }

    // On 8+ cores, almost always parallelize large queries
    if num_threads >= 8 && estimated_rows >= 10_000 {
        return true;
    }

    // Check specific operations
    if query.has_joins() && estimated_rows >= join_threshold {
        return true;
    }

    if query.has_aggregates() && estimated_rows >= aggregate_threshold {
        return true;
    }

    // Default: use scan threshold
    estimated_rows >= scan_threshold
}
```

**Benefits**:
- Single decision point (not re-evaluated per-operation)
- Considers query complexity (joins, aggregates, subqueries)
- Uses table statistics for row count estimation

---

## Examples

### Example 1: Small Query (Sequential)

```sql
SELECT * FROM users WHERE id = 42;
```

**Decision**: Sequential
- Estimated rows: 1 (index lookup)
- Below minimum threshold (1,000 rows)
- **Overhead > benefit**

### Example 2: Medium Scan (Parallel on 8+ cores)

```sql
SELECT * FROM orders WHERE status = 'pending';
```

**Decision**: Parallel (8+ cores)
- Estimated rows: 5,000
- Above scan threshold (2,000 for 8+ cores)
- **4-6x speedup expected**

### Example 3: Large Join (Parallel)

```sql
SELECT * FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
WHERE o.created_at > '2024-01-01';
```

**Decision**: Parallel
- Estimated rows: 50,000 × 10,000 = 500M potential combinations
- Has join: check join threshold (5,000)
- **5-8x speedup expected**

### Example 4: Complex Aggregation (Parallel)

```sql
SELECT category, COUNT(*), AVG(price)
FROM products
GROUP BY category
HAVING COUNT(*) > 10;
```

**Decision**: Parallel
- Estimated rows: 100,000
- Has aggregates: check aggregate threshold (3,000)
- **3-5x speedup expected**

---

## Implementation Sketch

### Phase 1: Heuristic Module

**File**: `crates/vibesql-executor/src/parallel/heuristics.rs`

```rust
use lazy_static::lazy_static;

lazy_static! {
    static ref PARALLEL_CONFIG: ParallelConfig = ParallelConfig::auto_detect();
}

pub struct ParallelConfig {
    pub num_threads: usize,
    pub thresholds: ParallelThresholds,
}

impl ParallelConfig {
    fn auto_detect() -> Self {
        let num_threads = rayon::current_num_threads();

        let thresholds = match num_threads {
            1 => ParallelThresholds::never(),
            2..=3 => ParallelThresholds::conservative(),
            4..=7 => ParallelThresholds::moderate(),
            _ => ParallelThresholds::aggressive(),
        };

        Self { num_threads, thresholds }
    }
}

pub struct ParallelThresholds {
    pub scan: usize,
    pub filter: usize,
    pub join: usize,
    pub aggregate: usize,
    pub sort: usize,
}

impl ParallelThresholds {
    fn aggressive() -> Self {
        Self {
            scan: 2_000,
            filter: 2_000,
            join: 5_000,
            aggregate: 3_000,
            sort: 5_000,
        }
    }

    // ... other configurations
}
```

### Phase 2: Query-Level Decision

**File**: `crates/vibesql-executor/src/select/executor/execute.rs`

```rust
pub fn execute_select(
    query: &SelectStatement,
    db: &Database,
) -> Result<Vec<Row>, ExecutorError> {
    // Analyze query and decide parallelism strategy
    let use_parallelism = should_use_parallelism_for_query(query, db);

    if use_parallelism {
        execute_select_parallel(query, db)
    } else {
        execute_select_sequential(query, db)
    }
}

fn should_use_parallelism_for_query(
    query: &SelectStatement,
    db: &Database,
) -> bool {
    let config = &PARALLEL_CONFIG;

    // Estimate row count from table statistics
    let estimated_rows = query.from_tables()
        .iter()
        .map(|table| db.get_table_row_count(table).unwrap_or(0))
        .sum();

    // Quick exit for tiny queries
    if estimated_rows < 1_000 {
        return false;
    }

    // For 8+ cores, aggressively parallelize large queries
    if config.num_threads >= 8 && estimated_rows >= 10_000 {
        return true;
    }

    // Check operation-specific thresholds
    if query.has_joins() && estimated_rows >= config.thresholds.join {
        return true;
    }

    if query.has_aggregates() && estimated_rows >= config.thresholds.aggregate {
        return true;
    }

    // Default to scan threshold
    estimated_rows >= config.thresholds.scan
}
```

### Phase 3: Override Mechanism

**Environment variable** for testing/tuning:

```bash
# Use custom threshold (applies to all operations)
PARALLEL_THRESHOLD=5000 cargo test

# Disable parallelism entirely
PARALLEL_THRESHOLD=max cargo test

# Super aggressive (for benchmarking)
PARALLEL_THRESHOLD=500 cargo test
```

**Implementation**:
```rust
impl ParallelThresholds {
    fn from_env_or_auto() -> Self {
        if let Ok(threshold_str) = std::env::var("PARALLEL_THRESHOLD") {
            if threshold_str == "max" || threshold_str == "never" {
                return Self::never();
            }

            if let Ok(threshold) = threshold_str.parse::<usize>() {
                return Self::uniform(threshold);
            }
        }

        // Auto-detect based on hardware
        ParallelConfig::auto_detect().thresholds
    }

    fn uniform(threshold: usize) -> Self {
        Self {
            scan: threshold,
            filter: threshold,
            join: threshold,
            aggregate: threshold,
            sort: threshold,
        }
    }
}
```

---

## Benefits of This Approach

### 1. Zero Configuration

**Before**:
```bash
# User has to know about this flag
PARALLEL_EXECUTION=true ./my_app
```

**After**:
```bash
# Just works on 8+ core systems
./my_app
```

### 2. Hardware Adaptive

**Laptop** (4 cores):
- Moderate thresholds (5k+ rows)
- Balances overhead vs speedup

**Desktop** (12 cores):
- Aggressive thresholds (2k+ rows)
- Maximizes parallelism

**Server** (64 cores):
- Very aggressive thresholds
- Uses available hardware

### 3. Query-Aware

**Simple query** (1k rows):
- Sequential (no overhead)

**Complex query** (joins + aggregates, 100k rows):
- Parallel (big win)

**Medium query** (10k rows, no joins):
- Parallel on 8+ cores
- Sequential on 2-4 cores

### 4. Future-Proof

As hardware evolves (more cores), thresholds automatically adjust:
- 2025: 8-12 cores common → aggressive thresholds
- 2030: 32+ cores common → very aggressive thresholds
- Code doesn't change, just runs faster

---

## Performance Expectations (8+ Cores)

| Query Type | Rows | Cores | Sequential | Parallel | Speedup |
|------------|------|-------|------------|----------|---------|
| Simple scan | 1k | 8 | 1ms | 1ms | 1.0x (sequential) |
| Medium scan | 10k | 8 | 10ms | 2.5ms | **4.0x** |
| Large scan | 1M | 8 | 1s | 150ms | **6.7x** |
| Hash join | 100k×100k | 8 | 2.5s | 500ms | **5.0x** |
| GROUP BY | 1M rows, 10k groups | 8 | 800ms | 200ms | **4.0x** |

**Key takeaway**: On modern 8+ core hardware, **most analytical queries will be 4-6x faster** with automatic parallelism.

---

## Risks and Mitigations

### Risk 1: Overhead on Small Queries

**Mitigation**: Hard floor at 1,000 rows (never parallelize below this)

### Risk 2: Memory Bloat

**Mitigation**: Memory-aware execution (Phase 3 of roadmap)

### Risk 3: Wrong Heuristics

**Mitigation**:
- Conservative defaults (prefer correctness over performance)
- Override mechanism (`PARALLEL_THRESHOLD`)
- Adaptive learning (future: track actual performance)

---

## Comparison to Other Databases

### PostgreSQL

**Approach**: Manual configuration + query planner hints
```sql
SET max_parallel_workers_per_gather = 4;
```

**Pros**: Fine-grained control
**Cons**: Requires tuning, doesn't adapt to hardware

### DuckDB

**Approach**: Automatic parallelism with morsel-driven execution
- Always uses all available cores
- No configuration needed

**Pros**: Zero config, excellent performance
**Cons**: Can over-parallelize small queries

### SQLite

**Approach**: Single-threaded by design
**Pros**: Simple, predictable
**Cons**: Can't leverage modern hardware

### VibeSQL (Proposed)

**Approach**: Heuristic-based automatic parallelism
- Detects hardware (core count)
- Analyzes query (row count, operations)
- Automatically enables parallelism when beneficial

**Pros**: Zero config + smart decisions + override mechanism
**Cons**: Heuristics may not always be perfect

---

## Implementation Timeline

### Week 1: Heuristics Infrastructure

1. Create `parallel/heuristics.rs` module
2. Implement hardware detection
3. Define operation-specific thresholds
4. Add override mechanism

**Deliverable**: Working heuristic system

### Week 2: Integration

1. Update executor to use heuristics
2. Replace `PARALLEL_EXECUTION` with automatic detection
3. Add tests for decision logic

**Deliverable**: Automatic parallelism working

### Week 3: Validation

1. Benchmark with automatic heuristics
2. Compare to manual `PARALLEL_EXECUTION=true`
3. Tune thresholds based on measurements

**Deliverable**: Validated performance improvements

---

## Success Criteria

- [ ] **Zero config**: Works automatically on 8+ core systems
- [ ] **Smart**: No parallelism for small queries (<1k rows)
- [ ] **Fast**: 4-6x speedup on large queries (>10k rows)
- [ ] **Override**: `PARALLEL_THRESHOLD` for power users
- [ ] **Tested**: All SQLLogicTest pass with automatic parallelism

---

## Open Questions

1. **Should we track query execution times to refine heuristics?**
   - Pros: Self-improving system
   - Cons: Complexity, storage overhead

2. **Should thresholds be per-table or global?**
   - Currently: Global thresholds
   - Alternative: Per-table statistics

3. **How to handle nested queries?**
   - Currently: Only outer query analyzed
   - Alternative: Recursive analysis

---

## References

- **DuckDB**: Automatic parallelism with morsel-driven execution
- **DataFusion**: Rust-based query engine with configurable parallelism
- **Rayon**: Data parallelism library with work-stealing thread pool

---

**Document Status**: 📋 Strategic Planning

**Related**: `PARALLELISM_ROADMAP.md` (detailed implementation plan)

**Next Steps**: Implement Phase 1.1 (heuristics infrastructure)
