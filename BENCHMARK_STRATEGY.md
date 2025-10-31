# Benchmark Strategy

## Overview

This document describes the performance benchmarking strategy for nistmemsql, including baseline measurement approach, comparison methodology, and future optimization tracking.

## Philosophy

**Current State**: nistmemsql is built for SQL:1999 correctness and compliance, not performance optimization. The project has deliberately prioritized standards conformance over execution speed.

**Benchmark Goals**:
1. **Establish baseline performance** - Understand current performance characteristics without optimization
2. **Quantify trade-offs** - Document the performance cost of SQL:1999 compliance
3. **Track progress** - Measure impact of future optimization work
4. **Normalize hardware** - Use head-to-head comparisons to make results portable
5. **Set realistic expectations** - Help users understand performance profile

**Non-Goals** (for now):
- Competing with production-optimized databases
- Achieving lowest latency or highest throughput
- Micro-optimization of hot paths
- Hardware-specific tuning

## Usage

### Running the Benchmarks

To run the benchmark suite, use the `run_benchmarks.sh` script:

```bash
./scripts/run_benchmarks.sh
```

This script will:

1.  Build the optimized Python bindings for `nistmemsql`.
2.  Install the required Python dependencies.
3.  Run the `pytest-benchmark` suite.
4.  Generate a `benchmark_results.json` file with the results.
5.  Generate a comparison report.

### Interpreting the Results

The benchmark results are saved in `benchmark_results.json`. This file contains detailed information about each benchmark run, including the minimum, maximum, mean, and median execution times.

The `run_benchmarks.sh` script also generates a comparison report that provides a high-level overview of the performance of `nistmemsql` compared to SQLite.
## Comparison Target: MySQL

### Why MySQL?

**Primary Comparison**: MySQL (with in-memory configuration)

**Rationale**:
- **Ubiquitous**: Most widely deployed open-source database
- **Well-understood reference point**: Developers know MySQL performance characteristics
- **Cross-platform**: Available on all major operating systems
- **In-memory capable**: Can be configured for fair comparison (tmpfs or memory engine)
- **Rust-compatible**: Easy to integrate in benchmark harness via mysql crate
- **SQL:1999 baseline**: Partial SQL:1999 support provides interesting comparison

**Alternative Targets** (future consideration):
- **SQLite (:memory:)**: Lighter-weight, also widely understood
- **DuckDB**: Modern in-memory OLAP database, excellent SQL compliance
- **PostgreSQL**: Gold standard for SQL compliance (though not primarily in-memory)

### Head-to-Head Methodology

**Hardware Normalization**:
- Run both databases on same machine simultaneously
- Report results as relative ratios (nistmemsql vs MySQL)
- Example: "Query execution 2.5x slower than MySQL on same hardware"
- Makes results portable across different test environments

**Fair Comparison Criteria**:
- Both databases in-memory mode (no disk I/O)
- Same dataset loaded into both
- Same queries executed against both
- Cold start vs warm cache clearly distinguished
- Memory limits applied equally

## Benchmark Dimensions

### 1. SQL:1999 Compliance vs Performance Trade-off

**Hypothesis**: Higher SQL:1999 compliance may come at performance cost

**Measurements**:
| Metric                     | nistmemsql | MySQL  | Ratio |
|----------------------------|------------|--------|-------|
| SQL:1999 Core Coverage     | 86.6%      | ~XX%   | X.Xx  |
| Simple SELECT (1M rows)    | XX ms      | YY ms  | X.Xx  |
| Complex JOIN (5 tables)    | XX ms      | YY ms  | X.Xx  |
| Aggregate with GROUP BY    | XX ms      | YY ms  | X.Xx  |
| Window functions           | XX ms      | YY ms  | X.Xx  |

**Value**: Shows whether compliance investment affects performance

### 2. Query Execution Performance

**Workload Types**:
- **Point queries**: Single-row lookups by primary key
- **Range scans**: WHERE clauses with comparison operators
- **Aggregations**: SUM, AVG, COUNT with GROUP BY
- **Joins**: INNER, LEFT, RIGHT, FULL, CROSS joins
- **Window functions**: RANK, ROW_NUMBER, aggregates OVER partitions
- **Subqueries**: Correlated and uncorrelated subqueries
- **CTEs**: Common Table Expressions (WITH clauses)

**Dataset Scale**:
- Small: 1K rows (parser/executor overhead dominant)
- Medium: 100K rows (algorithm efficiency matters)
- Large: 1M rows (scalability characteristics)

**Output Format**:
```
Query Type          | Scale | nistmemsql | MySQL | Ratio | Notes
--------------------|-------|------------|-------|-------|------
Point SELECT        | 1K    | X.X ms     | Y ms  | Z.Zx  | -
Range scan (10%)    | 100K  | X.X ms     | Y ms  | Z.Zx  | -
5-table INNER JOIN  | 10K   | X.X ms     | Y ms  | Z.Zx  | -
GROUP BY aggregate  | 100K  | X.X ms     | Y ms  | Z.Zx  | -
Window function     | 100K  | X.X ms     | Y ms  | Z.Zx  | -
```

### 3. Memory Usage Characteristics

**In-Memory Database Focus**:
- Peak memory usage per operation
- Memory overhead per row stored
- Memory efficiency of data structures
- Cache behavior under load

**Measurements**:
| Metric                    | nistmemsql | MySQL | Ratio |
|---------------------------|------------|-------|-------|
| Empty database size       | XX MB      | YY MB | Z.Zx  |
| Memory per 100K rows      | XX MB      | YY MB | Z.Zx  |
| Peak during complex query | XX MB      | YY MB | Z.Zx  |

### 4. SQL Parsing Performance

**Parser Complexity**:
- Simple query parsing (SELECT * FROM t)
- Complex query parsing (multi-table JOINs with subqueries)
- Very complex queries (nested CTEs, window functions, CASE expressions)
- Parser error handling overhead

**Why Separate Parsing**:
- nistmemsql uses Rust parser (potentially different characteristics than MySQL)
- Understand where execution time is spent (parse vs execute)
- Identify optimization opportunities

**Output Format**:
```
Query Complexity | nistmemsql Parse | MySQL Parse | Ratio | Query Example
-----------------|------------------|-------------|-------|---------------
Simple           | X μs             | Y μs        | Z.Zx  | SELECT * FROM t
Medium           | X μs             | Y μs        | Z.Zx  | 3-table JOIN
Complex          | X ms             | Y ms        | Z.Zx  | Nested CTEs + windows
```

### 5. Transaction Performance

**ACID Operations**:
- BEGIN/COMMIT overhead
- ROLLBACK cost
- SAVEPOINT usage
- Concurrent transaction handling (if implemented)

### 6. Standard SQL Feature Availability

**Qualitative Comparison**:
- Which NIST SQL:1999 tests pass in nistmemsql vs MySQL?
- Feature gaps in each database
- Edge case handling differences

**Example Matrix**:
```
Feature                  | nistmemsql | MySQL | Winner
-------------------------|------------|-------|--------
FULL OUTER JOIN          | ✅         | ❌    | nistmemsql
Window functions         | ✅         | ✅    | Tie
Recursive CTEs           | ❌         | ✅    | MySQL
```

## Benchmark Suite Design

### Tier 1: Micro-Benchmarks (Baseline Characterization)

**Purpose**: Understand fundamental performance characteristics

**Scope**:
- Single-table operations (INSERT, SELECT, UPDATE, DELETE)
- Basic aggregations (COUNT, SUM, AVG)
- Simple joins (2-3 tables)
- Data type operations (numeric, string, date/time)

**Implementation**:
- Custom Rust benchmark harness using `criterion` crate
- Small datasets (1K-10K rows)
- Isolated operations
- Warm cache vs cold start

**Example**:
```rust
// benches/micro_benchmarks.rs
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn benchmark_simple_select(c: &mut Criterion) {
    c.bench_function("SELECT * FROM table (1K rows)", |b| {
        b.iter(|| {
            // Execute query against nistmemsql
            // Execute same query against MySQL
            // Compare results
        });
    });
}
```

### Tier 2: TPC-H Inspired Analytical Queries

**Purpose**: Real-world analytical workload simulation

**Rationale**:
- TPC-H is industry-standard OLAP benchmark
- Designed for decision support systems
- Complex queries with joins, aggregations, subqueries
- Well-documented and widely recognized

**Adaptation for In-Memory**:
- Scale Factor 0.01 (10 MB dataset) for initial testing
- Scale Factor 0.1 (100 MB) for medium testing
- Scale Factor 1.0 (1 GB) for large testing

**TPC-H Query Coverage** (22 queries):
- Q1: Aggregate with GROUP BY and WHERE
- Q3: 3-table join with aggregation
- Q5: 5-table join with complex filtering
- Q10: Multi-table join with CASE expressions
- Q15: View creation and join
- ... (all 22 queries)

**Output Format**:
```
TPC-H Query | nistmemsql | MySQL | Ratio | Description
------------|------------|-------|-------|-------------
Q1          | XX ms      | YY ms | Z.Zx  | Aggregate pricing summary
Q3          | XX ms      | YY ms | Z.Zx  | Shipping priority
Q5          | XX ms      | YY ms | Z.Zx  | Local supplier volume
...
Geometric Mean | XX ms   | YY ms | Z.Zx  | Overall performance
```

### Tier 3: NIST SQL:1999 Test Suite Performance

**Purpose**: Measure performance on compliance tests themselves

**Rationale**:
- Already have 739 SQL:1999 conformance tests
- Can measure execution time per test
- Understand if certain features are performance bottlenecks

**Measurements**:
- Total time to run all 739 tests
- Per-feature execution time (E011, E021, F031, etc.)
- Identify slowest test categories

**Example Output**:
```
Feature Code | Test Count | nistmemsql Total | MySQL Total | Ratio
-------------|------------|------------------|-------------|-------
E011         | 50         | XX ms            | YY ms       | Z.Zx
E021         | 30         | XX ms            | YY ms       | Z.Zx
F031         | 40         | XX ms            | YY ms       | Z.Zx
...
TOTAL        | 739        | XX sec           | YY sec      | Z.Zx
```

### Tier 4: Memory Profiling

**Purpose**: Understand memory allocation patterns

**Tools**:
- Rust's built-in allocator stats
- `heaptrack` for detailed allocation tracking
- Custom memory instrumentation

**Measurements**:
- Total allocations per query
- Peak memory usage
- Memory fragmentation
- Allocation hot spots

## Implementation Plan

### Phase 1: Infrastructure Setup (Weeks 1-2)

**Deliverables**:
1. ✅ Benchmark strategy document (this document)
2. ⬜ Benchmark harness Rust project (`benches/` directory)
3. ⬜ MySQL integration via `mysql` crate
4. ⬜ Head-to-head comparison framework
5. ⬜ Result collection and reporting infrastructure

**Tooling**:
- `criterion` crate for micro-benchmarks
- `mysql` crate for MySQL connections
- JSON output format for results
- Automated result comparison scripts

**Directory Structure**:
```
benches/
├── micro_benchmarks.rs      # Tier 1: Basic operations
├── tpch_queries.rs           # Tier 2: TPC-H inspired
├── conformance_perf.rs       # Tier 3: NIST test suite timing
├── memory_profiling.rs       # Tier 4: Memory analysis
└── utils/
    ├── mysql_harness.rs      # MySQL integration
    ├── data_generator.rs     # Test dataset creation
    └── result_formatter.rs   # Output formatting
```

### Phase 2: Baseline Measurement (Weeks 3-4)

**Goal**: Establish current performance baseline

**Tasks**:
1. ⬜ Implement Tier 1 micro-benchmarks
2. ⬜ Run benchmarks on reference hardware
3. ⬜ Collect nistmemsql baseline results
4. ⬜ Collect MySQL baseline results
5. ⬜ Generate initial comparison report
6. ⬜ Document performance characteristics

**Success Criteria**:
- All micro-benchmarks runnable
- Results reproducible (±5% variance)
- Clear ratio comparisons (nistmemsql vs MySQL)
- Baseline report published in `docs/PERFORMANCE_BASELINE.md`

### Phase 3: Analytical Workload Testing (Weeks 5-8)

**Goal**: Understand performance on realistic analytical queries

**Tasks**:
1. ⬜ Implement TPC-H query adaptations (all 22 queries)
2. ⬜ Generate TPC-H dataset at Scale Factor 0.01
3. ⬜ Load data into both nistmemsql and MySQL
4. ⬜ Execute queries and collect timing
5. ⬜ Analyze results and identify bottlenecks
6. ⬜ Document findings in `docs/TPCH_PERFORMANCE.md`

**Success Criteria**:
- All 22 TPC-H queries executable
- Results show where nistmemsql excels vs struggles
- Clear bottleneck identification (parser? executor? join algorithm?)

### Phase 4: Continuous Benchmark Tracking (Ongoing)

**Goal**: Track performance changes over time

**Infrastructure**:
1. ⬜ GitHub Actions workflow for automated benchmarking
2. ⬜ Performance regression detection
3. ⬜ Historical trend tracking
4. ⬜ Performance badge generation (similar to compliance badge)

**CI Integration**:
```yaml
# .github/workflows/benchmarks.yml
name: Performance Benchmarks

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install MySQL
        run: sudo apt-get install -y mysql-server
      - name: Run benchmarks
        run: cargo bench --bench micro_benchmarks
      - name: Compare to baseline
        run: ./scripts/compare_performance.sh
      - name: Generate performance report
        run: ./scripts/generate_performance_report.sh
      - name: Upload results
        uses: actions/upload-artifact@v3
        with:
          name: benchmark-results
          path: target/criterion/
```

## Benchmark Reporting

### Result Format

**JSON Output** (`target/benchmark_results.json`):
```json
{
  "timestamp": "2025-10-30T10:00:00Z",
  "hardware": {
    "cpu": "Apple M1 Pro",
    "memory": "16 GB",
    "os": "macOS 14.6"
  },
  "benchmarks": {
    "micro": {
      "simple_select_1k": {
        "nistmemsql_ms": 1.2,
        "mysql_ms": 0.8,
        "ratio": 1.5
      },
      ...
    },
    "tpch": {
      "q1": {
        "nistmemsql_ms": 45.3,
        "mysql_ms": 32.1,
        "ratio": 1.41
      },
      ...
    }
  },
  "summary": {
    "geometric_mean_ratio": 1.8,
    "median_ratio": 1.6,
    "range": [1.1, 3.2]
  }
}
```

### Performance Report Document

**Location**: `docs/PERFORMANCE_BASELINE.md`

**Structure**:
```markdown
# Performance Baseline Report

**Generated**: 2025-10-30
**nistmemsql Version**: v0.1.0
**MySQL Version**: 8.0.35

## Executive Summary

nistmemsql is currently **1.8x slower** than MySQL on average (geometric mean)
for analytical workloads on identical hardware. Performance ranges from
1.1x slower (simple queries) to 3.2x slower (complex multi-table joins).

**Key Findings**:
- Simple single-table queries: ~1.2x slower (parse overhead)
- Complex joins: ~2.5x slower (join algorithm not optimized)
- Aggregations: ~1.5x slower (reasonable performance)
- Memory usage: ~1.4x higher (data structure overhead)

**Trade-off Assessment**:
- nistmemsql: 86.6% SQL:1999 compliance, 1.8x slower
- MySQL: ~65% SQL:1999 compliance, baseline speed

## Micro-Benchmarks (Tier 1)

[Detailed tables and charts]

## TPC-H Queries (Tier 2)

[Detailed query-by-query analysis]

## Memory Profiling (Tier 4)

[Memory usage analysis]

## Bottleneck Identification

1. **Join Algorithm**: Naive nested loop, no optimization
2. **Parser Overhead**: ~10% of query time on simple queries
3. **Type Coercion**: Frequent runtime type conversions
4. **Memory Allocations**: High allocation rate in executor

## Optimization Opportunities

[Prioritized list of potential improvements]
```

### Performance Badge

**Badge Endpoint**: `badges/performance-ratio.json`

**Format** (shields.io endpoint JSON):
```json
{
  "schemaVersion": 1,
  "label": "vs MySQL",
  "message": "1.8x slower",
  "color": "yellow"
}
```

**Color Scale**:
- Green (<1.5x): Competitive performance
- Yellow (1.5-2.5x): Acceptable trade-off
- Orange (2.5-5x): Significant overhead
- Red (>5x): Performance concern

## Success Criteria

### Phase 1: Infrastructure (Immediate)
1. ⬜ Benchmark harness created and runnable
2. ⬜ MySQL integration working
3. ⬜ Head-to-head comparison framework complete
4. ⬜ Result collection and JSON output working

### Phase 2: Baseline Established
1. ⬜ All micro-benchmarks executed
2. ⬜ nistmemsql vs MySQL ratios calculated
3. ⬜ Performance baseline documented
4. ⬜ Bottlenecks identified

### Phase 3: Analytical Workload Testing
1. ⬜ TPC-H queries adapted and executable
2. ⬜ Performance characteristics understood
3. ⬜ Optimization priorities identified
4. ⬜ TPC-H performance report published

### Phase 4: Continuous Tracking
1. ⬜ CI integration for automated benchmarking
2. ⬜ Performance regression detection
3. ⬜ Historical trend tracking
4. ⬜ Performance badge live on README

## Risks and Mitigations

### Risk 1: Unfair Comparison (MySQL Too Optimized)

**Risk**: MySQL has decades of optimization, comparison may be discouraging

**Mitigations**:
- Frame as "baseline vs optimized" rather than "failure"
- Document SQL:1999 compliance advantages
- Show performance is "acceptable for correctness-first database"
- Compare to other early-stage databases for context

### Risk 2: Hardware Variability

**Risk**: Performance results vary significantly across hardware

**Mitigations**:
- Use head-to-head ratios, not absolute times
- Report hardware specs with all results
- Test on multiple reference platforms (x86, ARM)
- Use consistent test environments in CI

### Risk 3: MySQL Configuration Complexity

**Risk**: Incorrect MySQL setup leads to invalid comparison

**Mitigations**:
- Document exact MySQL configuration used
- Use in-memory storage engine (MEMORY or tmpfs)
- Disable irrelevant features (replication, binary logs)
- Verify MySQL performing reasonably vs known benchmarks

### Risk 4: Benchmark Suite Incompleteness

**Risk**: Miss important workload patterns in benchmark design

**Mitigations**:
- Start with industry-standard TPC-H
- Add micro-benchmarks for specific features
- Solicit community feedback on workload coverage
- Iterate on benchmark suite based on user needs

### Risk 5: Performance Regressions Unnoticed

**Risk**: Code changes inadvertently degrade performance

**Mitigations**:
- Automated CI benchmarking on every commit
- Alert on >10% performance regression
- Require performance justification for known regressions
- Track historical trends over time

## Future Enhancements

### Phase 5: Optimization Work (Post-100% Compliance)

**When**: After achieving 100% SQL:1999 Core compliance

**Focus Areas**:
1. Join algorithm optimization (hash joins, sort-merge joins)
2. Query optimizer implementation (cost-based query planning)
3. Index support (B-tree, hash indexes)
4. Vectorization (SIMD for aggregations)
5. Reduced memory allocations (arena allocators, object pooling)

**Goal**: Reduce performance gap from 1.8x to <1.3x slower than MySQL

### Phase 6: Extended Comparison Matrix

**Additional Databases**:
- SQLite (:memory:)
- DuckDB
- PostgreSQL (with in-memory configuration)

**Expanded Metrics**:
- Concurrency handling (multi-threaded queries)
- Write-heavy workloads (INSERT/UPDATE/DELETE performance)
- Cold start times (database initialization)

### Phase 7: Real-World Application Benchmarks

**Beyond Synthetic**:
- Port real applications to nistmemsql
- Measure end-to-end performance
- User-facing metrics (latency percentiles, throughput)

## References

### Benchmark Standards

1. **TPC-H Specification**: http://www.tpc.org/tpch/
2. **TPC-DS** (Decision Support): http://www.tpc.org/tpcds/
3. **SQLite Speedtest**: https://www.sqlite.org/speed.html

### Rust Benchmarking Tools

4. **Criterion.rs**: https://github.com/bheisler/criterion.rs
5. **cargo-criterion**: https://github.com/bheisler/cargo-criterion

### MySQL Integration

6. **mysql crate**: https://crates.io/crates/mysql
7. **MySQL Performance Tuning**: https://dev.mysql.com/doc/refman/8.0/en/optimization.html

### Performance Analysis

8. **Rust Performance Book**: https://nnethercote.github.io/perf-book/
9. **flamegraph**: https://github.com/flamegraph-rs/flamegraph
10. **heaptrack**: https://github.com/KDE/heaptrack

### Related Work

11. **DuckDB Benchmarks**: https://duckdb.org/why_duckdb#performance
12. **SQLite Benchmarks**: https://www.sqlite.org/fasterthanfs.html

## Next Steps

### Immediate Actions

1. ⬜ Create `benches/` directory structure
2. ⬜ Set up criterion benchmarking harness
3. ⬜ Implement MySQL integration module
4. ⬜ Write first micro-benchmark (simple SELECT)
5. ⬜ Verify head-to-head comparison works

### Week 1 Deliverables

1. ⬜ All Tier 1 micro-benchmarks implemented
2. ⬜ MySQL setup documented
3. ⬜ Initial baseline measurements collected
4. ⬜ JSON result output working

### Month 1 Goals

1. ⬜ Complete Tier 1 and Tier 2 benchmarks
2. ⬜ Generate first performance baseline report
3. ⬜ Identify top 3 performance bottlenecks
4. ⬜ CI integration for automated benchmarking

---

**Document Version**: 1.0
**Last Updated**: 2025-10-30
**Status**: Planning / Not Yet Implemented
