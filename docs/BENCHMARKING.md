# Performance Benchmarking

## Overview

VibeSQL includes a comprehensive benchmark framework for comparing performance against SQLite3. This framework helps track performance characteristics, identify regressions, and validate optimization work.

The benchmark framework supports:
- **Dual-engine execution**: Run the same SQL tests on both VibeSQL and SQLite
- **Comprehensive metrics**: Time, memory, query counts, pass rates
- **Multiple output formats**: Console (human-readable), JSON (programmatic), Markdown (documentation)
- **Statistical analysis**: Percentiles (p95, p99), ratios, aggregates

## Quick Start

Run a simple benchmark test:

```bash
# Run benchmark on a small test file
cargo test --test sqllogictest_suite -- --nocapture benchmarks/simple

# Run with specific test file
cargo test --test sqllogictest_benchmark -- select1 --nocapture
```

The output will show a comparison table with performance metrics:

```
┌──────────────────────────────────────────────────────┐
│         VibeSQL vs SQLite Performance Report         │
│                   select1.test                       │
└──────────────────────────────────────────────────────┘

Summary:
┌────────────────┬───────────┬──────────┬─────────┐
│ Metric         │ SQLite    │ VibeSQL  │ Ratio   │
├────────────────┼───────────┼──────────┼─────────┤
│ Total Time     │ 1.23s     │ 2.45s    │ 1.99x   │
│ Peak Memory    │ 10.2 MB   │ 25.5 MB  │ 2.50x   │
│ Queries/Sec    │ 812       │ 408      │ 0.50x   │
│ Pass Rate      │ 100.0%    │ 100.0%   │ +0.0pp  │
└────────────────┴───────────┴──────────┴─────────┘
```

## Test Files

The benchmark framework uses `.slt` (SQLLogicTest) files. Example benchmark files are provided:

### Small Test File (Quick Sanity Check)
`tests/benchmarks/simple.slt` - Fast execution, validates infrastructure
- Basic CRUD operations
- Simple queries
- ~10 queries, completes in <100ms

### Medium Test File (Realistic Workload)
`tests/benchmarks/joins.slt` - Representative of real-world usage
- Multi-table joins
- WHERE clauses with joins
- Aggregations with joins
- ~20 queries, completes in <500ms

### Large Test File (Stress Test)
`tests/benchmarks/aggregates.slt` - Complex operations
- GROUP BY with multiple columns
- Multiple aggregate functions
- Complex calculations
- ~30 queries, completes in <1s

### Full SQLLogicTest Suite
Located in `tests/sqllogictest-files/` - Comprehensive coverage
- Thousands of test files
- Millions of individual queries
- Industry-standard test corpus

## Benchmark Framework API

### Collecting Metrics

The `BenchmarkMetrics` struct tracks execution metrics:

```rust
use sqllogictest::metrics::BenchmarkMetrics;
use std::time::Duration;

let mut metrics = BenchmarkMetrics::new("vibesql");

// Record successful query
metrics.record_success(Duration::from_millis(100));

// Record failed query
metrics.record_failure(Duration::from_millis(50), "error message".to_string());

// Update memory usage
metrics.update_peak_memory(1024 * 1024 * 10); // 10 MB

// Access statistics
println!("Pass rate: {:.2}%", metrics.pass_rate());
println!("QPS: {:.2}", metrics.queries_per_second());
println!("Avg time: {:?}", metrics.average_query_time());
println!("Median time: {:?}", metrics.median_query_time());
```

### Generating Reports

The `ComparisonReport` struct compares two engines:

```rust
use sqllogictest::report::ComparisonReport;

let report = ComparisonReport::new(
    "select1.test",
    sqlite_metrics,
    vibesql_metrics
);

// Console output (human-readable tables)
println!("{}", report.to_console());

// JSON output (for programmatic analysis)
let json = report.to_json().expect("JSON serialization failed");
println!("{}", json);

// Markdown output (for documentation/GitHub)
println!("{}", report.to_markdown());
```

### SQLite Wrapper

The `SqliteDB` wrapper implements the `AsyncDB` trait for SQLite:

```rust
use sqllogictest::Runner;

// Create test runner with SQLite
let mut tester = Runner::new(|| async {
    SqliteDB::new()
});

// Run test script
tester.run_script(r#"
    statement ok
    CREATE TABLE test (x INTEGER, y INTEGER)

    query I
    SELECT COUNT(*) FROM test
    ----
    0
"#).expect("Test should pass");
```

## Interpreting Results

### Performance Metrics

**Time Ratio**: How many times slower/faster than SQLite
- `< 1.0` - Faster than SQLite (good!)
- `1.0-2.0` - Acceptable performance
- `2.0-3.0` - Room for optimization
- `> 3.0` - Performance issue, investigate

**Memory Ratio**: Relative memory usage
- `< 1.0` - Using less memory than SQLite
- `1.0-2.0` - Reasonable memory overhead
- `> 2.0` - High memory usage, may need optimization

**Queries/Sec (QPS) Ratio**: Throughput comparison
- `> 1.0` - Higher throughput than SQLite
- `0.5-1.0` - Comparable throughput
- `< 0.5` - Lower throughput, investigate bottlenecks

**Pass Rate**: Correctness comparison
- `100%` - All queries executed successfully
- `< 100%` - Some queries failed, check errors list

### Percentile Metrics

**Median (p50)**: Middle value, represents typical query
**P95**: 95th percentile, catches slow outliers
**P99**: 99th percentile, identifies worst-case performance

Compare these percentiles between engines to understand latency distribution.

## Performance Goals

Based on the benchmark results, we aim for:

### Core Operations
- **SELECT queries**: Within 2-3x of SQLite
- **INSERT/UPDATE/DELETE**: Within 2x of SQLite
- **Simple aggregations**: Within 2-3x of SQLite
- **Joins**: Within 3x of SQLite (more complex operations)

### Correctness
- **100% conformance** on all supported SQL:1999 features
- **Identical results** to SQLite on standard queries
- **Proper error handling** for unsupported features

### Memory
- **Peak memory**: Within 2-3x of SQLite
- **No memory leaks**: Stable memory usage over time
- **Reasonable overhead**: Account for Rust safety features

## Writing Benchmark Tests

Create `.slt` files in `tests/benchmarks/`:

```sql
# my_benchmark.slt - Description of what this tests

# Setup
statement ok
CREATE TABLE test (id INTEGER, value TEXT)

statement ok
INSERT INTO test VALUES (1, 'hello')

# Benchmark query
query IT
SELECT * FROM test WHERE id = 1
----
1
hello

# Cleanup
statement ok
DROP TABLE test
```

**Best practices**:
- Keep tests focused on specific features
- Include setup and cleanup
- Use realistic data patterns
- Add comments explaining what's being tested
- Start simple, gradually increase complexity

## Running Benchmarks

### Individual Test Files

```bash
# Run specific benchmark
cargo test --test sqllogictest_suite -- benchmarks/simple

# Run with console output
cargo test --test sqllogictest_suite -- benchmarks/joins --nocapture
```

### Benchmark Suite

```bash
# Run all benchmark tests
cargo test --test sqllogictest_suite -- benchmarks/

# Run with JSON output (redirect to file)
cargo test --test sqllogictest_benchmark -- --format json > benchmark_results.json
```

### Integration with Test Infrastructure

The benchmark framework integrates with the existing sqllogictest infrastructure:

```rust
// tests/sqllogictest_benchmark.rs
#[tokio::test]
async fn benchmark_select() {
    let vibesql_metrics = run_test_with_metrics("select1.test", create_vibesql_db).await;
    let sqlite_metrics = run_test_with_metrics("select1.test", create_sqlite_db).await;

    let report = ComparisonReport::new("select1.test", sqlite_metrics, vibesql_metrics);
    println!("{}", report.to_console());
}
```

## Continuous Integration

### CI Integration (Optional)

Add benchmark runs to CI workflow:

```yaml
# .github/workflows/benchmark.yml
name: Performance Benchmarks

on:
  push:
    branches: [main]
  pull_request:

jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: dtolnay/rust-toolchain@stable
      - name: Run benchmarks
        run: |
          cargo test --test sqllogictest_suite -- benchmarks/simple --nocapture
      - name: Archive results
        uses: actions/upload-artifact@v3
        with:
          name: benchmark-results
          path: benchmark_results.json
```

### Performance Tracking

Track performance over time:
- Run benchmarks on every commit to main
- Store results in JSON format
- Compare against historical baselines
- Alert on significant regressions (>20% slower)

### Nightly Full Suite

For comprehensive coverage:
```yaml
# Run full benchmark suite nightly
- cron: '0 0 * * *'  # Midnight daily

jobs:
  full-benchmark:
    steps:
      - name: Run full suite
        run: |
          cargo test --test sqllogictest_suite -- benchmarks/ --nocapture
        timeout-minutes: 60
```

## Troubleshooting

### Benchmark Runs Too Slow

**Issue**: Benchmarks take too long in CI
**Solution**:
- Use small test files (`simple.slt`) for PR checks
- Reserve large test files for nightly runs
- Consider sampling strategy for SQLLogicTest suite

### Inconsistent Results

**Issue**: Benchmark results vary between runs
**Solution**:
- Run multiple iterations and average
- Disable turbo boost / CPU frequency scaling
- Use dedicated CI runners for benchmarking
- Consider using `criterion` for micro-benchmarks

### Memory Measurements

**Issue**: Memory tracking seems inaccurate
**Solution**:
- Memory is measured at specific points
- Use system tools (`valgrind`, `massif`) for deep analysis
- Consider RSS vs heap allocation differences

### SQLite Comparison Differences

**Issue**: Results differ from SQLite
**Solution**:
- Check SQL:1999 vs SQLite semantics differences
- Verify type coercion behavior
- Review NULL handling
- Check for floating point precision issues

## Advanced Topics

### Custom Metrics

Extend `BenchmarkMetrics` for custom tracking:

```rust
pub struct ExtendedMetrics {
    base: BenchmarkMetrics,
    cache_hits: usize,
    cache_misses: usize,
}

impl ExtendedMetrics {
    pub fn cache_hit_rate(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 { 0.0 } else { self.cache_hits as f64 / total as f64 }
    }
}
```

### Query-Level Analysis

Track individual query performance:

```rust
for query in queries {
    let stopwatch = QueryStopwatch::start();
    let result = execute_query(query).await;
    let duration = stopwatch.stop();

    if result.is_ok() {
        metrics.record_success(duration);
    } else {
        metrics.record_failure(duration, result.unwrap_err().to_string());
    }
}
```

### Comparison with Other Databases

Extend the framework for DuckDB, PostgreSQL comparisons:

```rust
pub struct MultiEngineReport {
    test_name: String,
    engines: HashMap<String, BenchmarkMetrics>,
}

impl MultiEngineReport {
    pub fn add_engine(&mut self, name: String, metrics: BenchmarkMetrics) {
        self.engines.insert(name, metrics);
    }

    pub fn to_comparison_table(&self) -> String {
        // Generate table comparing all engines
    }
}
```

## References

- [SQLLogicTest Documentation](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki)
- [SQL:1999 Standard](https://www.iso.org/standard/53681.html)
- [Benchmark Test Files](../tests/benchmarks/)
- [Metrics Module](../tests/sqllogictest/metrics.rs)
- [Report Generator](../tests/sqllogictest/report.rs)
- [SQLite Wrapper](../tests/sqllogictest_sqlite.rs)

## Contributing

When adding benchmarks:

1. Create focused `.slt` files testing specific features
2. Document what the benchmark measures
3. Include setup and cleanup
4. Add to `tests/benchmarks/` directory
5. Update this documentation with findings

For performance optimizations:

1. Run benchmarks before changes (baseline)
2. Implement optimization
3. Run benchmarks after changes
4. Document performance improvement in PR
5. Include benchmark results in PR description

## Future Work

Potential enhancements:
- **Automated regression detection**: Alert on >20% slowdowns
- **Performance trend graphs**: Visualize performance over time
- **Per-feature tracking**: Separate benchmarks for joins, aggregates, subqueries
- **Hardware profiles**: Benchmark on different CPU/memory configurations
- **Comparison with DuckDB/PostgreSQL**: Expand beyond SQLite baseline
- **Micro-benchmarks**: Use `criterion` for specific operation benchmarks
- **Profiling integration**: Automatic flamegraph generation for slow queries
