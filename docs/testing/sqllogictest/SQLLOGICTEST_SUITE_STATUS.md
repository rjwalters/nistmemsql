# SQLLogicTest Suite Status & Testing Guide

**Last Updated**: 2025-11-16
**Current Status**: 51.0% pass rate (321/629 files passing) ✅
**Test Infrastructure**: Stable parallel execution with 8 workers
**Memory Management**: Successfully running with blocklist for memory-intensive tests
**Root Cause of Failures**: Systematic aggregate function handling issues (130/130 aggregate tests failing)

## Quick Start

### Run Tests Locally (Quick)

```bash
# Run a quick smoke test (10 minutes, samples across test categories)
SQLLOGICTEST_TIME_BUDGET=600 cargo test --test sqllogictest_suite

# Test a specific file
cargo test --test sqllogictest_suite -- --exact random/select/slt_good_21.test

# Test a specific category
cargo test --test sqllogictest_suite -- random/select
```

### Run Full Test Suite (Comprehensive)

```bash
# Run full suite with parallel workers (requires 64-core machine)
./scripts/sqllogictest run --parallel --workers 64 --time 3600

# Aggregate results from all workers
python3 scripts/aggregate_worker_results.py /tmp/sqllogictest_results
```

### Store Results in Dogfooding Database

After running tests, save results to VibeSQL:

```bash
# Process latest test results into database
./scripts/process_test_results.py \
  --input target/sqllogictest_results_analysis.json \
  --database target/sqllogictest_results.sql

# Query results
python3 scripts/query_test_results.py --preset progress
python3 scripts/query_test_results.py --preset failed-files
```

---

## How to Use Our Testing Scripts

### 1. `run_parallel_tests.py` - Full Suite Runner

Runs SQLLogicTest files in parallel across multiple worker processes.

```bash
# Basic usage - use all available CPUs
./scripts/sqllogictest run --parallel

# Specify worker count (default: CPU count)
./scripts/sqllogictest run --parallel --workers 64

# Set time limit per worker (default: 3600 seconds)
./scripts/sqllogictest run --parallel --time-budget 1800

# Combined example
./scripts/sqllogictest run --parallel --workers 32 --time-budget 7200
```

**Output files**:
- `/tmp/sqllogictest_results/worker_*.log` - Individual worker logs
- `/tmp/sqllogictest_results/worker_*_analysis.json` - Per-worker analysis
- `target/sqllogictest_cumulative.json` - Aggregated results

### 2. `aggregate_worker_results.py` - Result Aggregator

Combines results from all parallel workers into single analysis.

```bash
# Aggregate results
python3 scripts/aggregate_worker_results.py /tmp/sqllogictest_results

# Specify output file
python3 scripts/aggregate_worker_results.py \
  /tmp/sqllogictest_results \
  --output target/my_results.json
```

**Output files**:
- `target/sqllogictest_cumulative.json` - All results combined
- `target/sqllogictest_results_analysis.json` - Detailed failure analysis
- `target/failure_pattern_analysis.md` - Human-readable failure patterns

### 3. `process_test_results.py` - Dogfooding Database

Stores test results in VibeSQL, demonstrating real-world database usage.

```bash
# After running tests and aggregating results
./scripts/process_test_results.py \
  --input target/sqllogictest_results_analysis.json \
  --database target/sqllogictest_results.sql \
  --schema scripts/schema/test_results.sql
```

**Features**:
- Creates database from schema on first run
- Loads existing database state
- Records test run metadata (timestamp, git commit, stats)
- Inserts individual test results with error messages
- Updates test file status
- Exports SQL dump for version control

**Output files**:
- `target/sqllogictest_results.sql` - Full database dump
- Can be loaded into web demo for live querying

### 4. `query_test_results.py` - Results Viewer

Query the dogfooding database to analyze test results.

```bash
# View recent progress
python3 scripts/query_test_results.py --preset progress

# Find failed files
python3 scripts/query_test_results.py --preset failed-files

# Get statistics by category
python3 scripts/query_test_results.py --preset by-category

# Run custom query
python3 scripts/query_test_results.py \
  --query "SELECT file_path, status FROM test_files WHERE category = 'random'"
```

---

## Dogfooding Database: VibeSQL Storing Its Own Test Results

We store our test results in VibeSQL itself, demonstrating real-world database usage.

### Schema Overview

The dogfooding database has three tables:

#### `test_files` - Current Status

Tracks the current status of each SQLLogicTest file.

| Column | Type | Notes |
|--------|------|-------|
| file_path | VARCHAR(500) | Primary key, stable identifier |
| category | VARCHAR(50) | "index", "random", "evidence", etc. |
| subcategory | VARCHAR(50) | Detailed category breakdown |
| status | VARCHAR(20) | 'PASS', 'FAIL', 'UNTESTED' |
| last_tested | TIMESTAMP | When last executed |
| last_passed | TIMESTAMP | When last passed (NULL if never) |

#### `test_runs` - Execution History

Metadata for each test run, enabling progress tracking.

| Column | Type | Notes |
|--------|------|-------|
| run_id | INTEGER | Primary key, surrogate ID |
| started_at | TIMESTAMP | Test run start time |
| completed_at | TIMESTAMP | Test run completion time |
| total_files | INTEGER | Files in this run |
| passed | INTEGER | Number of passing files |
| failed | INTEGER | Number of failing files |
| untested | INTEGER | Number of untested files |
| git_commit | VARCHAR(40) | Link to specific code version |
| ci_run_id | VARCHAR(100) | CI/CD system correlation |

#### `test_results` - Detailed Results

Individual test result for each file in each run.

| Column | Type | Notes |
|--------|------|-------|
| result_id | INTEGER | Primary key |
| run_id | INTEGER | Foreign key to test_runs |
| file_path | VARCHAR(500) | Foreign key to test_files |
| status | VARCHAR(20) | 'PASS', 'FAIL' |
| tested_at | TIMESTAMP | Execution timestamp |
| duration_ms | INTEGER | Test execution time |
| error_message | VARCHAR(2000) | Failure details (truncated) |

### Example Queries

Once data is in the dogfooding database, you can analyze it with SQL:

```sql
-- Current status summary by category
SELECT
    category,
    COUNT(*) as total,
    SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) as passed,
    SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) as failed,
    ROUND(100.0 * SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) / COUNT(*), 1) as pass_rate
FROM test_files
GROUP BY category
ORDER BY category;

-- Progress over time (recent 30 runs)
SELECT
    run_id,
    started_at,
    passed,
    failed,
    ROUND(100.0 * passed / total_files, 1) as pass_rate
FROM test_runs
WHERE completed_at IS NOT NULL
ORDER BY completed_at DESC
LIMIT 30;

-- Most problematic files (failed multiple times)
SELECT
    file_path,
    COUNT(*) as failure_count,
    category
FROM test_results
WHERE status = 'FAIL'
GROUP BY file_path
HAVING COUNT(*) > 3
ORDER BY failure_count DESC
LIMIT 20;

-- Recent failures with details
SELECT
    tf.file_path,
    tf.category,
    tr.error_message,
    tr.tested_at
FROM test_results tr
JOIN test_files tf ON tr.file_path = tf.file_path
WHERE tr.status = 'FAIL'
ORDER BY tr.tested_at DESC
LIMIT 50;
```

---

## Testing Workflow: From Execution to Analysis

### Step 1: Run Full Test Suite

```bash
# Run with parallel workers (1-2 hours depending on machine)
./scripts/sqllogictest run --parallel --workers 64
```

**What happens**:
- Tests divided among 64 worker processes
- Each worker runs test files in priority order
- Results logged to `/tmp/sqllogictest_results/worker_*.log`
- Individual analysis per worker in `worker_*_analysis.json`

**Monitoring**:
```bash
# Watch progress in real-time
tail -f /tmp/sqllogictest_results/worker_0.log
tail -f /tmp/sqllogictest_results/worker_1.log

# Check for hangs (CPU spikes)
watch -n 1 'ps aux | grep sqllogictest'
```

### Step 2: Aggregate Results

```bash
# Combine worker results
python3 scripts/aggregate_worker_results.py /tmp/sqllogictest_results
```

**Output**:
- `target/sqllogictest_cumulative.json` - Raw aggregated results
- `target/sqllogictest_results_analysis.json` - Detailed analysis with error messages
- `target/failure_pattern_analysis.md` - Human-readable summary

### Step 3: Store in Dogfooding Database

```bash
# Load results into VibeSQL
./scripts/process_test_results.py \
  --input target/sqllogictest_results_analysis.json \
  --database target/sqllogictest_results.sql
```

**Creates/Updates**:
- Database schema (if first run)
- test_runs entry with metadata
- test_results entries for each file
- test_files status update

### Step 4: Query and Analyze

```bash
# View progress
python3 scripts/query_test_results.py --preset progress

# Find specific failures
python3 scripts/query_test_results.py --preset failed-files

# Custom query
python3 scripts/query_test_results.py \
  --query "SELECT category, COUNT(*) FROM test_files WHERE status='FAIL' GROUP BY category"
```

---

## Current Test Results (as of 2025-11-16)

### ✅ Major Progress: 51.0% Pass Rate Achieved

**Overall Statistics**

| Metric | Value |
|--------|-------|
| Total Files | 629 |
| Passing | 321 (51.0%) |
| Failing | 308 (49.0%) |
| Blocklisted | 4 (memory-intensive) |

### Pass Rate by Category

| Category | Total | Passed | Failed | Pass Rate | Status |
|----------|-------|--------|--------|-----------|--------|
| **ddl** | 1 | 1 | 0 | 100.0% | ✅ Perfect |
| **evidence** | 12 | 12 | 0 | 100.0% | ✅ Perfect |
| **index** | 214 | 203 | 11 | 94.9% | ✅ Excellent |
| **other** | 11 | 3 | 8 | 27.3% | 🔴 Needs work |
| **random** | 391 | 102 | 289 | 26.1% | 🔴 Needs work |

### Index Tests: Detailed Breakdown (94.9% pass rate)

| Subcategory | Total | Passed | Failed | Pass Rate |
|-------------|-------|--------|--------|-----------|
| view | 16 | 16 | 0 | 100% ✅ |
| delete | 14 | 14 | 0 | 100% ✅ |
| between | 13 | 13 | 0 | 100% ✅ |
| in | 13 | 13 | 0 | 100% ✅ |
| orderby_nosort | 49 | 48 | 1 | 98.0% ✅ |
| orderby | 31 | 30 | 1 | 96.8% ✅ |
| commute | 52 | 50 | 2 | 96.2% ✅ |
| random | 26 | 19 | 7 | 73.1% ⚠️ |

### Random Tests: Detailed Breakdown (26.1% pass rate)

| Subcategory | Total | Passed | Failed | Pass Rate |
|-------------|-------|--------|--------|-----------|
| **expr** | 120 | 99 | 21 | 82.5% 🟡 |
| **select** | 127 | 2 | 125 | 1.6% 🔴 |
| **groupby** | 14 | 1 | 13 | 7.1% 🔴 |
| **aggregates** | 130 | 0 | 130 | 0.0% 🔴 |

### Root Cause Analysis

The failure pattern clearly indicates **systematic aggregate function handling issues**:

1. ✅ **Index lookups work excellently** (94.9%) - B-tree implementation solid
2. ✅ **Simple expressions work well** (82.5%) - Expression evaluator functional
3. 🔴 **ALL aggregate tests fail** (0% for 130 tests) - Aggregate bug systematic
4. 🔴 **Groupby nearly all fail** (7.1%) - Likely related to aggregate handling
5. 🔴 **Complex SELECT nearly all fail** (1.6%) - Likely uses aggregates

**Key Insight**: This is NOT a random bug or memory issue. It's a **specific, systematic problem with aggregate function handling** affecting ~289 tests.

### Blocklist Strategy

Successfully managing memory by blocklisting memory-intensive tests:
- `select4.test`, `select5.test` - Very large SELECT tests
- All `/10000/` pattern files - Tests with 10,000+ rows

**Result**: Stable test execution with 8 parallel workers, no memory crashes.

---

## Recommended Next Steps

### Priority 1: Fix Aggregate Function Handling

**Impact**: Would unlock ~130 tests (21% improvement)

The single biggest issue preventing higher pass rates is aggregate function handling:
- All 130 random/aggregates tests fail (0%)
- Nearly all groupby tests fail (7.1%)
- Many SELECT tests likely fail due to aggregate issues (1.6%)

**Action Items**:
1. Investigate aggregate function implementation in query executor
2. Debug specific failing test to identify root cause
3. Fix aggregate computation/result formatting
4. Verify with random/aggregates test suite

### Priority 2: Fix Complex SELECT Queries

**Impact**: Would unlock ~125 tests (20% improvement)

After aggregates are fixed, focus on complex SELECT:
- random/select: 2/127 passing (1.6%)
- Likely involves JOINs, subqueries, or complex expressions

### Priority 3: Polish Index Edge Cases

**Impact**: Would unlock ~11 tests (2% improvement)

Index tests are already excellent (94.9%), remaining failures:
- index/random: 7 failures
- index/commute/1000: 2 failures
- index/orderby/1000: 1 failure
- index/orderby_nosort/1000: 1 failure

### Priority 4: Fix "Other" Category

**Impact**: Would unlock ~8 tests (1% improvement)

Small category with mixed issues:
- select4.test, select5.test - Blocklisted for memory
- Custom vibesql/ tests - Multicolumn IN issues
- tests/issue-1929 - Known aggregate bug

---

## Detailed Testing Guide: Running Your Own Tests

### Local Testing (Development)

For quick feedback while implementing fixes:

```bash
# Test one specific file
cargo test --test sqllogictest_suite -- --exact random/select/slt_good_21.test

# Test all files in a category
cargo test --test sqllogictest_suite -- index/commute

# Test with output (see what passed/failed)
RUST_LOG=info cargo test --test sqllogictest_suite -- --nocapture

# Test with time budget (stop after 10 minutes)
SQLLOGICTEST_TIME_BUDGET=600 cargo test --test sqllogictest_suite
```

### Remote Testing (Full Suite)

For comprehensive testing on high-core-count machines:

```bash
# Compile in release mode first
cargo build --release --test sqllogictest_suite

# Run with 64 workers (best for high-core machines)
./scripts/sqllogictest run --parallel --workers 64 --time 3600

# Monitor progress
watch -n 5 'ls /tmp/sqllogictest_results/worker_*.log | wc -l'
watch -n 5 'grep -h "Passed" /tmp/sqllogictest_results/worker_*.log | tail -1'
```

### Debugging Test Failures

When a test fails, investigate it:

```bash
# Run just the failing test with output
cargo test --test sqllogictest_suite -- \
  --nocapture \
  --exact index/orderby_nosort/10/slt_good_16.test

# Get the test file content
cat third_party/sqllogictest/test/index/orderby_nosort/10/slt_good_16.test | head -50

# Extract and test specific SQL queries
# (Look for "query" lines in the test file)
```

### Measuring Progress After Fixes

After implementing a fix, measure its impact:

```bash
# 1. Run quick smoke test
SQLLOGICTEST_TIME_BUDGET=600 cargo test --test sqllogictest_suite

# 2. Run full test suite on remote (if making major changes)
./scripts/sqllogictest run --parallel --workers 64

# 3. Aggregate and analyze
python3 scripts/aggregate_worker_results.py /tmp/sqllogictest_results

# 4. Store in dogfooding database
./scripts/process_test_results.py \
  --input target/sqllogictest_results_analysis.json

# 5. Compare with previous run
python3 scripts/query_test_results.py --preset progress
```

---

## File Organization

### Key Documentation
- `SQLLOGICTEST_ROADMAP.md` - High-level roadmap with priority matrix
- `TESTING.md` - Testing strategy and methodology
- `SQLLOGICTEST_ISSUES.md` - Known issues and investigation notes

### Test Scripts
- `scripts/run_parallel_tests.py` - Run full test suite with workers
- `scripts/aggregate_worker_results.py` - Combine worker results
- `scripts/analyze_test_failures.py` - Analyze failures with clustering and pattern detection
- `scripts/process_test_results.py` - Store results in dogfooding database
- `scripts/query_test_results.py` - Query dogfooding database

### Data Files
- `target/sqllogictest_cumulative.json` - Raw aggregated results
- `target/sqllogictest_results_analysis.json` - Detailed analysis
- `target/failure_pattern_analysis.md` - Pattern summary
- `target/sqllogictest_results.sql` - Dogfooding database dump

### Test Suite
- `tests/sqllogictest_suite.rs` - Test runner harness
- `third_party/sqllogictest/test/` - 623 test files
- `scripts/schema/test_results.sql` - Dogfooding database schema

---

## Troubleshooting

### Tests Hang or Timeout - CURRENT BLOCKER

**Symptom**: All tests timeout after 60 seconds. No test files complete successfully.

**Current Status**: 
- Reported in SQLLOGICTEST_ISSUES.md as critical infinite loop issue
- All 8 workers (2025-11-08 test run) hung at 60-second mark
- Every test reported as failed with timeout
- Previous run (2025-11-06) had partial success but also experienced this

**Immediate Next Steps**:
1. Profile hanging test to identify infinite loop location
2. Add per-query timeout to test harness
3. Bisect test suite to find problematic SQL
4. Fix infinite loop before retesting

**Known problematic tests** (from SQLLOGICTEST_ISSUES.md):
- `index/commute/10/slt_good_31.test` - Known infinite loop trigger

**Solution**:
```bash
# Kill hanging tests
pkill -9 cargo
pkill -9 sqllogictest

# Check which test causes issues
grep -h "Last test\|Starting test\|Running\|Error" /tmp/sqllogictest_results/worker_*.log | tail -20

# Profile a specific test
timeout 30 cargo test --release --test sqllogictest_suite -- --exact <test_name> --nocapture
```

### Results Don't Aggregate Properly

**Symptom**: Missing worker results, incomplete aggregation

**Solution**:
```bash
# Check for worker crashes
ls /tmp/sqllogictest_results/worker_*_analysis.json | wc -l
# Should equal number of workers used

# Check for errors in logs
grep -h "Error\|ERROR" /tmp/sqllogictest_results/worker_*.log
```

### Database Dump Won't Load

**Symptom**: SQL syntax errors, constraint violations

**Solution**:
```bash
# Recreate from scratch
rm target/sqllogictest_results.sql

# Re-process test results
./scripts/process_test_results.py \
  --input target/sqllogictest_results_analysis.json \
  --database target/sqllogictest_results.sql \
  --schema scripts/schema/test_results.sql
```

---

## References

- [SQLLogicTest Suite](https://github.com/duckdb/sqllogictest)
- [Roadmap](./SQLLOGICTEST_ROADMAP.md) - Target improvements and priorities
- [Strategy](./TESTING.md) - Testing methodology and approach
- [Known Issues](./SQLLOGICTEST_ISSUES.md) - Documented bugs and blockers

---

## Status History

| Date | Pass Rate | Tests | Notes |
|------|-----------|-------|-------|
| 2025-11-16 | 51.0% | 321/629 | ✅ Stable infrastructure, aggregate bug identified |
| 2025-11-08 | 0% | 0/403 | ⚠️ All tests timeout (REGRESSION) |
| 2025-11-06 | 13.5% | 83/613 | Baseline measurement |

---

**Last Updated**: 2025-11-16
**Maintainer**: VibeSQL Team
**Status**: STABLE - Infrastructure solid, aggregate functions are primary blocker (#1841 closed)
