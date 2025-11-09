# SQLLogicTest Suite Status & Testing Guide

**Last Updated**: 2025-11-08 (current run)
**Current Status**: 0% pass rate (0/403 files tested) - REGRESSION DETECTED
**Previous Status**: 13.5% pass rate (83/613 files) as of 2025-11-06
**Coverage**: 64.7% (403/623 files tested in current run)
**Note**: All tests timing out after 60 seconds - timeout regression (#1012)

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
python3 scripts/run_parallel_tests.py --workers 64 --time-budget 3600

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
python3 scripts/run_parallel_tests.py

# Specify worker count (default: CPU count)
python3 scripts/run_parallel_tests.py --workers 64

# Set time limit per worker (default: 3600 seconds)
python3 scripts/run_parallel_tests.py --time-budget 1800

# Combined example
python3 scripts/run_parallel_tests.py --workers 32 --time-budget 7200
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

### 3. `analyze_failure_patterns.py` - Pattern Analysis

Analyzes test failures to identify root causes and high-impact fixes.

```bash
# Analyze failure patterns
python3 scripts/analyze_failure_patterns.py \
  target/sqllogictest_results_analysis.json

# Output: target/failure_pattern_analysis.md
# Groups failures by:
# - Error type frequency
# - Tests affected per issue
# - Estimated impact of fixes
```

### 4. `process_test_results.py` - Dogfooding Database

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

### 5. `query_test_results.py` - Results Viewer

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
python3 scripts/run_parallel_tests.py --workers 64
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

### Step 3: Analyze Failure Patterns

```bash
# (automatically done by aggregator, but can re-run)
python3 scripts/analyze_failure_patterns.py \
  target/sqllogictest_results_analysis.json
```

**Identifies**:
- Top failing categories
- Error type frequencies
- Tests affected by each issue
- Estimated impact of fixes

### Step 4: Store in Dogfooding Database

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

### Step 5: Query and Analyze

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

## Current Test Results (as of 2025-11-08)

### ⚠️ REGRESSION ALERT

All tests are timing out after 60 seconds, causing 100% failure rate. This is a critical issue that prevents accurate metrics.

- **Root Cause**: Infinite loop in test execution (identified in SQLLOGICTEST_ISSUES.md)
- **Last Good Run**: 2025-11-06 with 13.5% pass rate
- **Action Required**: Debug and fix infinite loop before tests can progress

### Overall Statistics (Current Run - INVALID)

| Metric | Value |
|--------|-------|
| Total Files | 623 |
| Tested | 403 (64.7%) |
| Passing | 0 (0%) - TIMEOUT |
| Failing | 403 (100%) - TIMEOUT |
| Untested | 220 (35.3%) |

### Previous Good Statistics (2025-11-06)

| Metric | Value |
|--------|-------|
| Total Files | 623 |
| Tested | 613 (98.4%) |
| Passing | 83 (13.5%) |
| Failing | 530 (86.5%) |
| Untested | 10 (1.6%) |

### Pass Rate by Category (from 2025-11-06)

| Category | Pass Rate | Status |
|----------|-----------|--------|
| index/commute | 95%+ | ✅ Strong |
| index/between | 90%+ | ✅ Strong |
| index/in | 85%+ | ✅ Good |
| evidence | 40%+ | ⚠️ Needs work |
| random | 2-5% | ❌ Weak |

**Note**: Current run shows 0% across all categories due to timeout issue

### Top Failure Categories

| Issue | Tests Affected | Effort | Priority |
|-------|----------------|--------|----------|
| Result hash mismatches | 138 (27.6%) | Medium | P0 |
| Result formatting | 112 (22.4%) | Medium | P0 |
| Decimal formatting | 56 (11.2%) | Low | P0 |
| Parse errors | 42 (8.4%) | High | P1 |
| Column resolution | 39 (7.8%) | Medium | P1 |
| Type mismatches | 29 (5.8%) | High | P2 |
| Multi-row formatting | 19 (3.8%) | Low | P0 |
| Missing functions | 13 (2.6%) | Low | P2 |

---

## Recommended Next Steps

### Short Term: Quick Wins

According to [SQLLOGICTEST_ROADMAP.md](./SQLLOGICTEST_ROADMAP.md), the top 2 quick wins are:

1. **Issue #956**: Decimal formatting (affects 56 tests, low effort)
   - Expected improvement: +8-10% pass rate

2. **Issue #957**: Multi-row formatting (affects 19 tests, low effort)
   - Expected improvement: +3-5% pass rate

Together: 14% → 25% pass rate (+75 passing tests)

### Medium Term: High-Impact Fixes

Issues #959, #960, #958 could take us from 25% to 87% pass rate:

- **#959**: Hash mismatches (138 tests)
- **#960**: General result mismatches (112 tests)
- **#958**: Column alias resolution (39 tests)

### Long Term: Full Compliance

Remaining issues for 87% → 100%:
- Parse error handling
- Missing SQL functions (NULLIF, COALESCE)
- Type coercion edge cases

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
python3 scripts/run_parallel_tests.py --workers 64 --time-budget 3600

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
python3 scripts/run_parallel_tests.py --workers 64

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
- `scripts/analyze_failure_patterns.py` - Identify failure patterns
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

| Date | Pass Rate | Tests | Coverage | Notes |
|------|-----------|-------|----------|-------|
| 2025-11-08 | 0% | 0/403 | 64.7% | ⚠️ All tests timeout at 60s - REGRESSION |
| 2025-11-06 | 13.5% | 83/613 | 98.4% | Baseline - infinite loop identified |

---

**Last Updated**: 2025-11-08
**Maintainer**: VibeSQL Team
**Status**: BLOCKED - Timeout regression preventing progress (#1012)
